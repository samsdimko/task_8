CREATE OR REPLACE PROCEDURE load_to_datamart()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS CALLER
AS 
BEGIN
    REMOVE @public.stage_3/;
    
    copy into @public.stage_3 
    from dimensional.flights 
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');

    create or replace TEMPORARY table to_datamart_flights_tmp 
        as select * from dimensional.flights where 0=1;
            
    COPY INTO to_datamart_flights_tmp
    FROM @public.stage_3
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');

    REMOVE @public.stage_3/;
    
    copy into @public.stage_3 
    from dimensional.passengers 
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');

    create or replace TEMPORARY table to_datamart_passengers_tmp 
        as select * from dimensional.passengers where 0=1;
            
    COPY INTO to_datamart_passengers_tmp
    FROM @public.stage_3
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');
    
    BEGIN TRANSACTION;
    MERGE INTO DATAMART.AIRPORTS_IN_CONTINENTS_COUNTS USING (
        SELECT DISTINCT continent_name, count(*) as airfport_count 
        FROM dimensional.airports
        GROUP BY CONTINENT_NAME
        ORDER BY airfport_count DESC) as src
    ON DATAMART.AIRPORTS_IN_CONTINENTS_COUNTS.CONTINENT_NAME = src.CONTINENT_NAME
    WHEN MATCHED THEN 
        UPDATE SET
            AIRFPORT_COUNT = src.AIRFPORT_COUNT
    WHEN NOT MATCHED THEN
        INSERT (continent_name, airfport_count)
        VALUES (src.continent_name, src.airfport_count);

    MERGE INTO DATAMART.GENDER_PASSENGER_COUNTS USING (
        SELECT DISTINCT continent_name, country_name, 
            COUNT(*) as flights_count,
            SUM(CASE WHEN passengers.GENDER = 'Male' THEN 1 ELSE 0 END) AS male_count,
            SUM(CASE WHEN passengers.GENDER = 'Female' THEN 1 ELSE 0 END) AS female_count,
            ROUND(male_count/flights_count*100, 2) as male_percent,
            ROUND(female_count/flights_count*100, 2) as female_percent
        FROM to_datamart_flights_tmp as flights
            INNER JOIN to_datamart_passengers_tmp as passengers ON flights.passenger_id = passengers.id
            INNER JOIN dimensional.airports ON flights.airport_id = airports.id
        GROUP BY continent_name, country_name
        ORDER BY flights_count DESC) as src
    ON DATAMART.GENDER_PASSENGER_COUNTS.country_name = src.country_name 
    AND DATAMART.GENDER_PASSENGER_COUNTS.continent_name = src.continent_name
    WHEN MATCHED THEN
        UPDATE SET
            flights_count = src.flights_count,
            male_count = src.male_count,
            female_count = src.female_count,
            male_percent = src.male_percent,
            female_percent = src.female_percent
    WHEN NOT MATCHED THEN
        INSERT (continent_name, country_name, flights_count, male_count, female_count, male_percent, female_percent)
        VALUES (src.continent_name, src.country_name, src.flights_count, 
                src.male_count, src.female_count, src.male_percent, src.female_percent);

    MERGE INTO DATAMART.PASSENGERS_AGE_GROUPS_BY_NATIONALITY USING (
        WITH passenger_counts AS (
          SELECT DISTINCT
            nationality,
            COUNT(*) AS total_count,
            SUM(CASE WHEN age > 25 THEN 1 ELSE 0 END) AS over_25_count,
            SUM(CASE WHEN age > 35 THEN 1 ELSE 0 END) AS over_35_count,
            SUM(CASE WHEN age > 50 THEN 1 ELSE 0 END) AS over_50_count
          FROM to_datamart_passengers_tmp as passengers
          GROUP BY nationality
        )
        SELECT
          nationality,
          total_count,
          ROUND((over_25_count / total_count) * 100, 2) AS pct_over_25,
          ROUND((over_35_count / total_count) * 100, 2) AS pct_over_35,
          ROUND((over_50_count / total_count) * 100, 2) AS pct_over_50
        FROM passenger_counts
        ORDER BY nationality) as src
    ON DATAMART.PASSENGERS_AGE_GROUPS_BY_NATIONALITY.nationality = src.nationality
    WHEN MATCHED THEN 
        UPDATE SET
            total_count = src.total_count,
            pct_over_25 = src.pct_over_25,
            pct_over_35 = src.pct_over_35,
            pct_over_50 = src.pct_over_50
    WHEN NOT MATCHED THEN
        INSERT (nationality, total_count, pct_over_25, pct_over_35, pct_over_50)
        VALUES (src.nationality, src.total_count, src.pct_over_25, 
                src.pct_over_35, src.pct_over_50);
    COMMIT;
    
    REMOVE @public.stage_3/;
    DROP TABLE to_datamart_flights_tmp;
END;