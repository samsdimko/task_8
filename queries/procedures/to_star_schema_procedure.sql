create OR REPLACE PROCEDURE landing_to_dimensional_procedure()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS CALLER
as
BEGIN
    REMOVE @public.stage_2/;
    
    copy into @public.stage_2 
    from landing_to_dimensional_stream 
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');

    create or replace TEMPORARY table to_dimensional_tmp 
        as select * from landing_to_dimensional_stream where 0=1;
        
    ALTER TABLE to_dimensional_tmp 
    MODIFY COLUMN "METADATA$ACTION" VARCHAR,
            COLUMN "METADATA$ROW_ID" VARCHAR;
            
    COPY INTO to_dimensional_tmp
    FROM @public.stage_2
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ','
                    FIELD_OPTIONALLY_ENCLOSED_BY ='"');
    BEGIN TRANSACTION;
    MERGE INTO dimensional.airports 
    USING (SELECT DISTINCT "Airport Name", "Airport Country Code", "Country Name", 
    "Airport Continent", "Continents" from to_dimensional_tmp) as to_dimensional_tmp_airports 
    on to_dimensional_tmp_airports."Airport Name" = dimensional.airports.Airport_Name and to_dimensional_tmp_airports."Airport Country Code" = dimensional.airports.COUNTRY_CODE
    WHEN MATCHED THEN
        UPDATE set 
            Country_Code = to_dimensional_tmp_airports."Airport Country Code",
            Country_Name = to_dimensional_tmp_airports."Country Name",
            Continent_Code = to_dimensional_tmp_airports."Airport Continent",
            Continent_Name = to_dimensional_tmp_airports."Continents"
    WHEN NOT MATCHED THEN 
        INSERT 
            (Airport_Name, Country_Code, Country_Name, Continent_Code, Continent_Name)
            VALUES (to_dimensional_tmp_airports."Airport Name", 
                    to_dimensional_tmp_airports."Airport Country Code", 
                    to_dimensional_tmp_airports."Country Name", 
                    to_dimensional_tmp_airports."Airport Continent", 
                    to_dimensional_tmp_airports."Continents");

    MERGE INTO dimensional.arrival_airports 
    USING (SELECT DISTINCT "Arrival Airport" from to_dimensional_tmp) as to_dimensional_tmp_arrival
    on dimensional.arrival_airports.code = to_dimensional_tmp_arrival."Arrival Airport"
    WHEN NOT MATCHED THEN 
        INSERT 
            (code)
            VALUES (to_dimensional_tmp_arrival."Arrival Airport");

    MERGE INTO dimensional.flight_statuses 
    USING (SELECT DISTINCT "Flight Status" from to_dimensional_tmp) as to_dimensional_tmp_flihgt_statuses
    on dimensional.flight_statuses.status_name = to_dimensional_tmp_flihgt_statuses."Flight Status"
    WHEN NOT MATCHED THEN 
        INSERT 
            (status_name)
            VALUES (to_dimensional_tmp_flihgt_statuses."Flight Status");


    MERGE INTO dimensional.passengers 
    USING (SELECT DISTINCT "Passenger ID", "First Name", "Last Name", "Gender", "Age", "Nationality" 
    from to_dimensional_tmp) as to_dimensional_tmp_passengers
    on dimensional.passengers.ID = to_dimensional_tmp_passengers."Passenger ID"
    WHEN MATCHED AND passengers.Age < to_dimensional_tmp_passengers."Age" THEN
        UPDATE set
            passengers.Age = to_dimensional_tmp_passengers."Age"         
    WHEN NOT MATCHED THEN 
        INSERT 
            (ID, first_name, last_name, gender, age, nationality)
            VALUES 
                (to_dimensional_tmp_passengers."Passenger ID", 
                to_dimensional_tmp_passengers."First Name", 
                to_dimensional_tmp_passengers."Last Name", 
                to_dimensional_tmp_passengers."Gender", 
                to_dimensional_tmp_passengers."Age",
                to_dimensional_tmp_passengers."Nationality");

    MERGE INTO dimensional.passenger_statuses 
    USING (SELECT DISTINCT "Passenger Status" from to_dimensional_tmp) as to_dimensional_tmp_passenger_statuses
    on dimensional.passenger_statuses.status_name = to_dimensional_tmp_passenger_statuses."Passenger Status"
    WHEN NOT MATCHED THEN 
        INSERT 
            (status_name)
            VALUES (to_dimensional_tmp_passenger_statuses."Passenger Status");

    MERGE INTO dimensional.ticket_types 
    USING (SELECT DISTINCT "Ticket Type" from to_dimensional_tmp) as to_dimensional_tmp_ticket_types
    on dimensional.ticket_types.type_name = to_dimensional_tmp_ticket_types."Ticket Type"
    WHEN NOT MATCHED THEN 
        INSERT 
            (type_name)
            VALUES (to_dimensional_tmp_ticket_types."Ticket Type");

    INSERT INTO dimensional.FLIGHTS(
        ID,
        PASSENGER_ID,
        AIRPORT_ID,
        FLIGHT_STATUS_ID,
        TICKET_TYPE_ID,
        PASSENGER_STATUS_ID,
        ARRIVAL_AIRPORTS_ID,
        DEPARTURE_DATE
        )   
    SELECT 
        tmp."id",
        tmp."Passenger ID" AS PASSENGER_ID,
        a.ID AS AIRPORT_ID,
        f.ID AS FLIGHT_STATUS_ID,
        t.ID AS TICKET_TYPE_ID,
        ps.ID AS PASSENGER_STATUS_ID,
        aa.ID AS ARRIVAL_AIRPORTS_ID,
        tmp."Departure Date"
        FROM to_dimensional_tmp tmp
            LEFT JOIN dimensional.airports a ON tmp."Airport Name" = a.Airport_Name
            LEFT JOIN dimensional.flight_statuses f ON tmp."Flight Status" = f.status_name
            LEFT JOIN dimensional.ticket_types t ON tmp."Ticket Type" = t.type_name
            LEFT JOIN dimensional.passenger_statuses ps ON tmp."Passenger Status" = ps.status_name
            LEFT JOIN dimensional.arrival_airports aa ON tmp."Arrival Airport" = aa.code;
    COMMIT;
    REMOVE @public.stage_2/;
    DROP table to_dimensional_tmp;
    
END;