CREATE OR REPLACE PROCEDURE TASK_8_DWH.LANDING.LOAD_DATA_WITH_TIMESTAMP()
RETURNS BOOLEAN
LANGUAGE SQL
EXECUTE AS CALLER
AS 
BEGIN
    copy files into @public.STAGE_1 from @public.Internal_stage;
    BEGIN TRANSACTION;
    UPDATE landing.LAST_CHECK set LAST_LOAD_TIME = current_timestamp();
    CREATE OR REPLACE TEMPORARY TABLE raw_data_tmp (
    "id" NUMBER(38,0),
    "Passenger ID" VARCHAR(16777216),
    "First Name" VARCHAR(16777216),
    "Last Name" VARCHAR(16777216),
    "Gender" VARCHAR(16777216),
    "Age" NUMBER(38,0),
    "Nationality" VARCHAR(16777216),
    "Airport Name" VARCHAR(16777216),
    "Airport Country Code" VARCHAR(16777216),
    "Country Name" VARCHAR(16777216),
    "Airport Continent" VARCHAR(16777216),
    "Continents" VARCHAR(16777216),
    "Departure Date" DATE,
    "Arrival Airport" VARCHAR(16777216),
    "Pilot Name" VARCHAR(16777216),
    "Flight Status" VARCHAR(16777216),
    "Ticket Type" VARCHAR(16777216),
    "Passenger Status" VARCHAR(16777216)
    );
    
    COPY INTO raw_data_tmp
    FROM @public.stage_1
    FILE_FORMAT = (type = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY ='"');
    
    ALTER TABLE raw_data_tmp ADD COLUMN load_time TIMESTAMP_LTZ;
    update raw_data_tmp set load_time = (select * from landing.LAST_CHECK);
    INSERT INTO landing.raw_data
    SELECT * FROM raw_data_tmp;
    COMMIT;
    drop table raw_data_tmp;
    
    REMOVE @public.internal_stage/;
    REMOVE @public.stage_1/;
  
END;