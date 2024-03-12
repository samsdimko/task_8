-- DDL for landing layer

-- last check for loading managing
create or replace TABLE TASK_8_DWH.LANDING.LAST_CHECK (
	LAST_LOAD_TIME TIMESTAMP_NTZ(9)
);

-- raw data with load time
create or replace TABLE TASK_8_DWH.LANDING.RAW_DATA (
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
	"Passenger Status" VARCHAR(16777216),
	LOAD_TIME TIMESTAMP_LTZ(9)
);