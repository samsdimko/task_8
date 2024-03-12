# Task 8 Snowflake DWH

This is a project for Data Warehouse with Snowflake. 

### DWH declaration
In ```/queries/``` folder there are folders for creation and processing the DWH. 

In ```/queries/DDL/``` there are DDL queries to create tables for landing, dimensional and matamart schemas. 
In ```/queries/procedures/``` there are queries to create procedures that implement mechanism for ETL within the Warehouse. 
In ```/queries/pipline/``` there is a file with queries to create necessary stages, streams and tasks to implement automatic CDC data loading.


### External data loading
In ```main.py``` there is an Airflow DAG that loads data from ```/source/``` directory into the Internal stage in Snowflake.