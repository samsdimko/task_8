from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import os

BASE_DIR = '/mnt/c/Users/User/PycharmProjects/task_8'

# Define default arguments
default_args = {
    'owner': 'samsdimko',
}

def create_put_query():
    files = os.listdir(f"{BASE_DIR}/source/")
    files = list(filter(lambda x: x.endswith('.csv'), files))
    query = '\n'.join([f'PUT file://{BASE_DIR}/source/{x} @Internal_stage;' for x in files])
    return query


def move_worked_files():
    files = os.listdir(f"{BASE_DIR}/source")
    files = list(filter(lambda x: x.endswith('.csv'), files))
    os.makedirs(f'{BASE_DIR}/archive', exist_ok=True)
    for fle in files:
        os.replace(f'{BASE_DIR}/source/{fle}', f'{BASE_DIR}/archive/{fle}')


with DAG(
    dag_id='load_data_to_snowflake',
        start_date=datetime(2024, 1, 1),
        schedule_interval="*/1 * * * *",
        catchup=False
) as dag:
    check_file = FileSensor(task_id="wait_for_file", filepath=f"{BASE_DIR}/source/")

    load_csv_to_inner_stage = SnowflakeOperator(
        task_id='load_csv_to_inner_stage',
        snowflake_conn_id='snowflake_conn',
        sql=create_put_query()
    )

    replace_files = PythonOperator(
        task_id='replace_files',
        python_callable=move_worked_files
    )

    check_file >> load_csv_to_inner_stage >> replace_files
