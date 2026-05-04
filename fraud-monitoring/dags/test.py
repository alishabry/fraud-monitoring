from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from tasks.load_data.cleanup_data_folder import cleanup_data_folder
from tasks.load_data.download_dataset import download_dataset
from tasks.load_data.create_stg_tables import create_stg_tables
from tasks.load_data.load_csv_to_stg import load_csv_to_stg
from tasks.load_data.load_json_to_stg import load_json_to_stg


def test_postgres_connection():
    hook = PostgresHook(postgres_conn_id="postgres")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print(f"Postgres connection test result: {result}")
    cursor.close()
    conn.close()


with DAG(
    dag_id="test_postgres_connection",
    start_date=datetime(2026, 4, 25),
    schedule_interval=None,
    catchup=False,
    tags=["test", "postgres"],
) as dag:

    test_conn = PythonOperator(
        task_id="test_connection",
        python_callable=load_json_to_stg
    )
