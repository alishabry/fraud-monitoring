from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException

from tasks.load_data.download_dataset import download_dataset
from tasks.load_data.create_stg_tables import create_stg_tables
from tasks.load_data.load_csv_to_stg import load_csv_to_stg
from tasks.load_data.cleanup_data_folder import cleanup_data_folder
from tasks.load_data.load_json_to_stg import load_json_to_stg

def skip_task():
    raise AirflowSkipException("Temporarily skipped")

with DAG(
    dag_id="fraud_data_load",
    start_date=datetime(2026, 4, 25),
    schedule_interval=None,
    catchup=False,

) as dag:

    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    ) 

    create_stg_tables_task = PythonOperator(
        task_id="create_stg_tables",
        python_callable=create_stg_tables,
    )

    load_csv_to_stg_task = PythonOperator(
        task_id="load_csv_to_stg",
        python_callable=load_csv_to_stg
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_data_folder",
        python_callable=cleanup_data_folder
    )
     
    load_json_to_stg_task = PythonOperator(
        task_id="load_json_to_stg",
        python_callable=load_json_to_stg
    )
    
    download_task >> create_stg_tables_task
    create_stg_tables_task >> load_csv_to_stg_task
    create_stg_tables_task >> load_json_to_stg_task
    [load_csv_to_stg_task, load_json_to_stg_task] >> cleanup_task

