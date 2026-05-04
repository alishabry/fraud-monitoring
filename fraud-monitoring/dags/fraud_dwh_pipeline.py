from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from tasks.dwh_pipeline.create_schemas import create_schemas
from tasks.dwh_pipeline.stg_to_ods import load_stg_to_ods
from tasks.dwh_pipeline.ods_to_dds import load_ods_to_dds
from tasks.dwh_pipeline.dds_to_cdm import load_dds_to_cdm


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="fraud_dwh_full_pipeline",
    default_args=default_args,
    description="Full STG -> ODS -> DDS -> CDM pipeline for fraud monitoring",
    start_date=datetime(2026, 5, 3),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "dwh", "full_pipeline"],
) as dag:

    create_schemas_task = PythonOperator(
        task_id="create_schemas",
        python_callable=create_schemas,
    )

    stg_to_ods_task = PythonOperator(
        task_id="stg_to_ods",
        python_callable=load_stg_to_ods,
    )

    ods_to_dds_task = PythonOperator(
        task_id="ods_to_dds",
        python_callable=load_ods_to_dds,
    )

    dds_to_cdm_task = PythonOperator(
        task_id="dds_to_cdm",
        python_callable=load_dds_to_cdm,
    )

    create_schemas_task >> stg_to_ods_task >> ods_to_dds_task >> dds_to_cdm_task

