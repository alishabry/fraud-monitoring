from pathlib import Path
import os
import shutil

def download_dataset(**context):
    from kaggle.api.kaggle_api_extended import KaggleApi

    dataset_name = "computingvictor/transactions-fraud-datasets"
    data_path = Path("/opt/airflow/data/transactions_fraud")
    data_path.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        dataset=dataset_name,
        path=str(data_path),
        unzip=True,
        quiet=False
    )

    files = [f.name for f in data_path.iterdir()]

    return files



