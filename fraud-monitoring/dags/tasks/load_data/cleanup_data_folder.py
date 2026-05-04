import shutil


def cleanup_data_folder():
    data_path = "/opt/airflow/data/transactions_fraud"
    shutil.rmtree(data_path, ignore_errors=True)
    print("Removed folder {}".format(data_path))

