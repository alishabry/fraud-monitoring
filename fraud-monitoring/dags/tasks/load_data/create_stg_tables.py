import os
import glob
import csv
import re

from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_stg_tables():
    data_path = "/opt/airflow/data/transactions_fraud"

    csv_files = glob.glob(os.path.join(data_path, "*.csv"))
    json_files = glob.glob(os.path.join(data_path, "*.json"))

    if not csv_files and not json_files:
        raise Exception("No CSV or JSON files found in {}".format(data_path))

    hook = PostgresHook(postgres_conn_id="postgres")
    hook.run("CREATE SCHEMA IF NOT EXISTS stg;")

    for csv_file in csv_files:
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name.lower())

        with open(csv_file, "r") as f:
            reader = csv.reader(f)
            headers = next(reader)

        columns = []
        for col in headers:
            col_name = re.sub(r"[^a-zA-Z0-9_]", "_", col.strip().lower())
            columns.append('"{}" TEXT'.format(col_name))

        ddl = """
        CREATE TABLE IF NOT EXISTS stg.{} (
            {}
        );
        """.format(table_name, ", ".join(columns))

        hook.run(ddl)
        print("Created CSV table stg.{}".format(table_name))

    json_file_names = [os.path.basename(f) for f in json_files]

    if "train_fraud_labels.json" in json_file_names:
        hook.run("""
        CREATE TABLE IF NOT EXISTS stg.train_fraud_labels (
            transaction_id TEXT,
            fraud_label INTEGER
        );
        """)
        print("Created JSON table stg.train_fraud_labels")

    if "mcc_codes.json" in json_file_names:
        hook.run("""
        CREATE TABLE IF NOT EXISTS stg.mcc_codes (
            mcc_code TEXT,
            mcc_description TEXT
        );
        """)
        print("Created JSON table stg.mcc_codes")

