import os
import json
import csv
import io

from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_json_to_stg():
    data_path = "/opt/airflow/data/transactions_fraud"
    hook = PostgresHook(postgres_conn_id="postgres")

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS stg;")

        train_labels_path = os.path.join(data_path, "train_fraud_labels.json")
        mcc_codes_path = os.path.join(data_path, "mcc_codes.json")

        if os.path.exists(train_labels_path):
            with open(train_labels_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict) and "target" in data and isinstance(data["target"], dict):
                labels_dict = data["target"]
            elif isinstance(data, dict) and len(data) == 1 and isinstance(next(iter(data.values())), dict):
                labels_dict = next(iter(data.values()))
            elif isinstance(data, dict):
                labels_dict = data
            else:
                raise ValueError("Unexpected format in train_fraud_labels.json")

            cursor.execute("DROP TABLE IF EXISTS stg.train_fraud_labels;")
            cursor.execute("""
                CREATE TABLE stg.train_fraud_labels (
                    transaction_id TEXT,
                    fraud_label TEXT
                );
            """)

            buffer = io.StringIO()
            writer = csv.writer(buffer)

            for transaction_id, fraud_label in labels_dict.items():
                writer.writerow([str(transaction_id), str(fraud_label)])

            buffer.seek(0)

            cursor.copy_expert(
                """
                COPY stg.train_fraud_labels (transaction_id, fraud_label)
                FROM STDIN WITH (FORMAT CSV)
                """,
                buffer
            )

            conn.commit()
            print(f"Loaded {len(labels_dict)} rows into stg.train_fraud_labels")

        if os.path.exists(mcc_codes_path):
            with open(mcc_codes_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                raise ValueError("Unexpected format in mcc_codes.json")

            cursor.execute("DROP TABLE IF EXISTS stg.mcc_codes;")
            cursor.execute("""
                CREATE TABLE stg.mcc_codes (
                    mcc_code TEXT,
                    mcc_description TEXT
                );
            """)

            buffer = io.StringIO()
            writer = csv.writer(buffer)

            for mcc_code, mcc_description in data.items():
                writer.writerow([str(mcc_code), str(mcc_description)])

            buffer.seek(0)

            cursor.copy_expert(
                """
                COPY stg.mcc_codes (mcc_code, mcc_description)
                FROM STDIN WITH (FORMAT CSV)
                """,
                buffer
            )

            conn.commit()
            print(f"Loaded {len(data)} rows into stg.mcc_codes")

    finally:
        cursor.close()
        conn.close()

