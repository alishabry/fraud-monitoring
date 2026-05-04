import os
import glob
import re

from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_csv_to_stg():
    data_path = "/opt/airflow/data/transactions_fraud"
    csv_files = glob.glob(os.path.join(data_path, "*.csv"))

    if not csv_files:
        raise Exception("No CSV files found in {}".format(data_path))

    hook = PostgresHook(postgres_conn_id="postgres")

    for csv_file in csv_files:
        file_name = os.path.splitext(os.path.basename(csv_file))[0]
        table_name = re.sub(r"[^a-zA-Z0-9_]", "_", file_name.lower())

        hook.run("TRUNCATE TABLE stg.{};".format(table_name))

        copy_sql = """
            COPY stg.{}
            FROM STDIN
            WITH CSV HEADER DELIMITER ',' QUOTE '"'
        """.format(table_name)

        hook.copy_expert(
            sql=copy_sql,
            filename=csv_file
        )

        print("Loaded {} into stg.{}".format(os.path.basename(csv_file), table_name))


