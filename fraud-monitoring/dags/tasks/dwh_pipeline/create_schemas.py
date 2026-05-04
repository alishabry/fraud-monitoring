from airflow.providers.postgres.hooks.postgres import PostgresHook


def create_schemas():
    hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    CREATE SCHEMA IF NOT EXISTS ods;
    CREATE SCHEMA IF NOT EXISTS dds;
    CREATE SCHEMA IF NOT EXISTS cdm;
    """

    hook.run(sql)

