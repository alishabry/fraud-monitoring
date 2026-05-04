from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_ods_to_dds():
    hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    CREATE SCHEMA IF NOT EXISTS dds;

    -- Dimension tables
    DROP TABLE IF EXISTS dds.dim_users;
    CREATE TABLE dds.dim_users AS
    SELECT DISTINCT
        user_id,
        current_age,
        retirement_age,
        birth_year,
        birth_month,
        gender,
        latitude,
        longitude,
        per_capita_income,
        yearly_income,
        total_debt,
        credit_score,
        num_credit_cards
    FROM ods.users;

    DROP TABLE IF EXISTS dds.dim_cards;
    CREATE TABLE dds.dim_cards AS
    SELECT DISTINCT
        card_id,
        user_id,
        card_brand,
        card_type,
        card_number_masked,
        expires_raw,
        cvv_raw,
        has_chip_flag,
        num_cards_issued,
        credit_limit,
        acct_open_date_raw,
        year_pin_last_changed,
        card_on_dark_web_flag
    FROM ods.cards;

    DROP TABLE IF EXISTS dds.dim_mcc;
    CREATE TABLE dds.dim_mcc AS
    SELECT DISTINCT
        mcc_code,
        mcc_description
    FROM ods.mcc_codes;

  -- Fact table
    DROP TABLE IF EXISTS dds.fact_transactions;
    CREATE TABLE dds.fact_transactions AS
    SELECT
        t.transaction_id,
        t.transaction_ts,
        t.user_id,
        t.card_id,
        t.mcc_code,
        t.amount,
        t.use_chip_flag,
        t.merchant_id,
        t.merchant_city,
        t.merchant_state,
        t.merchant_zip,
        t.transaction_errors,
        COALESCE(fl.is_fraud_flag, false) as is_fraud_flag,
        CASE
            WHEN u.credit_score >= 750 THEN 'A'
            WHEN u.credit_score >= 650 THEN 'B'
            WHEN u.credit_score >= 550 THEN 'C'
            ELSE 'D'
        END AS credit_segment,
        CASE
            WHEN t.amount > 1000 THEN 'high_value'
            WHEN t.amount > 100 THEN 'medium_value'
            ELSE 'low_value'
        END AS transaction_value_segment
    FROM ods.transactions t
    LEFT JOIN ods.users u ON t.user_id = u.user_id
    LEFT JOIN ods.cards c ON t.card_id = c.card_id
    LEFT JOIN ods.fraud_labels fl ON t.transaction_id = fl.transaction_id; """

    hook.run(sql)

