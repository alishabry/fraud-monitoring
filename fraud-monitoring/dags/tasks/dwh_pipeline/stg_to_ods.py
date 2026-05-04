from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_stg_to_ods():
    hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    CREATE SCHEMA IF NOT EXISTS ods;

    DROP TABLE IF EXISTS ods.transactions;
    CREATE TABLE ods.transactions AS
    SELECT
        id AS transaction_id,
        NULLIF(date, '')::timestamp AS transaction_ts,
        client_id AS user_id,
        card_id,
        NULLIF(REPLACE(REPLACE(amount, '$', ''), ',', ''), '')::numeric(18,2) AS amount,
        CASE
            WHEN LOWER(TRIM(use_chip)) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(use_chip)) IN ('no', 'false', '0') THEN FALSE
            ELSE NULL
        END AS use_chip_flag,
        merchant_id,
        merchant_city,
        merchant_state,
        zip AS merchant_zip,
        mcc AS mcc_code,
        NULLIF(errors, '') AS transaction_errors
    FROM stg.transactions_data;

    DROP TABLE IF EXISTS ods.users;
    CREATE TABLE ods.users AS
    SELECT
        id AS user_id,
        NULLIF(current_age, '')::int AS current_age,
        NULLIF(retirement_age, '')::int AS retirement_age,
        NULLIF(birth_year, '')::int AS birth_year,
        NULLIF(birth_month, '')::int AS birth_month,
        NULLIF(gender, '') AS gender,
        NULLIF(address, '') AS address,
        NULLIF(latitude, '')::numeric(12,8) AS latitude,
        NULLIF(longitude, '')::numeric(12,8) AS longitude,
        NULLIF(REPLACE(REPLACE(per_capita_income, '$', ''), ',', ''), '')::numeric(18,2) AS per_capita_income,
        NULLIF(REPLACE(REPLACE(yearly_income, '$', ''), ',', ''), '')::numeric(18,2) AS yearly_income,
        NULLIF(REPLACE(REPLACE(total_debt, '$', ''), ',', ''), '')::numeric(18,2) AS total_debt,
        NULLIF(credit_score, '')::int AS credit_score,
        NULLIF(num_credit_cards, '')::int AS num_credit_cards
    FROM stg.users_data;

    DROP TABLE IF EXISTS ods.cards;
    CREATE TABLE ods.cards AS
    SELECT
        id AS card_id,
        client_id AS user_id,
        NULLIF(card_brand, '') AS card_brand,
        NULLIF(card_type, '') AS card_type,
        NULLIF(card_number, '') AS card_number_masked,
        NULLIF(expires, '') AS expires_raw,
        NULLIF(cvv, '') AS cvv_raw,
        CASE
            WHEN LOWER(TRIM(has_chip)) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(has_chip)) IN ('no', 'false', '0') THEN FALSE
            ELSE NULL
        END AS has_chip_flag,
        NULLIF(num_cards_issued, '')::int AS num_cards_issued,
        NULLIF(REPLACE(REPLACE(credit_limit, '$', ''), ',', ''), '')::numeric(18,2) AS credit_limit,
        NULLIF(acct_open_date, '') AS acct_open_date_raw,
        NULLIF(year_pin_last_changed, '')::int AS year_pin_last_changed,
        CASE
            WHEN LOWER(TRIM(card_on_dark_web)) IN ('yes', 'true', '1') THEN TRUE
            WHEN LOWER(TRIM(card_on_dark_web)) IN ('no', 'false', '0') THEN FALSE
            ELSE NULL
        END AS card_on_dark_web_flag
    FROM stg.cards_data;

    DROP TABLE IF EXISTS ods.mcc_codes;
    CREATE TABLE ods.mcc_codes AS
    SELECT
        mcc_code,
        NULLIF(mcc_description, '') AS mcc_description
    FROM stg.mcc_codes;

    DROP TABLE IF EXISTS ods.fraud_labels;
    CREATE TABLE ods.fraud_labels AS
    SELECT
        transaction_id,
        NULLIF(fraud_label, '') AS fraud_label,
        CASE
            WHEN LOWER(TRIM(fraud_label)) IN ('yes', 'true', '1', 'fraud') THEN TRUE
            WHEN LOWER(TRIM(fraud_label)) IN ('no', 'false', '0', 'not fraud') THEN FALSE
            ELSE NULL
        END AS is_fraud_flag
    FROM stg.train_fraud_labels;
    """

    hook.run(sql)

