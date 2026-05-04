from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_dds_to_cdm():
    hook = PostgresHook(postgres_conn_id="postgres")

    sql = """
    CREATE SCHEMA IF NOT EXISTS cdm;

    -- CDM: Analytical marts for fraud monitoring dashboard
    -- 1. Fraud rate by customer segment
    DROP TABLE IF EXISTS cdm.fraud_rate_by_customer_segment;
    CREATE TABLE cdm.fraud_rate_by_customer_segment AS
    SELECT 
        credit_segment,
        transaction_value_segment,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud_flag = true) as fraud_transactions,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_fraud_flag = true) / 
            NULLIF(COUNT(*), 0), 2
        ) as fraud_rate_pct
    FROM dds.fact_transactions
    GROUP BY 1, 2;

    -- 2. Fraud rate by MCC category
    DROP TABLE IF EXISTS cdm.fraud_rate_by_mcc;
    CREATE TABLE cdm.fraud_rate_by_mcc AS
    SELECT 
        m.mcc_code,
        m.mcc_description,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud_flag = true) as fraud_transactions,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_fraud_flag = true) / 
            NULLIF(COUNT(*), 0), 2
        ) as fraud_rate_pct
    FROM dds.fact_transactions t
    JOIN dds.dim_mcc m ON t.mcc_code = m.mcc_code
    GROUP BY 1, 2
    ORDER BY fraud_rate_pct DESC;

    -- 3. High-risk users (top fraudsters)
    DROP TABLE IF EXISTS cdm.high_risk_users;
    CREATE TABLE cdm.high_risk_users AS
    SELECT 
        u.user_id,
        u.credit_score,
        u.yearly_income,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud_flag = true) as fraud_transactions,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_fraud_flag = true) / 
            NULLIF(COUNT(*), 0), 2
        ) as user_fraud_rate_pct
    FROM dds.fact_transactions t
    JOIN dds.dim_users u ON t.user_id = u.user_id
    GROUP BY 1, 2, 3
    HAVING COUNT(*) FILTER (WHERE is_fraud_flag = true) >= 5
    ORDER BY user_fraud_rate_pct DESC;

    -- 4. Fraud by time of day (hourly patterns)
    DROP TABLE IF EXISTS cdm.fraud_by_hour;
    CREATE TABLE cdm.fraud_by_hour AS
    SELECT 
        EXTRACT(HOUR FROM transaction_ts) as transaction_hour,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud_flag = true) as fraud_transactions,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_fraud_flag = true) / 
            NULLIF(COUNT(*), 0), 2
        ) as fraud_rate_pct
    FROM dds.fact_transactions
    GROUP BY 1
    ORDER BY 1;

    -- 5. Card brand fraud analysis
    DROP TABLE IF EXISTS cdm.fraud_by_card_brand;
    CREATE TABLE cdm.fraud_by_card_brand AS
    SELECT 
        card_brand,
        COUNT(*) as total_transactions,
        COUNT(*) FILTER (WHERE is_fraud_flag = true) as fraud_transactions,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE is_fraud_flag = true) / 
            NULLIF(COUNT(*), 0), 2
        ) as fraud_rate_pct
    FROM dds.fact_transactions t
    JOIN dds.dim_cards c ON t.card_id = c.card_id
    GROUP BY 1
    ORDER BY fraud_rate_pct DESC;
    """

    hook.run(sql)

