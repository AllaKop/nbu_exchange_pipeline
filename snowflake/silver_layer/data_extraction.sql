USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.SILVER;

CREATE OR REPLACE TABLE nbu_exchange.silver.exchange_rate_extracted (
    calculation_date STRING,
    currency_code STRING,
    currency_name STRING,
    exchange_date STRING,
    group_number INT,
    r030_code INT,
    rate FLOAT,
    rate_per_unit FLOAT,
    special_conditions STRING,
    currency_name_ua STRING,
    units INT
);

INSERT INTO nbu_exchange.silver.exchange_rate_extracted
SELECT
    value:calcdate::STRING,
    value:cc::STRING,
    value:enname::STRING,
    value:exchangedate::STRING,
    value:group::INT,
    value:r030::INT,
    value:rate::FLOAT,
    value:rate_per_unit::FLOAT,
    value:special::STRING,
    value:txt::STRING,
    value:units::INT
FROM nbu_exchange.bronze.exchange_rate_raw,
     LATERAL FLATTEN(input => raw);

SELECT count(*)
FROM nbu_exchange.silver.exchange_rate_extracted;

CREATE OR REPLACE STREAM exchange_rate_raw_stream ON TABLE nbu_exchange.bronze.exchange_rate_raw;

SHOW STREAMS IN SCHEMA NBU_EXCHANGE.SILVER;

SELECT count(*) 
FROM nbu_exchange.silver.exchange_rate_raw_stream;

CREATE OR REPLACE TASK load_silver_from_bronze
  SCHEDULE = 'USING CRON 0 7 * * * UTC'  
AS
INSERT INTO nbu_exchange.silver.exchange_rate_extracted
SELECT
    value:calcdate::STRING,
    value:cc::STRING,
    value:enname::STRING,
    value:exchangedate::STRING,
    value:group::INT,
    value:r030::INT,
    value:rate::FLOAT,
    value:rate_per_unit::FLOAT,
    value:special::STRING,
    value:txt::STRING,
    value:units::INT
FROM nbu_exchange.silver.exchange_rate_raw_stream,
     LATERAL FLATTEN(input => raw);

ALTER TASK nbu_exchange.silver.load_silver_from_bronze RESUME;

SHOW TASKS;

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE NAME = 'nbu_exchange.silver.load_silver_from_bronze'
ORDER BY COMPLETED_TIME DESC
LIMIT 10;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE DATA_ENGINEER;

SHOW TASKS LIKE 'load_silver_from_bronze' IN SCHEMA nbu_exchange.silver;

