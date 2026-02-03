USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.SILVER;
CREATE OR REPLACE TABLE nbu_exchange.silver.exchange_rate_extracted (
    calculation_date DATE,
    currency_code STRING,
    currency_name STRING,
    exchange_date DATE,
    group_number INT,
    r030 INT,
    rate FLOAT,
    rate_per_unit FLOAT,
    special STRING,
    currency_name_ua STRING,
    units INT
);

CREATE OR REPLACE STREAM exchange_rate_raw_stream ON TABLE nbu_exchange.bronze.exchange_rate_raw;

CREATE OR REPLACE TASK load_silver_from_bronze
  SCHEDULE = 'USING CRON 0 7 * * * UTC'  
AS
INSERT INTO nbu_exchange.silver.exchange_rate_silver
SELECT
    value:calcdate::DATE,
    value:cc::STRING,
    value:enname::STRING,
    value:exchangedate::DATE,
    value:group::INT,
    value:r030::INT,
    value:rate::FLOAT,
    value:rate_per_unit::FLOAT,
    value:special::STRING,
    value:txt::STRING,
    value:units::INT
FROM nbu_exchange.bronze.exchange_rate_staging_stream,
     LATERAL FLATTEN(input => raw);

ALTER TASK nbu_exchange.silver.load_silver_from_bronze RESUME;