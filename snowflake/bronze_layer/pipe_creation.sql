USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.BRONZE;

CREATE OR REPLACE TABLE nbu_exchange.bronze.exchange_rate_raw (
    raw VARIANT
);


CREATE OR REPLACE PIPE nbu_exchange.bronze.nbu_exchange_pipe
  AUTO_INGEST = TRUE
  AS
    COPY INTO nbu_exchange.bronze.exchange_rate_raw 
      FROM @nbu_exchange.bronze.nbu_exchange_stage
      FILE_FORMAT = (TYPE = 'JSON');

SHOW PIPES;

DESCRIBE PIPE nbu_exchange.bronze.nbu_exchange_pipe;

SELECT * 
FROM nbu_exchange.bronze.exchange_rate_raw 
;

SELECT
  $1
FROM
  @nbu_exchange_stage (FILE_FORMAT => nbu_exchange_json_format);

select *
  from table(nbu_exchange.information_schema.pipe_usage_history(
    date_range_start=>current_date(),
    date_range_end=>current_date()));

select *
  from table(nbu_exchange.information_schema.data_transfer_history(
    date_range_start=>current_date(),
    date_range_end=>current_date()));

select * from table(validate_pipe_load(
  pipe_name=>'nbu_exchange.bronze.nbu_exchange_pipe',
  start_time=>dateadd(hour, -1, current_timestamp())));

select *
from nbu_exchange.information_schema.load_history;

select *
from nbu_exchange.information_schema.pipes;

DESC TABLE SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY;

SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE PIPE_NAME = 'NBU_EXCHANGE.BRONZE.NBU_EXCHANGE_PIPE'
ORDER BY LAST_LOAD_TIME DESC;

LIST @nbu_exchange.bronze.nbu_exchange_stage;
