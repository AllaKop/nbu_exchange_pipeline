USE ROLE DATA_ENGINEER;

CREATE STORAGE INTEGRATION nbu_exchange_S3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::693426599524:role/nbu_exchage_s3_snowflake_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nbu-exchange-raw/');


DESC INTEGRATION nbu_exchange_S3_integration;

USE SCHEMA nbu_exchange.bronze;

CREATE FILE FORMAT nbu_exchange_json_format
  TYPE = 'JSON';

CREATE STAGE nbu_exchange_stage
    STORAGE_INTEGRATION = nbu_exchange_S3_integration
    URL = 's3://nbu-exchange-raw/json'
    FILE_FORMAT = nbu_exchange_json_format;