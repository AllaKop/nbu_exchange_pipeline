USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.BRONZE;

CREATE OR REPLACE FILE FORMAT nbu_exchange.bronze.csv_file_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

CREATE OR REPLACE STAGE nbu_exchange.bronze.r030_csv_files_stage
  FILE_FORMAT = nbu_exchange.bronze.csv_file_format;

LIST @nbu_exchange.bronze.r030_csv_files_stage;

SELECT
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
FROM @nbu_exchange.bronze.r030_csv_files_stage
LIMIT 5;

CREATE OR REPLACE TABLE nbu_exchange.bronze.r030_csv_raw (
    PR STRING,
    R030 STRING,
    K040 STRING,
    A3 STRING,
    R031 STRING,
    R032 STRING,
    R033 STRING,
    R034 STRING,
    R035 STRING,
    GR STRING,
    KOD_LIT STRING,
    LOD_NUM STRING,
    currency_name_ua STRING,
    NOMIN STRING,
    NAIM STRING,
    D_OPEN STRING,
    D_CLOSE STRING,
    D_MODI STRING
);

COPY INTO nbu_exchange.bronze.r030_csv_raw
FROM @nbu_exchange.bronze.r030_csv_files_stage
FILE_FORMAT = (FORMAT_NAME = 'nbu_exchange.bronze.csv_file_format');

SELECT * 
FROM nbu_exchange.bronze.r030_csv_raw
LIMIT 5;


