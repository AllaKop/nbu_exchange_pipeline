USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.BRONZE;

CREATE OR REPLACE FILE FORMAT nbu_exchange.bronze.xml_file_format
  TYPE = 'XML';

CREATE OR REPLACE STAGE nbu_exchange.bronze.iso_4217_files_stage
  FILE_FORMAT = nbu_exchange.bronze.xml_file_format;

LIST @nbu_exchange.bronze.iso_4217_files_stage;

SELECT
  METADATA$FILENAME,
  $1
FROM @nbu_exchange.bronze.iso_4217_files_stage
LIMIT 5;

CREATE OR REPLACE TABLE nbu_exchange.bronze.iso4217_xml_raw (
  file_name STRING,
  xml_content VARIANT
);

COPY INTO nbu_exchange.bronze.iso4217_xml_raw (file_name, xml_content)
FROM (
  SELECT
    METADATA$FILENAME,
    PARSE_XML($1)
  FROM @nbu_exchange.bronze.iso_4217_files_stage
)
FILE_FORMAT = (TYPE = 'XML');

SELECT * 
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 5;

-- list-one.xml and list-three.xml


