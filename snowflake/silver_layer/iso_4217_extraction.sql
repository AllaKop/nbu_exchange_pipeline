USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.SILVER;

SELECT * 
FROM nbu_exchange.bronze.iso4217_xml_raw
;

/* list-one.xml
<ISO_4217 Pblshd="2026-01-01">
  <CcyTbl>
    <CcyNtry>
      <CtryNm>AFGHANISTAN</CtryNm>
      <CcyNm>Afghani</CcyNm>
      <Ccy>AFN</Ccy>
      <CcyNbr>971</CcyNbr>
      <CcyMnrUnts>2</CcyMnrUnts>
    </CcyNtry>
*/

/*
list-three.xml
<ISO_4217 Pblshd="2026-01-01">
  <HstrcCcyTbl>
    <HstrcCcyNtry>
      <CtryNm>AFGHANISTAN</CtryNm>
      <CcyNm>Afghani</CcyNm>
      <Ccy>AFA</Ccy>
      <CcyNbr>004</CcyNbr>
      <WthdrwlDt>2003-01</WthdrwlDt>
    </HstrcCcyNtry>
*/

-- 1ST LEVEL EXPLORATION
SELECT OBJECT_KEYS(xml_content) AS root_keys
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1; -- ["$","@","@Pblshd","CcyTbl"]

SELECT OBJECT_KEYS(xml_content:"$") AS keys_in_root
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 2; -- ["$","@","CcyNtry"] for 1st file, ["$","@","HstrcCcyNtry"] for 2nd file


-- 2ND LEVEL EXPLORATION
SELECT TYPEOF(xml_content:"$"."$") AS type_of_2nd_level_root_element
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1;-- array

SELECT
  OBJECT_KEYS(entry.value) AS entry_keys
FROM nbu_exchange.bronze.iso4217_xml_raw,
LATERAL FLATTEN(input => xml_content:"$"."$") AS entry
LIMIT 1;
/*
["$","@","Ccy","CcyMnrUnts","CcyNbr","CcyNm","CtryNm"]
 */

SELECT OBJECT_KEYS(xml_content:"$"."$") AS keys_in_array
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1; -- error 
/*
[{"$":[{"$":"AFGHANISTAN","@":"CtryNm"},
       {"$":"Afghani","@":"CcyNm"},
       {"$":"AFN","@":"Ccy"},
       {"$":971,"@":"CcyNbr"},
*/

SELECT entry.value AS value_of_2nd_level_root_element
FROM nbu_exchange.bronze.iso4217_xml_raw,
LATERAL FLATTEN(input => xml_content:"$"."$") AS entry
LIMIT 1;

/*
<CcyNtry>
  <CtryNm>AFGHANISTAN</CtryNm>
  <CcyNm>Afghani</CcyNm>
  <Ccy>AFN</Ccy>
  <CcyNbr>971</CcyNbr>
  <CcyMnrUnts>2</CcyMnrUnts>
</CcyNtry>
*/

SELECT OBJECT_KEYS(entry_value) AS root_keys
FROM (
SELECT
  entry.value AS entry_value
FROM nbu_exchange.bronze.iso4217_xml_raw,
LATERAL FLATTEN(input => xml_content:"$"."$") AS entry
LIMIT 1)
;
-- ["$","@","Ccy","CcyMnrUnts","CcyNbr","CcyNm","CtryNm"]

SELECT entry_value:"$" AS entry_value_extracted
FROM (
SELECT
  entry.value AS entry_value
FROM nbu_exchange.bronze.iso4217_xml_raw,
LATERAL FLATTEN(input => xml_content:"$"."$") AS entry
LIMIT 2)
;

/*
[
{"$":"AFGHANISTAN","@":"CtryNm"},
{"$":"Afghani","@":"CcyNm"},
{"$":"AFN","@":"Ccy"},
{"$":971,"@":"CcyNbr"},
{"$":2,"@":"CcyMnrUnts"}
]
 */

/* xml_content (VARIANT)
│
├── "$" (object: content of root XML element)
│    │
│    ├── "$" (array: each element is a currency entry object)
│    │    │
│    │    ├── [1] (object: one currency entry)
│    │    │    ├── "$" (array: each element is a field object)
│    │    │    │    ├── [1] {"$": "AFGHANISTAN", "@": "CtryNm"}
│    │    │    │    ├── [2] {"$": "Afghani", "@": "CcyNm"}
│    │    │    │    ├── [3] {"$": "AFN", "@": "Ccy"}
│    │    │    │    ├── [4] {"$": 971, "@": "CcyNbr"}
│    │    │    │    └── [5] {"$": 2, "@": "CcyMnrUnts"}
│    │    │    └── ... (other keys: field positions, etc.)
│    │    ├── [2] (object: next currency entry)
│    │    │    ...
│    │    └── ...
│    │
│    └── "@" (attributes at this level, if any)
│
├── "@" (attributes of root XML element)
├── "@Pblshd" (publish date)
└── "CcyTbl" (parsing artifact, not used)
*/

-- 3D LEVEL EXPLORATION

-- 1st file
WITH root_level_extraction AS (
SELECT xml_content:"$" AS root_value
FROM nbu_exchange.bronze.iso4217_xml_raw
WHERE file_name = 'list-one.xml'
LIMIT 1
),
second_level_extraction AS (
SELECT entry.value AS entry_value
FROM root_level_extraction,
LATERAL FLATTEN(input => root_value:"$") AS entry
),
final_extraction AS (
SELECT
  MAX(CASE WHEN field.value:"@" = 'CtryNm' THEN field.value:"$" END) AS country_name,
  MAX(CASE WHEN field.value:"@" = 'CcyNm' THEN field.value:"$" END) AS currency_name,
  MAX(CASE WHEN field.value:"@" = 'Ccy' THEN field.value:"$" END) AS currency_code,
  MAX(CASE WHEN field.value:"@" = 'CcyNbr' THEN field.value:"$" END) AS currency_number,
  MAX(CASE WHEN field.value:"@" = 'CcyMnrUnts' THEN field.value:"$" END) AS minor_units
FROM second_level_extraction,
LATERAL FLATTEN(input => entry_value:"$") AS field
GROUP BY entry_value
)
SELECT *
FROM final_extraction
WHERE currency_number LIKE '%N.A.%' OR minor_units LIKE '%N.A.%'
;

-- 2nd file
WITH root_level_extraction AS (
SELECT xml_content:"$" AS root_value
FROM nbu_exchange.bronze.iso4217_xml_raw
WHERE file_name = 'list-three.xml'
LIMIT 1
),
second_level_extraction AS (
SELECT entry.value AS entry_value
FROM root_level_extraction,
LATERAL FLATTEN(input => root_value:"$") AS entry
),
final_extraction AS (
SELECT
  MAX(CASE WHEN field.value:"@" = 'CtryNm' THEN field.value:"$" END) AS country_name,
  MAX(CASE WHEN field.value:"@" = 'CcyNm' THEN field.value:"$" END) AS currency_name,
  MAX(CASE WHEN field.value:"@" = 'Ccy' THEN field.value:"$" END) AS currency_code,
  MAX(CASE WHEN field.value:"@" = 'CcyNbr' THEN field.value:"$" END) AS currency_number,
  MAX(CASE WHEN field.value:"@" = 'WthdrwlDt' THEN field.value:"$" END) AS withdrawal_date
FROM second_level_extraction,
LATERAL FLATTEN(input => entry_value:"$") AS field
GROUP BY entry_value
)
SELECT *
FROM final_extraction
WHERE currency_number LIKE '%N.A.%' OR withdrawal_date LIKE '%N.A.%'
;

-- FINAL QUERY TO UNITE EXTRACTIONS AND POPULATE SILVER TABLE
CREATE OR REPLACE TABLE nbu_exchange.silver.iso_4217_currencies (
  country_name STRING,
  currency_name STRING,
  currency_code STRING,
  currency_number INTEGER,
  minor_units INTEGER,
  withdrawal_date STRING
);

INSERT INTO nbu_exchange.silver.iso_4217_currencies (
  country_name, 
  currency_name, 
  currency_code, 
  currency_number, 
  minor_units, 
  withdrawal_date 
)
WITH
-- Current currencies
current_root AS (
  SELECT xml_content:"$" AS root_value
  FROM nbu_exchange.bronze.iso4217_xml_raw
  WHERE file_name = 'list-one.xml'
  LIMIT 1
),
current_entries AS (
  SELECT entry.value AS entry_value
  FROM current_root,
  LATERAL FLATTEN(input => root_value:"$") AS entry
),
-- Historic currencies
historic_root AS (
  SELECT xml_content:"$" AS root_value
  FROM nbu_exchange.bronze.iso4217_xml_raw
  WHERE file_name = 'list-three.xml'
  LIMIT 1
),
historic_entries AS (
  SELECT entry.value AS entry_value
  FROM historic_root,
  LATERAL FLATTEN(input => root_value:"$") AS entry
)
SELECT
  MAX(CASE WHEN field.value:"@" = 'CtryNm' THEN field.value:"$" END) AS country_name,
  MAX(CASE WHEN field.value:"@" = 'CcyNm' THEN field.value:"$" END) AS currency_name,
  MAX(CASE WHEN field.value:"@" = 'Ccy' THEN field.value:"$" END) AS currency_code,
  MAX(CASE WHEN field.value:"@" = 'CcyNbr' THEN field.value:"$" END) AS currency_number,
  MAX(
  CASE 
    WHEN field.value:"@" = 'CcyMnrUnts' THEN TRY_TO_NUMBER(field.value:"$"::STRING)
    ELSE NULL
  END
  ) AS minor_units,
  NULL AS withdrawal_date
FROM current_entries,
LATERAL FLATTEN(input => entry_value:"$") AS field
GROUP BY entry_value

UNION ALL

SELECT
  MAX(CASE WHEN field.value:"@" = 'CtryNm' THEN field.value:"$" END) AS country_name,
  MAX(CASE WHEN field.value:"@" = 'CcyNm' THEN field.value:"$" END) AS currency_name,
  MAX(CASE WHEN field.value:"@" = 'Ccy' THEN field.value:"$" END) AS currency_code,
  MAX(CASE WHEN field.value:"@" = 'CcyNbr' THEN field.value:"$" END) AS currency_number,
  NULL AS minor_units,
  MAX(CASE WHEN field.value:"@" = 'WthdrwlDt' THEN field.value:"$" END) AS withdrawal_date
FROM historic_entries,
LATERAL FLATTEN(input => entry_value:"$") AS field
GROUP BY entry_value
;

SELECT *
FROM nbu_exchange.silver.iso_4217_currencies;