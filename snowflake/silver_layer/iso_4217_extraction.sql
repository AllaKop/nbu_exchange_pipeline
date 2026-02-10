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

SELECT OBJECT_KEYS(xml_content) AS root_keys
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1; -- ["$","@","@Pblshd","CcyTbl"]

SELECT OBJECT_KEYS(xml_content:"$") AS keys_in_root
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1; 
/*
[
  "$",
  "@",
  "CcyNtry"
]
*/

SELECT TYPEOF(xml_content:"$"."$") AS type_of_dollar_dollar
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

SELECT OBJECT_KEYS(xml_content:"$"."$") AS keys_in_dollar
FROM nbu_exchange.bronze.iso4217_xml_raw
LIMIT 1; -- error 
/*
[{"$":[{"$":"AFGHANISTAN","@":"CtryNm"},
       {"$":"Afghani","@":"CcyNm"},
       {"$":"AFN","@":"Ccy"},
       {"$":971,"@":"CcyNbr"},
*/

/* xml_content 
    → "$" (object) 
        → "$" (array of entries) 
            → each entry: "$" (array of field values), plus field name indexes
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



/*
<CcyNtry>
  <CtryNm>AFGHANISTAN</CtryNm>
  <CcyNm>Afghani</CcyNm>
  <Ccy>AFN</Ccy>
  <CcyNbr>971</CcyNbr>
  <CcyMnrUnts>2</CcyMnrUnts>
</CcyNtry>
*/







CREATE OR REPLACE TABLE nbu_exchange.silver.iso_4217_currencies (
  country_name STRING,
  currency_name STRING,
  currency_code STRING,
  currency_number INTEGER,
  minor_units INTEGER,
  withdrawal_date DATE,
  source_type STRING
);

INSERT INTO nbu_exchange.silver.iso_4217_currencies (
  country_name, 
  currency_name, 
  currency_code, 
  currency_number, 
  minor_units, 
  withdrawal_date, 
  source_type
)
-- Current currencies
SELECT
  entry.value:CtryNm::string AS country_name,
  entry.value:CcyNm::string AS currency_name,
  entry.value:Ccy::string AS currency_code,
  entry.value:CcyNbr::integer AS currency_number,
  entry.value:CcyMnrUnts::integer AS minor_units,
  NULL AS withdrawal_date,
  'current' AS source_type
FROM nbu_exchange.bronze.iso4217_xml_raw,
     LATERAL FLATTEN(input => xml_content:ISO_4217.CcyTbl.CcyNtry) entry

UNION ALL

-- Historic currencies
SELECT
  entry.value:CtryNm::string AS country_name,
  entry.value:CcyNm::string AS currency_name,
  entry.value:Ccy::string AS currency_code,
  entry.value:CcyNbr::integer AS currency_number,
  NULL AS minor_units,
  TO_DATE(entry.value:WthdrwlDt::string, 'YYYY-MM') AS withdrawal_date,
  'historic' AS source_type
FROM nbu_exchange.bronze.iso4217_xml_raw,
     LATERAL FLATTEN(input => xml_content:ISO_4217.HstrcCcyTbl.HstrcCcyNtry) entry;

SELECT * 
FROM nbu_exchange.silver.iso_4217_currencies
LIMIT 5;