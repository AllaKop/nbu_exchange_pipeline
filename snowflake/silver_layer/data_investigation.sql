USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.SILVER;

select *
from nbu_exchange.bronze.exchange_rate_raw
limit 1
;

SELECT
  value:calcdate::string AS calculation_date_str,
  value:exchangedate::string AS exchange_date_str
FROM
  nbu_exchange.bronze.exchange_rate_raw,
  LATERAL FLATTEN(input => raw) AS f
WHERE
  value:calcdate::string = '03.02.2026';

select count(*)
from nbu_exchange.silver.exchange_rate_extracted
;

select 
    calculation_date,
    currency_code,
    currency_name,
    exchange_date,
    group_number,
    r030_code,
    rate,
    rate_per_unit,
    special_conditions,
    currency_name_ua,
    units,
    count(*)
from nbu_exchange.silver.exchange_rate_extracted
group by calculation_date,
    currency_code,
    currency_name,
    exchange_date,
    group_number,
    r030_code,
    rate,
    rate_per_unit,
    special_conditions,
    currency_name_ua,
    units
having count(*) > 1;

select * 
from nbu_exchange.silver.exchange_rate_extracted
where calculation_date = ' '
limit 1
;

select count(distinct currency_code)
from nbu_exchange.silver.exchange_rate_extracted
;

select distinct currency_code
from nbu_exchange.silver.exchange_rate_extracted
where currency_code = ' ' or currency_code = '' or currency_code is null or trim(lower(currency_code)) = 'null'
;

select distinct currency_name
from nbu_exchange.silver.exchange_rate_extracted
where trim(currency_name) = '' or trim(lower(currency_name)) = 'null' or currency_name is null
;

select distinct exchange_date
from nbu_exchange.silver.exchange_rate_extracted
where trim(exchange_date) = '' or trim(lower(exchange_date)) = 'null' or exchange_date is null
;

select group_number, count(distinct currency_code) as distinct_currency_count
from nbu_exchange.silver.exchange_rate_extracted
group by group_number 
;

select count(distinct currency_code) as currency_count, count(distinct r030_code) as r030_count
from nbu_exchange.silver.exchange_rate_extracted
;

-- Need to do:
-- 0. deduplicate
-- 1. Calculation_date cast to date format
-- 2. calculation_date has empty values ' ' (inside one space)
-- 3. currency_code upper case maybe
-- 4. currency_name has nulls (IS null), map
-- 5. exchange_date cast to date format
-- 6. group_number (1,2,3) find what's the meaning, map with names
-- 7. r030_code - 150 r030 count, 109 currency count?
-- 8. r030_code - 



select *
from nbu_exchange.silver.exchange_rate_extracted
limit 5
;