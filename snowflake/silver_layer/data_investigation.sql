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

select distinct currency_code, currency_name, currency_name_ua
from nbu_exchange.silver.exchange_rate_extracted
where group_number = 1;

select distinct currency_code, currency_name, currency_name_ua
from nbu_exchange.silver.exchange_rate_extracted
where group_number = 2;

select distinct currency_code, currency_name, currency_name_ua
from nbu_exchange.silver.exchange_rate_extracted
where group_number = 3; -- precious metals

select distinct currency_code, currency_name, currency_name_ua, group_number
from nbu_exchange.silver.exchange_rate_extracted
where group_number = 1 or group_number = 2
order by currency_code;

select rate
from nbu_exchange.silver.exchange_rate_extracted
where rate is null or rate = 0 or rate like '%,%' or rate < 0;

select rate_per_unit
from nbu_exchange.silver.exchange_rate_extracted
where rate_per_unit is null or rate_per_unit = 0 or rate_per_unit like '%,%' or rate_per_unit < 0;

select distinct special_conditions
from nbu_exchange.silver.exchange_rate_extracted
;

select distinct special_conditions
from nbu_exchange.silver.exchange_rate_extracted
where trim(upper(currency_code)) != 'USD'
;

select distinct currency_name_ua
from nbu_exchange.silver.exchange_rate_extracted
where trim(currency_name_ua) = '' or trim(lower(currency_name_ua)) = 'null' or currency_name_ua is null
;

select count(distinct currency_name_ua), count(distinct currency_name)
from nbu_exchange.silver.exchange_rate_extracted;

select distinct currency_name_ua, currency_name
from nbu_exchange.silver.exchange_rate_extracted;

select distinct units
from nbu_exchange.silver.exchange_rate_extracted
;

select distinct units
from nbu_exchange.silver.exchange_rate_extracted
where units = 0 or units is null or units < 1
;



-- Need to do:
-- 0. deduplicate - after cleaning and mapping!
-- 1. Calculation_date cast to date format
-- 2. calculation_date has empty values ' ' (inside one space)
-- 3. currency_code upper case maybe
-- 4. currency_name has nulls (IS null), map
-- 5. exchange_date cast to date format
-- 6. group_number (1,2,3) find what's the meaning, map with names
-- 7. r030_code - 150 r030 count, 109 currency count?
-- 8. r030_code - 3d group is precious metals, 2nd and 1st the same currency?
-- 9. rate - seems ok
-- 10. rate_per_unit - seems ok
-- 11. special_conditions - seems ok
-- Special=null/Y/N - sign of the conditions for calculating the hryvnia to US dollar 
-- exchange rate: null - for records for days when the sign was not determined and for
-- valcode≠usd, Y - under special conditions, N - under normal conditions;
-- 12. currency_name_ua - seems ok, can add additional column with countries 
-- 13. units - seems ok



select *
from nbu_exchange.silver.exchange_rate_extracted
where exchange_date LIKE '%.02.2026'
order by exchange_date desc
;