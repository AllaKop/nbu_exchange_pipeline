USE ROLE DATA_ENGINEER;
USE SCHEMA NBU_EXCHANGE.SILVER;

-- Need to do:
-- 0. deduplicate - after cleaning and mapping!
-- 3. currency_code upper case maybe
-- 4. currency_name has nulls (IS null), map
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
-- add collumn if records valid

-- Create a new table for cleaned and transformed data


select distinct currency_code, currency_name
from nbu_exchange.silver.exchange_rate_extracted
group by currency_code, currency_name
having currency_name is null
;

select distinct currency_code, currency_name, currency_name_ua, r030_code
from nbu_exchange.silver.exchange_rate_extracted
where currency_name is null
order by currency_code;

select distinct currency_code, currency_name, currency_name_ua, r030_code
from nbu_exchange.silver.exchange_rate_extracted
where currency_name is null
order by r030_code, currency_code;

select distinct currency_code, currency_name, currency_name_ua, r030_code
from nbu_exchange.silver.exchange_rate_extracted
where currency_code = 'SDR' or currency_code = 'XDR' or r030_code = 960
;

-- function to cast string format columns with dates to date format
CREATE OR REPLACE FUNCTION nbu_exchange.silver.cast_to_date_type(data_column STRING)
    RETURNS DATE
    AS
    $$
        CASE
            WHEN trim(data_column) = '' OR data_column IS NULL 
            THEN TO_DATE('01.01.1900', 'DD.MM.YYYY')
            ELSE 
                TO_DATE(TRIM(data_column), 'DD.MM.YYYY')
        END
    $$
    ;

CREATE OR REPLACE PROCEDURE nbu_exchange.silver.clean_exchange_rate()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
        INSERT INTO nbu_exchange.silver.exchange_rate_extracted
        SELECT 
            nbu_exchange.silver.cast_to_date_type(calculation_date) AS calculation_date,
            CASE
                WHEN trim(upper(currency_code)) == 'SDR' THEN currency_code = 'XDR'
                WHEN currency_name is null AND trim(upper(currency_code)) == 'XDR' THEN currency_name = 'SDR (Special Drawing Right)'
                WHEN trim(upper(currency_code)) == 'XDR' THEN currency_name_ua = 'СПЗ (спеціальні права запозичення)'
                WHEN trim(upper(currency_code)) == 'XDR' THEN r030_code = 960
            END 

        FROM nbu_exchange.silver.exchange_rate_extracted -- after 1st run, change to stream
        RETURN 'Clean data inserted'
    $$;



-- function to 
CREATE OR REPLACE FUNCTION nbu_exchange.silver.
    RETURNS 
    AS
    $$
    
    $$
    ;

SELECT *
FROM nbu_exchange.silver.exchange_rate_extracted
limit 3;

