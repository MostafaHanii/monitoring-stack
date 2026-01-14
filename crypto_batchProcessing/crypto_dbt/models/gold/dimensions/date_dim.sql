{{
    config(
        materialized='table',
        schema='gold'
    )
}}
{% set date_start = var('date_start', '2019-01-01') %}
{% set date_end = var('date_end', '2027-12-31') %}

WITH date_seed AS (
    SELECT * FROM {{ source('raw_data', 'date') }}
),

date_dimension AS (

  SELECT
    date_sk,
    date as full_date,
    day,
    month,
    monthname,
    year,

    -- suffix of day like th ,st ,......
    daysuffix,
    dayofweek as dayname,

    -- occurence of day in the month
    dowinmonth as weekday_of_month, 
    dayofyear,
    weekofyear,
    weekofmonth,
    quarter,
    quartername,

    -- model metadata
    '{{ date_start }}'                                                  AS dimension_start_date,
    '{{ date_end }}'                                                    AS dimension_end_date,
    CURRENT_TIMESTAMP()                                                 AS created_at

  FROM date_seed

)

SELECT * FROM date_dimension