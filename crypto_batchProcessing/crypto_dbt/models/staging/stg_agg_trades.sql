{{ config(
    schema='silver'
) }}

WITH raw_agg_trades AS (
    SELECT * 
    FROM {{ source('raw_data', 'agg_trades') }}

)

SELECT 
    agg_trade_id,
    pair_symbol,
    price,
    quantity,
    first_trade_id,
    last_trade_id,
    DATE(TO_TIMESTAMP(transact_time / 1000)) AS transaction_date,  
    TIME(TO_TIMESTAMP(transact_time / 1000)) AS transaction_time  
FROM raw_agg_trades