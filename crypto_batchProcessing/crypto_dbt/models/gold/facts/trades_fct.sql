{{
    config(
        materialized='incremental',
        schema='gold',
        on_schema_change='fail'
    )
}}

WITH base AS (
        SELECT t.agg_trade_id,
            t.pair_symbol,
            t.price ,  
            t.quantity ,  
            t.first_trade_id,
            t.last_trade_id,
            t.transaction_date,
            t.transaction_time,
            LEFT(t.pair_symbol, POSITION('USDT' IN t.pair_symbol) - 1) AS base_asset,
            RIGHT(t.pair_symbol, LENGTH(t.pair_symbol) - POSITION('USDT' IN t.pair_symbol) + 1) AS quote_asset,
            t.transaction_date AS date_key,
            t.transaction_time AS time_key
    FROM {{ ref("stg_agg_trades") }} t
    {% if is_incremental() %}
    WHERE t.transaction_date > (
        SELECT max(transaction_date)
        FROM {{ this }}
        WHERE pair_symbol = t.pair_symbol   
    )
    {% endif %}

),
dedup AS (
    SELECT * 
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY agg_trade_id, pair_symbol, price ORDER BY agg_trade_id) AS rn
        FROM base
    )
    WHERE rn = 1
)

SELECT  
    {{ dbt_utils.generate_surrogate_key(['agg_trade_id', 'pair_symbol', 'price']) }} AS trades_sk,
    dd.agg_trade_id,
    dd.pair_symbol,
    base_a.asset_sk AS base_asset_fk,  -- base_asset_fk
    quote_a.asset_sk AS quote_asset_fk, -- quote_asset_fk
    dd.price,  
    dd.quantity,
    dd.first_trade_id,
    dd.last_trade_id,
    dd.transaction_date,
    dd.transaction_time,
    d.date_sk AS date_fk, 
    ti.time_sk AS time_fk
FROM dedup dd
-- Join for base asset to get base_asset_fk
LEFT JOIN {{ ref("assets_dim") }} base_a          
    ON base_a.symbol = dd.base_asset  
-- Join for quote asset to get quote_asset_fk
LEFT JOIN {{ ref("assets_dim") }} quote_a         
    ON quote_a.symbol = dd.quote_asset
LEFT JOIN {{ ref('date_dim') }} d         
    ON d.full_date = dd.transaction_date
LEFT JOIN {{ ref('time_dim') }} ti
    ON DATE_TRUNC('second', ti.time_of_day) = DATE_TRUNC('second', dd.transaction_time)
