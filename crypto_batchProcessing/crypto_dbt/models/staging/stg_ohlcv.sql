{{ config(
    schema='silver'
) }}

WITH raw_ohlcv 
AS 
(
    select * from {{ source('raw_data', 'ohlcv') }}
),
-- Public streaming OHLCV
streamed_ohlcv AS (
    SELECT * FROM {{ source('streamed_data', 'streamed_ohlcv') }}
),
batch_cleaned 
AS (
    select 
        asset_symbol AS asset_pair_symbol,
        -- split asset symbol to two columns base nad quote
        CASE 
            WHEN ASSET_SYMBOL LIKE '%USDT' THEN REPLACE(ASSET_SYMBOL, 'USDT', '')
            ELSE ASSET_SYMBOL
        END AS base_asset_symbol,
    
        CASE 
            WHEN ASSET_SYMBOL LIKE '%USDT' THEN 'USDT'
            ELSE 'UNKNOWN'
        END AS quote_asset_symbol,
    
        -- date columns handling
        DATE(TO_TIMESTAMP(open_time)) AS open_date,
        TIME(TO_TIMESTAMP(open_time)) AS open_time,
        DATE(TO_TIMESTAMP(close_time)) AS close_date,
        TIME(TO_TIMESTAMP(close_time)) AS close_time,
        
    
        -- remaing columns
        open, 
        high,
        low,
        close,
        volume as base_asset_volume,
        quote_asset_volume,
        num_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume,
        'batch' AS record_source    
    from raw_ohlcv
),
-- =====================================================
-- 2) Number streaming rows per hour
-- =====================================================
streamed_numbered AS (
    SELECT
        asset_symbol AS asset_pair_symbol,

        CASE 
            WHEN asset_symbol LIKE '%USDT' THEN REPLACE(asset_symbol, 'USDT', '')
            ELSE asset_symbol
        END AS base_asset_symbol,

        CASE 
            WHEN asset_symbol LIKE '%USDT' THEN 'USDT'
            ELSE 'UNKNOWN'
        END AS quote_asset_symbol,

        DATE_TRUNC('hour', TO_TIMESTAMP(open_time)) AS hour_bucket,
        TO_TIMESTAMP(open_time)  AS open_ts,
        TO_TIMESTAMP(close_time) AS close_ts,


        open,
        close,
        high,
        low,
        volume AS base_asset_volume,
        quote_asset_volume,
        num_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume,

        ROW_NUMBER() OVER (
            PARTITION BY asset_symbol, DATE_TRUNC('hour', TO_TIMESTAMP(open_time))
            ORDER BY TO_TIMESTAMP(open_time) ASC
        ) AS rn_open,

        ROW_NUMBER() OVER (
            PARTITION BY asset_symbol, DATE_TRUNC('hour', TO_TIMESTAMP(open_time))
            ORDER BY TO_TIMESTAMP(close_time) DESC
        ) AS rn_close

    FROM streamed_ohlcv
),

-- =====================================================
-- 3) Hourly open & close (streaming)
-- =====================================================
streamed_open_close AS (
    SELECT
        asset_pair_symbol,
        base_asset_symbol,
        quote_asset_symbol,
        hour_bucket,

        MAX(CASE WHEN rn_open = 1 THEN open END)  AS open,
        MAX(CASE WHEN rn_close = 1 THEN close END) AS close
    FROM streamed_numbered
    GROUP BY
        asset_pair_symbol,
        base_asset_symbol,
        quote_asset_symbol,
        hour_bucket
),

-- =====================================================
-- 4) Hourly aggregation (streaming)
-- =====================================================
streamed_cleaned AS (
    SELECT
        n.asset_pair_symbol,
        n.base_asset_symbol,
        n.quote_asset_symbol,

        DATE(n.hour_bucket) AS open_date,
        TIME(n.hour_bucket) AS open_time,
        DATE(n.hour_bucket) AS close_date,
        TIME(n.hour_bucket) AS close_time,

        o.open,
        MAX(n.high) AS high,
        MIN(n.low)  AS low,
        o.close,

        SUM(n.base_asset_volume)  AS base_asset_volume,
        SUM(n.quote_asset_volume) AS quote_asset_volume,
        SUM(n.num_trades)         AS num_trades,
        SUM(n.taker_buy_base_volume)  AS taker_buy_base_volume,
        SUM(n.taker_buy_quote_volume) AS taker_buy_quote_volume,

        'streamed' AS record_source
    FROM streamed_numbered n
    JOIN streamed_open_close o
      ON n.asset_pair_symbol = o.asset_pair_symbol
     AND n.hour_bucket = o.hour_bucket
    GROUP BY
        n.asset_pair_symbol,
        n.base_asset_symbol,
        n.quote_asset_symbol,
        n.hour_bucket,
        o.open,
        o.close
),

-- =====================================================
-- 5) Union batch + streamed
-- =====================================================
unioned AS (
    SELECT * FROM batch_cleaned
    UNION ALL
    SELECT * FROM streamed_cleaned
),

-- =====================================================
-- 6) Deduplicate (prefer streamed)
-- =====================================================
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY asset_pair_symbol, open_date, open_time
            ORDER BY
                CASE WHEN record_source = 'streamed' THEN 1 ELSE 2 END
        ) AS rn
    FROM unioned
)

-- =====================================================
-- Final result
-- =====================================================
SELECT
    asset_pair_symbol,
    base_asset_symbol,
    quote_asset_symbol,
    open_date,
    open_time,
    close_date,
    close_time,
    open,
    high,
    low,
    close,
    base_asset_volume,
    quote_asset_volume,
    num_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    record_source
FROM deduplicated
WHERE rn = 1