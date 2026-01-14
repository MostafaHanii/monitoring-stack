WITH classified_data AS (
    SELECT 
        pair_symbol,
        price * quantity AS trade_value,
        CASE 
            WHEN price * quantity < 100 THEN 'Micro (<$100)'
            WHEN price * quantity < 1000 THEN 'Small ($100-1K)'
            WHEN price * quantity < 10000 THEN 'Medium ($1K-10K)'
            WHEN price * quantity < 100000 THEN 'Large ($10K-100K)'
            ELSE 'Whale (>$100K)'
        END AS trade_size_category,
        quantity,
        price,
        transaction_date
    FROM {{ref('trades_fct')}}
)
, aggregated_data AS (
    SELECT 
        pair_symbol,
        COUNT(*) AS total_trades,
        SUM(CASE WHEN trade_size_category = 'Micro (<$100)' THEN 1 ELSE 0 END) AS micro_trades,
        SUM(CASE WHEN trade_size_category = 'Small ($100-1K)' THEN 1 ELSE 0 END) AS small_trades,
        SUM(CASE WHEN trade_size_category = 'Medium ($1K-10K)' THEN 1 ELSE 0 END) AS medium_trades,
        SUM(CASE WHEN trade_size_category = 'Large ($10K-100K)' THEN 1 ELSE 0 END) AS large_trades,
        SUM(CASE WHEN trade_size_category = 'Whale (>$100K)' THEN 1 ELSE 0 END) AS whale_trades
    FROM classified_data
    GROUP BY pair_symbol
)
SELECT * FROM aggregated_data