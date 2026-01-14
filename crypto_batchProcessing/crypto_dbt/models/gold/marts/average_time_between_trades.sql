-- Liquidity
WITH base AS (
    SELECT *
    FROM {{ ref("trades_fct") }} 
    
),
time_diff AS (
    SELECT
        base.pair_symbol,
        base.transaction_date,
        base.transaction_time,
        d.year AS trade_year,  
        LAG(base.transaction_time) OVER (PARTITION BY base.pair_symbol 
                                        ORDER BY base.transaction_date ASC, base.transaction_time ASC) 
                                        AS previous_trade_time
    FROM base
    LEFT JOIN {{ ref('date_dim') }} d  
    ON d.date_sk = base.date_fk
)
SELECT
    base.pair_symbol,
    base.trade_year, 
    AVG(DATEDIFF('second', previous_trade_time, base.transaction_time))
                    AS avg_time_between_trades_in_seconds --  get the result in minutes
FROM time_diff base
WHERE previous_trade_time IS NOT NULL
        AND previous_trade_time <= base.transaction_time
GROUP BY base.pair_symbol, base.trade_year 
ORDER BY base.pair_symbol, base.trade_year