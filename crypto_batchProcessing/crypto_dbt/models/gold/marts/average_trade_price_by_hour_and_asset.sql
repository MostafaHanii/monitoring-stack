WITH base AS (
    SELECT *
    FROM {{ ref('trades_fct') }} 
),
average_trade_price_by_hour AS (
    SELECT
        d.full_date,
        base.pair_symbol,
        t.hour_of_day as trade_hour,  
        AVG(base.price) AS avg_price,  
        MIN(base.price) AS min_price,  
        MAX(base.price) AS max_price   
    FROM base
    JOIN {{ ref('time_dim') }} t
        ON t.time_sk = base.time_fk
    JOIN {{ref('date_dim')}} d
        ON d.date_sk=base.date_fk
    GROUP BY
        d.full_date,
        pair_symbol,
        trade_hour
)
SELECT *
FROM average_trade_price_by_hour