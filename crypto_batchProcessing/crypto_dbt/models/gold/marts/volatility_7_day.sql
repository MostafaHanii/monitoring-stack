WITH base_data AS (
    SELECT
        TRANSACTION_DATE,
        PAIR_SYMBOL,
        PRICE
    FROM {{ ref('trades_fct') }} -- Reference your fact table here
)

SELECT
    TRANSACTION_DATE,
    PAIR_SYMBOL,
    PRICE,
    -- 7-day standard deviation (volatility) for PRICE
    STDDEV(PRICE) OVER (
        PARTITION BY PAIR_SYMBOL
        ORDER BY TRANSACTION_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS volatility_7_day
FROM base_data
