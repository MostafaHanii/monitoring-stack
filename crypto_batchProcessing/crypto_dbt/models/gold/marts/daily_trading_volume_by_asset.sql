WITH base AS (
    SELECT *
    FROM {{ ref('trades_fct') }} 
),
-- Get daily trading volume and value per asset
daily_trading_volume_by_asset AS (
    SELECT
        base.date_fk,
        base.transaction_date,
        base.pair_symbol,
        SUM(base.quantity) AS total_quantity_traded,
        SUM(base.quantity * base.price) AS total_value_traded,
        SUM(base.last_trade_id - base.first_trade_id + 1) AS total_trades,
        AVG(base.price) AS avg_price,  
        MIN(base.price) AS min_price,  
        MAX(base.price) AS max_price  
    FROM base
    JOIN {{ ref('date_dim') }} d
        ON d.date_sk = base.date_fk
    GROUP BY
        base.date_fk,
        pair_symbol,
        base.transaction_date
    
)

SELECT *
FROM daily_trading_volume_by_asset