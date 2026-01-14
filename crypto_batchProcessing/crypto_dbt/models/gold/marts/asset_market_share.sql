WITH total_trading_value AS (
    SELECT 
        base_asset_fk,
        d.year AS trade_year,
        d.month AS trade_month,
        SUM(base.quantity * base.price) AS total_value_traded
    FROM {{ ref('trades_fct') }} base
    JOIN {{ ref('date_dim') }} d
        ON d.date_sk= base.date_fk
    GROUP BY base_asset_fk, trade_year, trade_month
),
total_market_value AS (
    SELECT 
        trade_year,
        trade_month,
        SUM(total_value_traded) AS total_market_value
    FROM total_trading_value
    GROUP BY trade_year, trade_month
)
SELECT 
    t.trade_year,
    t.trade_month,
    a.asset_name,
    t.total_value_traded,
    m.total_market_value,
    (t.total_value_traded / m.total_market_value) * 100 AS asset_market_share_percentage  
FROM total_trading_value t
JOIN total_market_value m
    ON t.trade_year = m.trade_year
    AND t.trade_month = m.trade_month  
JOIN {{ ref('assets_dim') }} a
    ON t.base_asset_fk = a.asset_sk
ORDER BY t.trade_year, t.trade_month, a.asset_name