-- ================================================================
-- LARGE TRADES - 3X QUANTITY THRESHOLD
-- ================================================================
WITH avg_trade_size AS (
    SELECT 
        t.base_asset_fk,                       
        t.pair_symbol,                         
        d.year AS trade_year,                  
        d.month AS trade_month,                
        AVG(t.quantity) AS avg_quantity,       
        AVG(t.quantity * t.price) AS avg_trade_value  
    FROM {{ ref('trades_fct') }} t  
    JOIN {{ref('date_dim')}} d ON d.date_sk = t.date_fk  
    GROUP BY t.base_asset_fk, t.pair_symbol, d.year, d.month
)

SELECT      
    t.pair_symbol,                      
    d.year AS trade_year,               
    d.month AS trade_month,            
    t.quantity AS large_trades_quantity, 
    t.price,                            
    (t.quantity * t.price) AS trade_value_usdt,   
    ats.avg_quantity,                  
    ats.avg_trade_value                 
FROM {{ ref('trades_fct') }} t  
JOIN {{ref('date_dim')}} d ON d.date_sk = t.date_fk    
JOIN avg_trade_size ats  
    ON t.base_asset_fk = ats.base_asset_fk
    AND d.year = ats.trade_year
    AND d.month = ats.trade_month
WHERE t.quantity >= ats.avg_quantity * 5  
ORDER BY d.year ASC, d.month ASC,t.pair_symbol
