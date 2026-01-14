SELECT 
    DATE(TRANSACTION_DATE) as Trade_Date,
    HOUR(TRANSACTION_TIME) as Trade_Hour,
    SUM(last_trade_id - first_trade_id + 1) as Trades_Per_Hour,
    SUM(QUANTITY) as Volume_Per_Hour,
    SUM(PRICE * QUANTITY) as Value_Per_Hour,
    SUM(last_trade_id - first_trade_id + 1) / 60.0 as Trades_Per_Minute
FROM {{ref('trades_fct')}} 
-- NO DATE FILTER - Use all your 2022-2024 data
GROUP BY DATE(TRANSACTION_DATE), HOUR(TRANSACTION_TIME)
ORDER BY Trade_Date, Trade_Hour