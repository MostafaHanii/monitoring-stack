SELECT 
    ti.hour_of_day,
    d.dayname,
    CASE 
        WHEN d.dayname IN ('Saturday', 'Sunday') THEN 1 
        ELSE 0 
    END as is_weekend, 
    t.pair_symbol,
    SUM(t.last_trade_id - t.first_trade_id + 1) as trade_count,
    SUM(t.quantity) as hourly_volume,
    AVG(t.price) as avg_hourly_price,
    STDDEV(t.price) as price_volatility
FROM {{ref('trades_fct')}} t
LEFT JOIN {{ref('time_dim')}} ti ON t.time_fk = ti.time_sk
LEFT JOIN {{ref('date_dim')}} d ON t.date_fk = d.date_sk
GROUP BY ti.hour_of_day, d.dayname, is_weekend, t.pair_symbol
ORDER BY t.pair_symbol, is_weekend, d.dayname, ti.hour_of_day