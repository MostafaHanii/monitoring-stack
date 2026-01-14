SELECT 
    t.hour_of_day,
    t.time_division ,
    AVG(tr.quantity) as avg_quantity,
    AVG(tr.price * tr.quantity) as avg_trade_value,
    SUM(tr.last_trade_id - tr.first_trade_id + 1) as trade_frequency
FROM {{ref('trades_fct')}} tr
JOIN {{ref('time_dim')}} t ON tr.time_fk = t.time_sk
GROUP BY t.hour_of_day, t.time_division
ORDER BY t.hour_of_day