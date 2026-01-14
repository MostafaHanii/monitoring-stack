-- models/views/vw_gold_market_daily.sql  (materialized view or table)
{{ 
    config(
        materialized='table',
        schema='gold'
    )
    
}}

select
    o.open_date,
    d.day,
    d.month,
    d.monthname,
    d.year,
    d.quarter,
    d.quartername,

    o.asset_pair_symbol,
    o.base_asset_symbol,
    o.quote_asset_symbol,

    a.asset_name,
    a.num_market_pairs,
    a.asset_launch_date,
    a.supply_type,
    a.market_cap_based_rank,
    o.open, o.high, o.low, o.close,
    o.high - o.low as daily_range,        -- add
    o.num_trades,

    o.return_abs, -- close - open
    o.return_pct, -- (close - open) / open
    o.cumulative_return as cumulative_return_pct, -- (exp(sum(log_return)) - 1 )  * 100
    o.atr_14, -- avg(TR) over last 14
    o.TR, 
    o.rolling_vol_10, -- stddev(log_return) over 10 days
    o.trend_direction, -- return_pct >  0 then 'UP/bullish' , return_pct <  0 then 'DOWN/bearish' , else flat
    o.volatility_label, -- atr_14 < percentile_cont(0.33) low , atr_14 < percentile_cont(0.66) normal, else high.


    -- technical
    t.price_avg_50, t.price_avg_200, t.ma_crossover_event,

    -- liquidity
    l.liquidity_usd, -- quote_asset_volume
    l.avg_trade_size_usd, -- quote_asset_volume / num_trades
    l.buy_liquidity_usd, -- taker_buy_quote_volume
    l.sell_liquidity_usd, -- quote_asset_volume - taker_buy_quote_volume
    l.range_pct as price_range_pct, -- (high - low) / close
    l.liquidity_market_share,
    l.normalized_liquidity,
    l.rolling_trades_20, -- avg(quote_asset_volume) rolling avg
    l.rolling_volume_20, -- avg(num_trades) rolling avg

    -- pressure
    p.taker_buy_base_volume,
    p.taker_buy_quote_volume,
    p.instant_buy_ratio_quote,
    p.instant_buy_ratio_base,
    p.passive_quote_volume,
    p.passive_base_volume,
    p.passive_ratio_quote,
    p.passive_ratio_base,
    p.aggression_index, -- abs(taker_buy_base_volume - 0.5) * 2
    p.pressure_regime -- buy_ratio_base >= 0.60 buyers, buy_ratio_base <= 0.40 sellers, else balanced

    
from {{ ref('fct_ohlcv_daily') }} o
left join {{ ref('date_dim') }} d on o.OPEN_DATE_FK = d.date_sk
left join {{ ref('assets_dim') }} a on o.base_asset_fk = a.asset_sk and a.rank_valid_to IS NULL
left join {{ ref('fct_technical_daily') }} t on o.asset_pair_symbol = t.asset_pair_symbol and o.open_date = t.open_date
left join {{ ref('fct_liquidity_daily') }} l on o.asset_pair_symbol = l.asset_pair_symbol and o.open_date = l.open_date
left join {{ ref('daily_pressure_fact') }} p on o.asset_pair_symbol = p.asset_pair_symbol and o.open_date = p.open_date
