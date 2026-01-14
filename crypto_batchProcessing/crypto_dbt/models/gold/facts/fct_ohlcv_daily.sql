/*
Daily OHLCV

daily_open = first hourly open
daily_close = last hourly close
daily_high = max(high)
daily_low = min(low)
daily_volume_base = sum(volume)
daily_volume_quote = sum(quote volume)
daily_num_trades = sum(num_trades)

- Daily return measures done

daily_return_abs = daily_close – daily_open
daily_return_pct
daily_log_return
daily_cumulative_return
daily_volatility_range = (high - low)

Daily classification

daily_trend_direction (UP/DOWN/FLAT) done
daily_volatility_bucket (low/normal/high)
*/

{{ config(
  materialized='table',
  unique_key = 'daily_ohlcv_sk',
  schema = 'gold',
  cluster_by = ['open_date', 'base_asset_fk']
) }}

with source as (
    select * from {{ ref('ohlcv_fct') }}
),
numbered as (
    select
        asset_pair_symbol,
        base_asset_fk,
        quote_asset_fk,
        base_asset_symbol,
        quote_asset_symbol,
        open_date,
        open_date_fk,
        open_time,
        open,
        close,
        high,
        low,
        base_asset_volume,
        quote_asset_volume,
        num_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume,
        row_number() over (
            partition by asset_pair_symbol, open_date
            order by open_time asc
        ) as rn_open,
        row_number() over (
            partition by asset_pair_symbol, open_date
            order by open_time desc
        ) as rn_close
    from source

    
),
-- 2) pick first open and last close per day
daily_open_close as (

    select
        asset_pair_symbol,
        base_asset_fk,
        quote_asset_fk,
        base_asset_symbol,
        quote_asset_symbol,
        open_date,
        open_date_fk,
        max(case when rn_open = 1 then open end) as daily_open,
        max(case when rn_close = 1 then close end) as daily_close
    from numbered
    group by asset_pair_symbol, base_asset_fk, quote_asset_fk,
            base_asset_symbol, quote_asset_symbol, open_date,open_date_fk
),
-- 3) aggregate other daily fields (high/low/volumes/trades)
base as (
    select
        n.base_asset_fk,
        n.quote_asset_fk,
        n.open_date_fk,
        n.asset_pair_symbol,
        n.base_asset_symbol,
        n.quote_asset_symbol,
        n.open_date,
        d.daily_open     as open,
        max(n.high)      as high,
        min(n.low)       as low,
        d.daily_close    as close,
        sum(n.base_asset_volume)  as base_asset_volume,
        sum(n.quote_asset_volume) as quote_asset_volume,
        sum(n.num_trades)         as num_trades,
        sum(n.taker_buy_base_volume)  as taker_buy_base_volume,
        sum(n.taker_buy_quote_volume) as taker_buy_quote_volume

    from numbered n
    join daily_open_close d
        on n.asset_pair_symbol = d.asset_pair_symbol
        and n.open_date = d.open_date
    group by 1,2,3,4,5,6,7,8,11
),
-- 4) attach previous close (needed for TR and gap calculations)
lagged as (
    select
        *,
        lag(close) over (partition by asset_pair_symbol order by open_date) as prev_close
    from base
),
-- 5) returns calculations (absolute, pct and log-return)
returns_ as (
    select
        *,
        -- Absolute daily price move (useful to show USD move magnitude)
        (close - open) as return_abs, -- answers: “Absolute daily price change”. Simple performance measure.
        
         -- Percent return (normalized) - common for period comparisons
        case when open = 0 then null else (close - open) / open end as return_pct, -- answers: “Daily percentage gain/loss”. Normalized performance measure across coins.
        
        -- Log return for compounding and for volatility math
        case when open > 0 and close > 0 then ln(close) - ln(open) else null end as log_return
    from lagged
),
-- 6) cumulative return (compound since series start)
cum_returns as(
    select
        *,
        (
            exp(
                sum(log_return) over (
                    partition by asset_pair_symbol
                    order by open_date
                    rows unbounded preceding
                )
            ) - 1
        ) * 100 as cumulative_return -- “Total return since the beginning”. Shows historical growth trajectory of a coin.
    from returns_
),
-- 7) volatility measures: candle range, True Range (TR), short rolling volatility
volatility as (
    select
        *,
        high - low as candle_range, -- “How much did price move within the day?” Quick measure of intraday risk.
        
        -- TR measures the real movement of price INCLUDING overnight gaps
        {{ true_range('high', 'low', 'prev_close') }} as TR, -- macro true_range(high, low, prev_close)
        
        -- rolling short-term volatility of log returns (10-day stddev)
        stddev(log_return) over (
            partition by asset_pair_symbol
            order by open_date
            rows between 9 preceding and current row
        ) as rolling_vol_10, -- “Short-term volatility”. Detects short-term instability.
 
    from cum_returns
),
-- 8) ATR14: average of TR over 14 days (standard volatility measure)
volatility_atr as(
    select
        *,
        avg(TR) over (
            partition by ASSET_PAIR_SYMBOL
            order by open_date
            rows between 13 preceding and current row
        ) as atr_14, -- “Average True Range”. Measures real volatility including overnight gaps.
    from volatility
),
-- 9) simple volatility bucket: Low / Normal / High using ATR percentiles per asset
classification_cases as(
    select
        *,
        case 
            when return_pct >  0 then 'UP/bullish'          -- Buy during upward trend (bullish)
            when return_pct <  0 then 'DOWN/bearish'        -- Sell or wait during downward trend (bearish)
            else 'SIDEWAYS/flat'                         -- Avoid trading during flat markets (sideways)
        end as trend_direction, -- “Market direction for the day”.
       -- volatility_label buckets: divide each asset's ATR history into thirds
        case
            when atr_14 is null then null
            when atr_14 < percentile_cont(0.33) within group (order by atr_14) over (partition by asset_pair_symbol) then 'Low volatility'
            when atr_14 < percentile_cont(0.66) within group (order by atr_14) over (partition by asset_pair_symbol) then 'Normal volatility'
            else 'High volatility'
        end as volatility_label  -- "daily_volatility_bucket" 
        -- “Which days are unusually calm or volatile?”

    from volatility_atr
)

select
      {{ dbt_utils.generate_surrogate_key(['base_asset_fk', 'open_date']) }} as daily_ohlcv_sk,
      *
from classification_cases

