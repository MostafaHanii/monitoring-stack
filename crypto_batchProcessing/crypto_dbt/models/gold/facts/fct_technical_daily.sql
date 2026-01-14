-- models/marts/gold/fct_technical_daily.sql
/*
fct_technical_daily
Compute common technical indicators at daily resolution for each asset:
- SMA10, SMA24 (moving averages)
- MACD (EMA12 - EMA26 approximated using SMA windows for PoC)
- MACD signal (SMA of MACD)
- RSI14 (SMA approach)
- ATR14 (if needed; reuses TR from fct_ohlcv_daily)
- MA crossover event flag (golden_cross / death_cross)
*/
{{ config(
    materialized='incremental',
    unique_key = 'technical_sk',
    schema = 'gold',
    cluster_by = ['open_date', 'asset_pair_symbol']
) }}


with src as (
    select *
    from {{ ref('fct_ohlcv_daily') }}
    {% if is_incremental() %}
        where open_date >
            (select coalesce(max(open_date), '1970-01-01'::date) from {{ this }})
    {% endif %}
),

simple_tech as (
    select
      *,
      -- 1. 50-day average price (the big slow line)
      avg(close) over (
            partition by asset_pair_symbol
            order by open_date
            rows between 49 preceding and current row
      ) as price_avg_50,

      -- 2. 200-day average price (the "long-term" line everyone watches)
      avg(close) over (
          partition by asset_pair_symbol
          order by open_date
          rows between 199 preceding and current row
      ) as price_avg_200,
          
    from src
),

crossover as (
    select
        *,
        -- Crossovers suggest momentum changes.
        -- 3. Did the fast line cross above the slow line? (Golden Cross = good sign)
        case
            when price_avg_50 > price_avg_200
             and lag(price_avg_50) over (partition by asset_pair_symbol order by open_date) <= lag(price_avg_200) over (partition by asset_pair_symbol order by open_date)
            then 'GOLDEN CROSS - BIG BULL SIGNAL'
            when price_avg_50 < price_avg_200
             and lag(price_avg_50) over (partition by asset_pair_symbol order by open_date) >= lag(price_avg_200) over (partition by asset_pair_symbol order by open_date)
            then 'DEATH CROSS - BIG BEAR SIGNAL'
            else 'NO BIG CROSS TODAY'
        end as ma_crossover_event
    from simple_tech
)

select
    {{ dbt_utils.generate_surrogate_key(['asset_pair_symbol','open_date']) }} as technical_sk,
    asset_pair_symbol,
    base_asset_fk,
    open_date,
    
    price_avg_50,
    price_avg_200,
    ma_crossover_event
from crossover
