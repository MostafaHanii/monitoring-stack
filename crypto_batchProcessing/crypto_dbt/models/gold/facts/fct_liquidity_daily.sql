/*
fct_liquidity_daily.sql

Daily liquidity fact (gold layer) — standard, non-advanced measures.
- Uses fct_ohlcv_daily as source.
- Provides simple liquidity & trade-behavior metrics for dashboards.

Fields:
- liquidity_usd: quote_asset_volume (preferred liquidity metric in USD/USDT)
- avg_trade_size_usd: quote_asset_volume / num_trades (proxy for whale vs retail)
- price_range_pct: (high - low) / close (intraday volatility relative to price)
- buy_liquidity_usd: taker_buy_quote_volume (market buy volume in quote)
- sell_liquidity_usd: liquidity_usd - buy_liquidity_usd (approx. market sell volume)
- buy_pressure_ratio: buy_liquidity_usd / liquidity_usd (0-1)
*/


{{ config(
    materialized = 'incremental',
    schema = 'gold',
    unique_key = 'liquidity_sk',
    cluster_by = ['open_date', 'base_asset_fk']
) }}

---------------------------------------------------------------
-- 1. Base Input (Daily OHLCV)
---------------------------------------------------------------
with base as (
    select
        asset_pair_symbol,
        base_asset_fk,
        quote_asset_fk,
        base_asset_symbol,
        quote_asset_symbol,
        open_date,
        open_date_fk,
        open, high, low, close,
        base_asset_volume,
        quote_asset_volume,
        num_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume
    from {{ ref('fct_ohlcv_daily') }}

    {% if is_incremental() %}
        where open_date >
            (select coalesce(max(open_date), '1970-01-01'::date) from {{ this }})
    {% endif %}
),

---------------------------------------------------------------
-- 2. Core Calculations (Liquidity + Candle Behavior)
---------------------------------------------------------------
calc as (
    select
        *,
        -- Candle width relative to price (volatility-lite)
        case when close = 0 then null
             else (high - low) / close end as range_pct,

        -- USD-sized trades
        case when num_trades = 0 then null
             else quote_asset_volume / num_trades end as avg_trade_size_usd,

        -- Effective liquidity = USD traded in a day
        quote_asset_volume as liquidity_usd,

        -- Buy-side liquidity
        taker_buy_quote_volume as buy_liquidity_usd,

        -- Sell-side liquidity (approx.)
        case when quote_asset_volume = 0 then null
             else quote_asset_volume - taker_buy_quote_volume end as sell_liquidity_usd,

    from base
),

---------------------------------------------------------------
-- 3. Rolling Windows (Volatility, Volume, Trades)
---------------------------------------------------------------
rolling as (
    select
        *,
        -- Rolling volume avg (20d)
        avg(quote_asset_volume) over (
            partition by asset_pair_symbol
            order by open_date
            rows between 19 preceding and current row
        ) as rolling_volume_20,

        -- Rolling trades avg
        avg(num_trades) over (
            partition by asset_pair_symbol
            order by open_date
            rows between 19 preceding and current row
        ) as rolling_trades_20

    from calc
),

---------------------------------------------------------------
-- 4. Z-Scores & Liquidity Stress no (good for live data)
---------------------------------------------------------------

---------------------------------------------------------------
-- 5. Cross-Coin Liquidity (Top 10 Comparisons)
---------------------------------------------------------------
cross_coin as (
    select
        *,

        -- 1. Was today a busy day for this coin?
        --    > 1 = busier than usual
        --    < 1 = quieter than usual
        --    Example: 3.5 = 3.5 times more money traded than normal
        quote_asset_volume / nullif(rolling_volume_20, 0)   as normalized_liquidity,

        -- 2. How much attention did this coin get from the whole market?
        --    Example: 0.45 = 45% of ALL money traded that day was on this coin
        quote_asset_volume / nullif( sum(quote_asset_volume) over (partition by open_date), 0 )  
        as liquidity_market_share
    from rolling
)

---------------------------------------------------------------
-- 6. Final Select (No intermediate columns exposed)
---------------------------------------------------------------
select
    {{ dbt_utils.generate_surrogate_key(['asset_pair_symbol','open_date']) }} as liquidity_sk,

    asset_pair_symbol,
    base_asset_fk,
    quote_asset_fk,
    base_asset_symbol,
    quote_asset_symbol,
    open_date,
    open_date_fk,

    -- direct OHLCV
    open, high, low, close,
    base_asset_volume,
    quote_asset_volume,
    num_trades,

    -- liquidity metrics
    liquidity_usd,
    avg_trade_size_usd,
    buy_liquidity_usd,
    sell_liquidity_usd,

    -- volatility-lite
    range_pct,

    -- rolling windows
    rolling_volume_20,
    rolling_trades_20,

    -- cross-coin
    normalized_liquidity,
    liquidity_market_share

from cross_coin
