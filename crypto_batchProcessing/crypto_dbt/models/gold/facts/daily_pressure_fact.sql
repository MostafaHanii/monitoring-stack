/*
Daily Pressure Fact

This fact table aggregates buy/sell pressure metrics to daily level.
- Focuses on taker buy volumes as a measure of aggressive buying.
- Includes ratios and simple flags for dominance.
- Answers basic questions on buying vs. selling sentiment.

Measures:
- total_taker_buy_base: Sum of base asset bought by takers (aggressive buys).
- total_taker_buy_quote: Sum of quote asset (USDT) spent by takers.
- total_quote_volume: Total daily turnover in USDT (for context).
- taker_buy_ratio: % of volume from taker buys (buying pressure).
- pressure_regime: Simple label for buyer/seller dominance.


fct_pressure_daily
Daily buy/sell pressure measures (gold layer)
Source: fct_ohlcv_daily (canonical daily OHLCV)
Purpose: measure taker (market) buy activity, compute simple buy/sell ratios and flags

*/

{{ config(
    materialized='table',
    unique_key = 'daily_pressure_sk',
    schema = 'gold',
    cluster_by = ['open_date', 'base_asset_fk']
) }}


with daily_agg as (
    select
        base_asset_fk,
        quote_asset_fk,
        ASSET_PAIR_SYMBOL,
        BASE_ASSET_SYMBOL,
        QUOTE_ASSET_SYMBOL,
        open_date_fk,
        open_date,
        taker_buy_base_volume ,  -- Total base coin (e.g., BTC) bought aggressively
        taker_buy_quote_volume ,  -- Total USDT spent on aggressive buys
        quote_asset_volume , -- Total daily turnover in USDT
        base_asset_volume,
        num_trades
    from {{ ref('fct_ohlcv_daily') }}
),
pressure_metrics as (
    select
        *,
        -- Instant buy ratios (percentage of volume executed as aggressive buys)
        {{ safe_divide('taker_buy_quote_volume', 'quote_asset_volume') }}
            as instant_buy_ratio_quote,

        {{ safe_divide('taker_buy_base_volume', 'base_asset_volume') }}
            as instant_buy_ratio_base,

        -- Passive volume (not aggressive buys) — NOT "sells"
        (quote_asset_volume - taker_buy_quote_volume)
            as passive_quote_volume,

        (base_asset_volume - taker_buy_base_volume)
            as passive_base_volume,

        -- Passive ratios
        {{ safe_divide('(quote_asset_volume - taker_buy_quote_volume)', 'quote_asset_volume') }}
            as passive_ratio_quote,

        {{ safe_divide('(base_asset_volume - taker_buy_base_volume)', 'base_asset_volume') }}
            as passive_ratio_base,

        -- Aggression index (0 = perfectly balanced, 1 = extreme aggression)
        abs(instant_buy_ratio_base - 0.5) * 2 as aggression_index
    from daily_agg
),
regimes as (
    select
        *,
        case
            when instant_buy_ratio_base is null then null
            when instant_buy_ratio_base >= 0.55 then 'Buyers Dominant'
            when instant_buy_ratio_base <= 0.40 then 'Passive/Neutral Trades'
            else 'Balanced'
        end as pressure_regime
    from pressure_metrics
)
select
    {{ dbt_utils.generate_surrogate_key(['asset_pair_symbol','open_date']) }} as daily_pressure_sk,
    asset_pair_symbol,
    base_asset_fk,
    quote_asset_fk,
    base_asset_symbol,
    quote_asset_symbol,
    open_date,
    open_date_fk,

    taker_buy_base_volume,
    taker_buy_quote_volume,
    base_asset_volume,
    quote_asset_volume,
    num_trades,

    instant_buy_ratio_quote,
    instant_buy_ratio_base,
    passive_quote_volume,
    passive_base_volume,
    passive_ratio_quote,
    passive_ratio_base,
    aggression_index,
    pressure_regime

from regimes