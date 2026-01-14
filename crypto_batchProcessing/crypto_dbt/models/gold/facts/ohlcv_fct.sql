{{ config(
    materialized='incremental',
    schema='gold'
) }}
-- 1) load data from stage
with src_raw as (

    select *
    from {{ ref('stg_ohlcv') }}

    {% if is_incremental() %}
        where (open_date, open_time, base_asset_symbol) not in (
            select open_date, open_time, base_asset_symbol
            from {{ this }}
        ) 
    {% endif %}
),
-- 2) Resolve asset SK
base_asset_resolved as (

    select
        s.*,
        d.asset_sk as base_asset_fk
    from src_raw s
    left join {{ ref('assets_dim') }} d
      on upper(s.base_asset_symbol) = upper(d.symbol)
      and  d.rank_valid_to IS NULL

),

quote_asset_resolved as (

    select
        s.*,
        d.asset_sk as quote_asset_fk
    from base_asset_resolved s
    left join {{ ref('assets_dim') }} d
      on upper(s.quote_asset_symbol) = upper(d.symbol)
      and  d.rank_valid_to IS NULL

),

-- 3) Resolve date SK
date_open_resolved as (
    select
        ar.*,
        dd.date_sk as open_date_fk
    from quote_asset_resolved ar
    left join {{ ref('date_dim') }} dd
        on open_date = dd.full_date
),
-- 4) Resolve time SK
time_resolved as (
    select
        dr.*,
        tt.time_sk as open_time_fk
    from date_open_resolved dr
    left join {{ ref('time_dim') }} tt
        on dr.open_time = tt.time_of_day
),

-- 4) Final Fact Payload
final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'base_asset_fk',
            'open_date',
            'open_time'
        ]) }} as ohlcv_sk,
        open_date_fk,
        open_time_fk,
        base_asset_fk,
        quote_asset_fk,

        asset_pair_symbol,
        base_asset_symbol,
        quote_asset_symbol,
        open_date,
        open_time,
        close_date,
        close_time,     
        open, 
        high,
        low,
        close,
        base_asset_volume,
        quote_asset_volume,
        num_trades,
        taker_buy_base_volume,
        taker_buy_quote_volume 

    from time_resolved
    where base_asset_fk is not null 
        AND open_date < '2025-01-01'
)

select * from final



