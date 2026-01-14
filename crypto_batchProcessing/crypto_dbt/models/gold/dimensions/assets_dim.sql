{{ config(
    materialized='table',
    schema='gold'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['ASSET_ID', 'dbt_valid_from']) }} as asset_sk,
    asset_id,
    asset_name,
    symbol,
    num_market_pairs,
    asset_launch_date,
    max_supply,
    circulating_supply,
    supply_type,
    market_cap_based_rank,
    cmc_last_updated,
    ingestion_timestamp,
    dbt_valid_from as rank_valid_from,
    dbt_valid_to as rank_valid_to

from {{ ref('stg_assets') }}
