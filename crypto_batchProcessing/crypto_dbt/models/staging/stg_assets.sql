{{ config(
    schema='silver'
) }}
WITH raw_assets
AS
(
    SELECT *
    FROM {{ source('raw_data', 'scd_raw_assets') }}
)

SELECT id AS asset_id,
    name AS asset_name,
    pair_symbol AS symbol,
    num_market_pairs,
    TO_DATE(date_added) AS asset_launch_date,
    max_supply,
    circulating_supply,
    CASE
        WHEN max_supply IS NULL THEN 'Unlimited'
        WHEN max_supply IS NOT NULL THEN 'Fixed'
        ELSE 'Unknown'
    END AS supply_type,
    cmc_rank as market_cap_based_rank,
    last_updated AS cmc_last_updated,
    timestamp AS ingestion_timestamp,
    dbt_valid_from,
    dbt_valid_to
FROM raw_assets