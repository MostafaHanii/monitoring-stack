{{
    config(
        materialized='incremental',
        unique_key=['ASSET_SYMBOL', 'OPEN_TIME']
    )
}}

SELECT *
FROM {{ ref('stg_binance_kline') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  WHERE kafka_offset > (select max(kafka_offset) from {{ this }})
{% endif %}

qualify row_number() over (partition by ASSET_SYMBOL, OPEN_TIME order by kafka_offset desc) = 1
