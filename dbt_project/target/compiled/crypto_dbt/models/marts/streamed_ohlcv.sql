

SELECT *
FROM CRYPTO_DB.PUBLIC.stg_binance_kline



qualify row_number() over (partition by ASSET_SYMBOL, OPEN_TIME order by kafka_offset desc) = 1