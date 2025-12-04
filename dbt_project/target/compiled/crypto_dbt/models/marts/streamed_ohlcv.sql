

SELECT *
FROM CRYPTO_DB.PUBLIC.stg_binance_kline


  -- this filter will only be applied on an incremental run
  WHERE kafka_offset > (select max(kafka_offset) from CRYPTO_DB.PUBLIC.streamed_ohlcv)


qualify row_number() over (partition by ASSET_SYMBOL, OPEN_TIME order by kafka_offset desc) = 1