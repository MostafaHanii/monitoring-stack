SELECT
    -- Parse JSON fields and map to target schema
    record_content:data:k:s::string as ASSET_SYMBOL,
    to_timestamp_ntz(record_content:data:k:t::int, 3) as OPEN_TIME,
    record_content:data:k:o::float as OPEN,
    record_content:data:k:h::float as HIGH,
    record_content:data:k:l::float as LOW,
    record_content:data:k:c::float as CLOSE,
    record_content:data:k:v::float as VOLUME,
    to_timestamp_ntz(record_content:data:k:T::int, 3) as CLOSE_TIME,
    record_content:data:k:q::float as QUOTE_ASSET_VOLUME,
    record_content:data:k:n::int as NUM_TRADES,
    record_content:data:k:V::float as TAKER_BUY_BASE_VOLUME,
    record_content:data:k:Q::float as TAKER_BUY_QUOTE_VOLUME,
    record_content:data:k:B::string as IGNORE,
    -- Metadata (for incremental logic)
    record_metadata:offset::int as kafka_offset
FROM CRYPTO_DB.PUBLIC.binance_kline
WHERE record_content:data:k:x::boolean = true -- Only closed candles