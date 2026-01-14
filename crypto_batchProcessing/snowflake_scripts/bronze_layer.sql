-- 1. Create or Replace a Warehouse (crypto_wh)
-- This warehouse is set to XSMALL, auto-suspends after 60 seconds of inactivity,
-- and starts in a suspended state.
CREATE OR REPLACE WAREHOUSE crypto_wh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE
COMMENT = 'For loading raw data';

-- 2. Create or Replace a Database (crypto_db)

-- Create databases for each environment
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;        -- Raw data storage

-- 3. Create or Replace a Schema (raw)
USE DATABASE CRYPTO_DB;
CREATE OR REPLACE SCHEMA BRONZE;

----------------------------------------

-- Create Bronze Layer Tables

-- create OHLCV table
CREATE OR REPLACE TABLE BRONZE.ohlcv (
    asset_symbol           STRING,       
    open_time              TIMESTAMP_NTZ,
    open                   FLOAT,
    high                   FLOAT,
    low                    FLOAT,
    close                  FLOAT,
    volume                 FLOAT,
    close_time             TIMESTAMP_NTZ,
    quote_asset_volume     FLOAT,
    num_trades             NUMBER,
    taker_buy_base_volume  FLOAT,
    taker_buy_quote_volume FLOAT,
    ignore                 NUMBER
);



-- create agg_trade table

CREATE OR REPLACE TABLE BRONZE.agg_trades (
    agg_trade_id INT,
    pair_symbol STRING,
    price FLOAT,
    quantity FLOAT,
    first_trade_id INT,
    last_trade_id INT,
    transact_time BIGINT
);

-- DROP TABLE BRONZE.agg_trades;


-- create asset table
CREATE OR REPLACE TABLE BRONZE.assets (
    id                              NUMBER,
    name                            STRING,
    pair_symbol                     STRING,
    slug                            STRING,
    num_market_pairs                NUMBER,
    date_added                      TIMESTAMP_TZ,
    max_supply                      FLOAT,
    circulating_supply              FLOAT,
    total_supply                    FLOAT,
    infinite_supply                 BOOLEAN,
    cmc_rank                        NUMBER,
    last_updated                    TIMESTAMP_TZ,
    price_usdt                      FLOAT,
    volume_24h_usdt                 FLOAT,
    volume_change_24h_usdt          FLOAT,
    pct_change_1h_usdt              FLOAT,
    pct_change_24h_usdt             FLOAT,
    pct_change_7d_usdt              FLOAT,
    pct_change_30d_usdt             FLOAT,
    pct_change_60d_usdt             FLOAT,
    pct_change_90d_usdt             FLOAT,
    market_cap_usdt                 FLOAT,
    market_cap_dominance_usdt       FLOAT,
    fully_diluted_market_cap_usdt   FLOAT,
    last_updated_usdt               TIMESTAMP_TZ(0),
    timestamp                       TIMESTAMP_NTZ(0)
);

DROP TABLE CRYPTO_DB.BRONZE.ASSETS
---------------------------------------
-- create stages

-- stage for aggregate trades
CREATE OR REPLACE STAGE agg_trades_stage
FILE_FORMAT = 
(
    TYPE = 'CSV' 
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY='"' 
    ESCAPE_UNENCLOSED_FIELD = '\\'
);

-- stage for OHLCV
CREATE OR REPLACE STAGE ohlcv_stage
FILE_FORMAT = 
(
    TYPE = 'CSV' 
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY='"' 
    ESCAPE_UNENCLOSED_FIELD = '\\'
);

-- stage for Assets
CREATE OR REPLACE STAGE assets_stage
FILE_FORMAT =
(
    TYPE = 'CSV' 
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY='"' 
    ESCAPE_UNENCLOSED_FIELD = '\\'
);


LIST @assets_stage

LIST @ohlcv_stage;

----------------------------------------------
-- copy data into tables

-- Create a temporary table
CREATE OR REPLACE TEMP TABLE temp_preview (
    id                              NUMBER,
    name                            STRING,
    symbol                     STRING,
    slug                            STRING,
    num_market_pairs                NUMBER,
    date_added                      TIMESTAMP_TZ,
    max_supply                      FLOAT,
    circulating_supply              FLOAT,
    total_supply                    FLOAT,
    infinite_supply                 BOOLEAN,
    cmc_rank                        NUMBER,
    last_updated                    TIMESTAMP_TZ,
    price_usdt                      FLOAT,
    volume_24h_usdt                 FLOAT,
    volume_change_24h_usdt          FLOAT,
    pct_change_1h_usdt              FLOAT,
    pct_change_24h_usdt             FLOAT,
    pct_change_7d_usdt              FLOAT,
    pct_change_30d_usdt             FLOAT,
    pct_change_60d_usdt             FLOAT,
    pct_change_90d_usdt             FLOAT,
    market_cap_usdt                 FLOAT,
    market_cap_dominance_usdt       FLOAT,
    fully_diluted_market_cap_usdt   FLOAT,
    last_updated_usdt               TIMESTAMP_TZ,
    timestamp                       TIMESTAMP_NTZ
);

-- DROP TABLE temp_preview

COPY INTO BRONZE.assets
FROM (
    SELECT $1 AS id,
           $2 AS name,
           $3 AS symbol,
           $4 AS slug,
           $5 AS num_market_pairs,
           $6 AS date_added,
           $8 AS max_supply,
           $9 AS circulating_supply,
           $10 AS total_supply,
           $11 AS infinite_supply,
           $13 AS cmc_rank,
           $17 AS last_updated,
           $18 AS price_usdt,
           $19 AS volume_24h_usdt,
           $20 AS volume_change_24h_usdt,
           $21 AS pct_change_1h_usdt,
           $22 AS pct_change_24h_usdt,
           $23 AS pct_change_7d_usdt,
           $24 AS pct_change_30d_usdt,
           $25 AS pct_change_60d_usdt,
           $26 AS pct_change_90d_usdt,
           $27 AS market_cap_usdt,
           $28 AS market_cap_dominance_usdt,
           $29 AS fully_diluted_market_cap_usdt,
           $31 AS last_updated_usdt,
           $37 AS timestamp
    FROM @CRYPTO_DB.BRONZE.ASSETS_STAGE/CoinMarketCapTop11Crypto_Updated.csv
)
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' ESCAPE_UNENCLOSED_FIELD = '\\' SKIP_HEADER=1)




-- Preview
SELECT * FROM BRONZE.assets;


-- test for OHLCV
CREATE OR REPLACE TEMP TABLE temp_preview_ohlcv (
    asset_symbol           STRING,       
    open_time              TIMESTAMP_NTZ,
    open                   FLOAT,
    high                   FLOAT,
    low                    FLOAT,
    close                  FLOAT,
    volume                 FLOAT,
    close_time             TIMESTAMP_NTZ,
    quote_asset_volume     FLOAT,
    num_trades             NUMBER,
    taker_buy_base_volume  FLOAT,
    taker_buy_quote_volume FLOAT,
    ignore                 NUMBER
);

DROP TABLE temp_preview_ohlcv

COPY INTO BRONZE.ohlcv
(
    asset_symbol,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time,
    quote_asset_volume,
    num_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    ignore
)
FROM (
    SELECT
        REPLACE(METADATA$FILENAME, '.csv', '') AS asset_symbol,
        $1 AS open_time,
        $2  AS open,
        $3  AS high,
        $4  AS low,
        $5  AS close,
        $6  AS voCRYPTO_DB.PUBLIC.SNOWFLAKE_KAFKA_CONNECTOR_SNOWFLAKE_SINK_1996731346_STAGE_BINANCE_KLINElume,
        $7 AS close_time,
        $8  AS quote_asset_volume,
        $9  AS num_trades,
        $10 AS taker_buy_base_volume,
        $11 AS taker_buy_quote_volume,
        $12 AS ignore
    FROM @ohlcv_stage
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1);
CRYPTO_DB.SILVER_SILVER
select * from BRONZE.ohlcv

Drop table BRONZE.assets



-- trades.sql
CRYPTO_DB.BRONZE.STG_ASSETSLIST @agg_trades_stage PATTERN = '.*XRPUSDT-aggTrades.*';

SELECT COUNT(*) as total_rows
FROM @agg_trades_stage
-- WHERE METADATA$FILENAME RLIKE 'XRPUSDT-aggTrades.*\\.csv';


COPY INTO BRONZE.agg_trades
FROM (
    SELECT
        $1 AS agg_trade_id,
        SPLIT_PART(REPLACE(METADATA$FILENAME, '.csv', ''), '-', 1) AS pair_symbol,
        $2 AS price,
        $3 AS quantity,
        $4 AS first_trade_id,
        $5 AS last_trade_id,
        $6 AS transact_time
    FROM @agg_trades_stage
)
PATTERN = '.*XRPUSDT-aggTrades.*\\.csv'
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)


SELECT COUNT(*)
FROM BRONZE.agg_trades


SELECT * FROM PUBLIC.streamed_ohlcv

-- gold trial

SELECT 
    ti.hour_of_day,
    d.dayname,
    CASE 
        WHEN d.dayname IN ('Saturday', 'Sunday') THEN 1 
        ELSE 0 
    END as is_weekend, 
    t.pair_symbol,
    SUM(t.last_trade_id - t.first_trade_id + 1) as trade_count,
    SUM(t.quantity) as hourly_volume,
    AVG(t.price) as avg_hourly_price,
    STDDEV(t.price) as price_volatility
FROM trades_fct t
LEFT JOIN time_dim ti ON t.time_fk = ti.time_sk
LEFT JOIN date_dim d ON t.date_fk = d.date_sk
GROUP BY ti.hour_of_day, d.dayname, is_weekend, t.pair_symbol
ORDER BY t.pair_symbol, is_weekend, d.dayname, ti.hour_of_day

-- 

SELECT 
    t.hour_of_day,
    t.time_division ,
    AVG(tr.quantity) as avg_quantity,
    AVG(tr.price * tr.quantity) as avg_trade_value,
    SUM(tr.last_trade_id - tr.first_trade_id + 1) as trade_frequency
FROM trades_fct tr
JOIN time_dim t ON tr.time_fk = t.time_sk
GROUP BY t.hour_of_day, t.time_division
ORDER BY t.hour_of_day
-- 


WITH asset_pair_volume AS (
    SELECT
        base.pair_symbol,
        SUM(base.quantity * base.price) AS total_value_traded
    FROM trades_fct AS base
    GROUP BY base.pair_symbol
)
SELECT * FROM asset_pair_volume
ORDER BY total_value_traded DESC;

-- 
-- dbt model: mart_trade_size_distribution.sql
WITH trade_size AS(
    SELECT 
        pair_symbol,
        price * quantity as trade_value,
        CASE 
            WHEN price * quantity < 100 THEN 'Micro (<$100)'
            WHEN price * quantity < 1000 THEN 'Small ($100-1K)'
            WHEN price * quantity < 10000 THEN 'Medium ($1K-10K)'
            WHEN price * quantity < 100000 THEN 'Large ($10K-100K)'
            ELSE 'Whale (>$100K)'
        END as trade_size_category,
        quantity,
        price,
        transaction_date
    FROM trades_fct
)
SELECT *
FROM trade_size


-- 
SELECT 
    DATE(TRANSACTION_DATE) as Trade_Date,
    HOUR(TRANSACTION_TIME) as Trade_Hour,
    SUM(last_trade_id - first_trade_id + 1) as Trades_Per_Hour,
    SUM(QUANTITY) as Volume_Per_Hour,
    SUM(PRICE * QUANTITY) as Value_Per_Hour,
    SUM(last_trade_id - first_trade_id + 1) / 60.0 as Trades_Per_Minute
FROM trades_fct
-- NO DATE FILTER - Use all your 2022-2024 data
GROUP BY DATE(TRANSACTION_DATE), HOUR(TRANSACTION_TIME)
ORDER BY Trade_Date, Trade_Hour;
-- 
WITH WhaleTrades AS (
    SELECT 
        AGG_TRADE_ID,
        PAIR_SYMBOL,
        BASE_ASSET_FK,
        QUOTE_ASSET_FK,
        PRICE,
        QUANTITY,
        TRANSACTION_DATE,
        TRANSACTION_TIME,
        DATEADD(DAY, -1, TRANSACTION_DATE) AS Previous_Date,
        DATEADD(DAY, 1, TRANSACTION_DATE) AS Next_Date
    FROM trades_fct
    WHERE QUANTITY > 100  -- حدد الحد لحجم الكمية الكبيرة، يمكن تعديله حسب الحاجة
)

SELECT 
    wt.AGG_TRADE_ID,
    wt.PAIR_SYMBOL,
    wt.PRICE AS Current_Price,
    wt.QUANTITY AS Trade_Quantity,
    wt.TRANSACTION_DATE,
    wt.TRANSACTION_TIME,
    prev.PRICE AS Previous_Day_Price,
    next.PRICE AS Next_Day_Price
FROM WhaleTrades wt
LEFT JOIN trades_fct prev
    ON prev.PAIR_SYMBOL = wt.PAIR_SYMBOL
    AND prev.TRANSACTION_DATE = wt.Previous_Date
LEFT JOIN trades_fct next
    ON next.PAIR_SYMBOL = wt.PAIR_SYMBOL
    AND next.TRANSACTION_DATE = wt.Next_Date
ORDER BY wt.TRANSACTION_DATE DESC, wt.TRANSACTION_TIME DESC;


-- 
SELECT *
FROM CRYPTO_DB.BRONZE_GOLD.DAILY_TRADING_VOLUME_BY_ASSET
LIMIT 5

-- 








































