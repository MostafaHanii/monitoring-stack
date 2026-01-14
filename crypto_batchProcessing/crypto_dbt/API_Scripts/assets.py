import json
import pandas as pd
import numpy as np
import requests
import os
from dotenv import load_dotenv
import snowflake.connector
from datetime import datetime, timezone
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects


# -------------------------------------------------
# CONFIG
# -------------------------------------------------
CMC_API_KEY = "edc9ea32d97240519a256afeb022c3a2"

BASE_DIR = os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)

load_dotenv(os.path.join(BASE_DIR, ".env"))

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}


# -------------------------------------------------
# Snowflake connection
# -------------------------------------------------
def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


# -------------------------------------------------
# CoinMarketCap API
# -------------------------------------------------
def fetch_top_coins(limit=30):
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
    params = {
        "start": 1,
        "limit": limit,
        "convert": "USDT"
    }
    headers = {
        "Accepts": "application/json",
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=params)
        response.raise_for_status()
        data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects, requests.HTTPError) as e:
        raise RuntimeError(f"CoinMarketCap API error: {e}")

    return pd.json_normalize(data["data"])


# -------------------------------------------------
# Binance symbols
# -------------------------------------------------
def get_binance_symbols():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    response = requests.get(url).json()

    df = pd.DataFrame(response["symbols"])
    df = df[
        (df["quoteAsset"] == "USDT") &
        (df["status"] == "TRADING")
    ]

    return set(df["baseAsset"])


# -------------------------------------------------
# Normalize to BRONZE.assets schema
# -------------------------------------------------
def normalize_assets(df):
    df = df.copy()

    # -------------------------------
    # Binance + Stable Coin Filtering
    # -------------------------------
    stable_coins = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USTC"}

    binance_symbols = get_binance_symbols()

    df = df[
        (df["symbol"].isin(binance_symbols)) &
        (~df["symbol"].isin(stable_coins))
    ]

    # -------------------------------
    # Enforce TOP 10 by CMC rank
    # -------------------------------
    df = (
        df.sort_values("cmc_rank")
          .head(10)
    )
    

    df["pair_symbol"] = df["symbol"]

    df["price_usdt"] = df["quote.USDT.price"]
    df["volume_24h_usdt"] = df["quote.USDT.volume_24h"]
    df["volume_change_24h_usdt"] = df["quote.USDT.volume_change_24h"]
    df["pct_change_1h_usdt"] = df["quote.USDT.percent_change_1h"]
    df["pct_change_24h_usdt"] = df["quote.USDT.percent_change_24h"]
    df["pct_change_7d_usdt"] = df["quote.USDT.percent_change_7d"]
    df["pct_change_30d_usdt"] = df["quote.USDT.percent_change_30d"]
    df["pct_change_60d_usdt"] = df["quote.USDT.percent_change_60d"]
    df["pct_change_90d_usdt"] = df["quote.USDT.percent_change_90d"]

    df["market_cap_usdt"] = df["quote.USDT.market_cap"]
    df["market_cap_dominance_usdt"] = df["quote.USDT.market_cap_dominance"]
    df["fully_diluted_market_cap_usdt"] = df["quote.USDT.fully_diluted_market_cap"]


    # ---- FIX TIMESTAMPS ----
    # ---- FIX TIMESTAMPS (FINAL, SNOWFLAKE-SAFE) ----

    df["date_added"] = (
        pd.to_datetime(df["date_added"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df["last_updated"] = (
        pd.to_datetime(df["last_updated"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    df["last_updated_usdt"] = (
        pd.to_datetime(df["quote.USDT.last_updated"], utc=True)
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )

    # ingestion timestamp (NTZ)
    df["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    # ----------------------------------------------
    df = df.replace({np.nan: None})

    return df[
        [
            "id", "name", "pair_symbol", "slug", "num_market_pairs",
            "date_added", "max_supply", "circulating_supply",
            "total_supply", "infinite_supply", "cmc_rank",
            "last_updated", "price_usdt", "volume_24h_usdt",
            "volume_change_24h_usdt", "pct_change_1h_usdt",
            "pct_change_24h_usdt", "pct_change_7d_usdt",
            "pct_change_30d_usdt", "pct_change_60d_usdt",
            "pct_change_90d_usdt", "market_cap_usdt",
            "market_cap_dominance_usdt",
            "fully_diluted_market_cap_usdt",
            "last_updated_usdt", "timestamp"
        ]
    ]


# -------------------------------------------------
# Insert into Snowflake
# -------------------------------------------------
def insert_assets(df):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO assets (
            id, name, pair_symbol, slug, num_market_pairs, date_added,
            max_supply, circulating_supply, total_supply, infinite_supply,
            cmc_rank, last_updated, price_usdt, volume_24h_usdt,
            volume_change_24h_usdt, pct_change_1h_usdt,
            pct_change_24h_usdt, pct_change_7d_usdt,
            pct_change_30d_usdt, pct_change_60d_usdt,
            pct_change_90d_usdt, market_cap_usdt,
            market_cap_dominance_usdt,
            fully_diluted_market_cap_usdt,
            last_updated_usdt, timestamp
        )
        VALUES (
            %(id)s, %(name)s, %(pair_symbol)s, %(slug)s, %(num_market_pairs)s, %(date_added)s,
            %(max_supply)s, %(circulating_supply)s, %(total_supply)s, %(infinite_supply)s,
            %(cmc_rank)s, %(last_updated)s, %(price_usdt)s, %(volume_24h_usdt)s,
            %(volume_change_24h_usdt)s, %(pct_change_1h_usdt)s,
            %(pct_change_24h_usdt)s, %(pct_change_7d_usdt)s,
            %(pct_change_30d_usdt)s, %(pct_change_60d_usdt)s,
            %(pct_change_90d_usdt)s, %(market_cap_usdt)s,
            %(market_cap_dominance_usdt)s,
            %(fully_diluted_market_cap_usdt)s,
            %(last_updated_usdt)s, %(timestamp)s
        )
    """

    cursor.executemany(insert_sql, df.to_dict(orient="records"))
    conn.commit()

    cursor.close()
    conn.close()


# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":
    df = fetch_top_coins(limit=30)
    df = normalize_assets(df)
    #print(df)
    insert_assets(df)

    print(f"Inserted {len(df)} rows into BRONZE.assets")
