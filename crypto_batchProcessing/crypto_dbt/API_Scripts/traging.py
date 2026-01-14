import requests
import pandas as pd
import time
from datetime import datetime, timedelta, timezone
import os
import snowflake.connector
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from dotenv import load_dotenv

# ----------------------------------
# Config
# ----------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": "BRONZE"
}

BINANCE_URL = "https://api.binance.com/api/v3/aggTrades"

# -------------------------------------------------
# Snowflake helpers
# -------------------------------------------------
def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def get_symbols_from_snowflake(limit=10):
    conn = get_snowflake_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT pair_symbol
        FROM BRONZE.assets
        WHERE pair_symbol != 'USDT'
        ORDER BY DATE(timestamp) DESC, cmc_rank DESC
        LIMIT %s
        """,
        (limit,)
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [f"{row[0]}USDT" for row in rows]


def get_max_transact_time(symbol):
    conn = get_snowflake_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT MAX(TRANSACT_TIME)
        FROM BRONZE.AGG_TRADES
        WHERE pair_symbol = %s
        """,
        (symbol,)
    )

    result = cur.fetchone()[0]

    cur.close()
    conn.close()

    return result  # epoch ms or None


# -------------------------------------------------
# Binance API
# -------------------------------------------------
def fetch_agg_trades(symbol, start_ms, end_ms):
    params = {
        "symbol": symbol,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000
    }

    try:
        r = requests.get(BINANCE_URL, params=params, timeout=10)
        r.raise_for_status()
        return r.json()

    except (ConnectionError, Timeout, TooManyRedirects, requests.HTTPError) as e:
        print(f"⚠️ API error {symbol}: {e}")
        return []


# -------------------------------------------------
# Normalize
# -------------------------------------------------
def normalize_agg_trades(data, symbol):
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df = df.rename(columns={
        "a": "agg_trade_id",
        "p": "price",
        "q": "quantity",
        "f": "first_trade_id",
        "l": "last_trade_id",
        "T": "transact_time"
    })

    df["pair_symbol"] = symbol
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df["transact_time"] = df["transact_time"].astype("int64")

    return df[
        [
            "pair_symbol",
            "agg_trade_id",
            "price",
            "quantity",
            "first_trade_id",
            "last_trade_id",
            "transact_time"
        ]
    ]


# -------------------------------------------------
# Insert
# -------------------------------------------------
def insert_agg_trades(df):
    conn = get_snowflake_connection()
    cur = conn.cursor()

    cur.executemany(
        """
        INSERT INTO BRONZE.AGG_TRADES (
            pair_symbol,
            agg_trade_id,
            price,
            quantity,
            first_trade_id,
            last_trade_id,
            transact_time
        )
        VALUES (
            %(pair_symbol)s,
            %(agg_trade_id)s,
            %(price)s,
            %(quantity)s,
            %(first_trade_id)s,
            %(last_trade_id)s,
            %(transact_time)s
        )
        """,
        df.to_dict(orient="records")
    )

    conn.commit()
    cur.close()
    conn.close()


# -------------------------------------------------
# Extraction logic (CORRECT)
# -------------------------------------------------
def extract_agg_trades(symbol):
    print(f"▶ Processing {symbol}")

    max_ts = get_max_transact_time(symbol)

    if max_ts:
        start_ms = int(max_ts) + 1
        print(
            f"  ↳ Incremental from "
            f"{datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)}"
        )
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=365)
        start_ms = int(start_dt.timestamp() * 1000)
        print(f"  ↳ Initial backfill from {start_dt}")

    end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    while start_ms < end_ms:
        data = fetch_agg_trades(symbol, start_ms, end_ms)

        if not data:
            break  # no more data forward

        df = normalize_agg_trades(data, symbol)
        print(f"  ↳ Fetched {len(df)} records from API")
        print(f"    - Time range: "
              f"{datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)} "
              f"to {datetime.fromtimestamp(end_ms/1000, tz=timezone.utc)}"
              )
        print(df.head())
        if df.empty:
            break

        insert_agg_trades(df)
        print(f"  ✓ Inserted {len(df)} records for {symbol}")

        last_ts = df["transact_time"].max()

        # 🔒 safety: ensure forward-only progress
        if last_ts < start_ms:
            print("⚠️ No forward progress, stopping loop")
            break

        start_ms = last_ts + 1
        time.sleep(0.2)

    print(f"✓ Done {symbol}\n")


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    print("🚀 aggTrades extraction started")

    symbols = get_symbols_from_snowflake(limit=10)
    print(f"Symbols: {symbols}")

    for symbol in symbols:
        extract_agg_trades(symbol)


if __name__ == "__main__":
    main()
