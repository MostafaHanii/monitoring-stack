import pandas as pd
import requests
from datetime import datetime, timedelta
import time
import os
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import snowflake.connector
from dotenv import load_dotenv



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



def get_assets_from_snowflake():
    """Fetch asset symbols from Snowflake bronze layer"""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT pair_symbol
        FROM BRONZE.assets
        WHERE pair_symbol != 'USDT'
        order by date(timestamp) desc, cmc_rank DESC
        limit 10
    """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    
    symbols = [row[0] for row in results]
    return symbols


def get_ohlcv(symbol, start_time, end_time, interval='1h'):
    """
    Fetch OHLCV data from Binance for a given symbol and time range.
    """
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {symbol}: {response.text}")
            return pd.DataFrame()
        
        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "num_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ])
        
        # Convert numeric columns
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        
        # Convert timestamps to datetime
        df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
        df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')
        
        return df
    
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"Connection error for {symbol}: {e}")
        return pd.DataFrame()


def extract_ohlcv_last_year(symbol="BTCUSDT"):
    """
    Extract OHLCV data for the last year (hourly interval)
    """
    interval = '1h'
    
    # Calculate dates: last year to today
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    current = start_date
    all_data = []

    print(f"Extracting OHLCV for {symbol} from {start_date} to {end_date}")
    
    while current < end_date:
        next_time = current + timedelta(hours=1000)
        
        # Ensure we don't go past end_date
        if next_time > end_date:
            next_time = end_date
        
        start_ms = int(current.timestamp() * 1000)
        end_ms = int(next_time.timestamp() * 1000)

        df = get_ohlcv(symbol, start_ms, end_ms, interval)
        
        if df.empty:
            print(f"No data for {symbol} in range {current} to {next_time}")
            break
        
        all_data.append(df)
        current = df["close_time"].iloc[-1]
        time.sleep(0.2)  # Rate limiting

    if not all_data:
        print(f"No data collected for {symbol}")
        return None
    
    # Combine all chunks
    full_df = pd.concat(all_data, ignore_index=True)
    print(f"Total records for {symbol}: {len(full_df)}")
    
    return full_df

def normalize_ohlcv(df, symbol):
    df = df.copy()
    df["asset_symbol"] = symbol

    df["open_time"] = pd.to_datetime(df["open_time"], utc=True)\
                          .dt.strftime("%Y-%m-%d %H:%M:%S")

    df["close_time"] = pd.to_datetime(df["close_time"], utc=True)\
                           .dt.strftime("%Y-%m-%d %H:%M:%S")
    df = df[
        [
            "asset_symbol",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "num_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore"
        ]
    ]

    df = df.where(pd.notnull(df), None)
    return df

def insert_ohlcv(df):
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO BRONZE.OHLCV (
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
        VALUES (
            %(asset_symbol)s,
            %(open_time)s,
            %(open)s,
            %(high)s,
            %(low)s,
            %(close)s,
            %(volume)s,
            %(close_time)s,
            %(quote_asset_volume)s,
            %(num_trades)s,
            %(taker_buy_base_volume)s,
            %(taker_buy_quote_volume)s,
            %(ignore)s
        )
    """

    cursor.executemany(insert_sql, df.to_dict(orient="records"))
    conn.commit()

    cursor.close()
    conn.close()




def main():
    """Main execution function"""
    print("Starting OHLCV extraction...")
    
    # Get symbols from Snowflake
    symbols = get_assets_from_snowflake()
    print(f"Found {len(symbols)} assets to process: {symbols}")
    
    # Convert to trading pairs
    trade_pairs = [f"{coin}USDT" for coin in symbols]
    
    # Extract and save data for each symbol
    for symbol in trade_pairs:
        try:
            df = extract_ohlcv_last_year(symbol)

            if df is not None and not df.empty:
                df = normalize_ohlcv(df, symbol)
                insert_ohlcv(df)

            
            print(f"✓ Completed {symbol}\n")
            
        except Exception as e:
            print(f"✗ Error processing {symbol}: {e}\n")
            continue


if __name__ == "__main__":
    main()