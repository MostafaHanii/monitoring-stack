import json
import time
import traceback
from threading import Thread
from prometheus_client import start_http_server, Gauge
from websocket import create_connection, WebSocketConnectionClosedException

# --- Configuration ---
# Binance WebSocket for the BTCUSDT Ticker stream
URL = "wss://stream.binance.com:9443/stream?streams=solusdt@kline_1m/btcusdt@kline_1m/bnbusdt@kline_1m/xrpusdt@kline_1m/ethusdt@kline_1m"

LISTEN_PORT = 9001  # Port for the Prometheus Exporter to listen on

# --- Prometheus Metrics ---
# Gauge to track the last received trade price for BTC/USDT


# Gauge to track the 24h price change percentage
gauge_close = Gauge(
    'close_price_1m_stats',
    'The 1m close price stats',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
gauge_open = Gauge(
    'open_price_1m_stats',
    'The 1m open price stats',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
gauge_high = Gauge(
    'high_price_1m_stats',
    'The 1m high price stats',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
gauge_low = Gauge(
    'low_price_1m_stats',
    'The 1m low price stats',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
gauge_volume = Gauge(
    'volume_price_1m_stats',
    'The 1m volume price stats',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
gauge_ts = Gauge(
    'timestamp',
    'ts for debug purposes',
    ['symbol', 'interval']  # <--- Combined label names as the single third argument
)
# --- WebSocket Client Logic ---
def on_message(ws, raw_message):
    try:
        message = json.loads(raw_message)
        
        # 1. Identify the stream and extract the kline data
        stream_name = message.get("stream")
        kline_data = message.get("data", {}).get("k", {})
        
        # If 'k' data is not present (e.g., connection confirmations), skip
        if not kline_data:
            return

        # Extract the symbol (e.g., "BTCUSDT")
        symbol = kline_data.get("s")
        
        # 2. Check for Candle Closure
        is_closed = kline_data.get("x")
        
        if is_closed:
            # 3. Extract FINAL OHLC values
            final_ohlc = {
                "symbol": symbol,
                "interval": kline_data.get("i"), # Should be "1m"
                "timestamp_ms": kline_data.get("T"),
                "open": float(kline_data.get("o")),
                "high": float(kline_data.get("h")),
                "low": float(kline_data.get("l")),
                "close": float(kline_data.get("c")),
                "volume": float(kline_data.get("v"))
            }

            # 4. SEND TO PROMETHEUS EXPORTER LOGIC HERE
            print(f"CLOSED CANDLE: {final_ohlc}")
            # --- Call your Prometheus Gauge update functions here ---
            # e.g., update_prometheus_metric("binance_kline_close_price", final_ohlc)
            gauge_close.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['close'])
            gauge_open.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['open'])
            gauge_high.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['high'])
            gauge_low.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['low'])
            gauge_volume.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['volume'])
            gauge_ts.labels(symbol=final_ohlc['symbol'],interval=final_ohlc['interval']).set(final_ohlc['timestamp_ms'])
            
    except Exception as e:
        print(f"Error processing message: {e}")
def websocket_client():
    """Continuously connects to the Binance WebSocket and updates Prometheus metrics."""
    print("Starting Binance WebSocket client thread...")
    while True:
        ws = None
        try:
            ws = create_connection(URL, timeout=10)
            print(f"Successfully connected to Binance WebSocket at {URL}")

            while True:
                # Receive message from the socket
                result = ws.recv()
                
                # Check for empty result before attempting to load JSON
                if not result:
                    print("Received empty result. Reconnecting...")
                    break
                    
                on_message(ws,result)

        except WebSocketConnectionClosedException as e:
            print(f"WebSocket closed gracefully: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            # Print the full traceback to help diagnose parsing or network errors
            #print(data.keys)
            print(f"An unexpected error occurred in the WS loop: {e}")
            traceback.print_exc()
            print("Reconnecting in 10 seconds...")
            time.sleep(10)
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

# --- Main Logic ---
if __name__ == '__main__':
    # 1. Start the Prometheus HTTP server to expose metrics
    start_http_server(LISTEN_PORT)
    print(f"Prometheus exporter listening on port {LISTEN_PORT}...")

    # 2. Start the WebSocket connection in a separate thread
    ws_thread = Thread(target=websocket_client)
    ws_thread.daemon = True # Allows the program to exit even if this thread is running
    ws_thread.start()

    # Keep the main thread alive to serve the metrics
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Exporter stopped by user.")
