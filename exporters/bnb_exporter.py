import json
import time
import traceback
from threading import Thread
from prometheus_client import start_http_server, Gauge
from websocket import create_connection, WebSocketConnectionClosedException

# --- Configuration ---
# Binance WebSocket for the BTCUSDT Ticker stream
COIN_LABEL='BNB'
URL = "wss://stream.binance.com:9443/ws/bnbusdt@miniTicker"

LISTEN_PORT = 9892  # Port for the Prometheus Exporter to listen on

# --- Prometheus Metrics ---
# Gauge to track the last received trade price for BTC/USDT

# Gauge to track the 24h price change percentage
gauge = Gauge(
    f'{COIN_LABEL}coin_24h_stats',
    'The 24-hour price change percentage for chosen coin',
    ['coin', 'metric']  # <--- Combined label names as the single third argument
)

# --- WebSocket Client Logic ---
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
                    
                data = json.loads(result)
                
                # Log the raw data for debugging purposes
                # print(f"Received raw data: {data}")

                # Check for miniTicker data (event type 'e' is '24hrMiniTicker')
                if 'e' in data and data['e'] == '24hrMiniTicker':
                    close_price = float(data['c'])
                    open_price = float(data['o'])
                    high_price=float(data['h'])
                    low_price=float(data['l'])
                    base_volume=float(data['v'])
                    quote_volume=float(data['q'])
                    volatility=high_price-low_price
                    abs_change=close_price-open_price


                    # Set the Gauge values
                    gauge.labels(coin=data['s'],metric='close_price').set(close_price)
                    gauge.labels(coin=data['s'],metric='open_price').set(open_price)
                    gauge.labels(coin=data['s'],metric='high_price').set(high_price)
                    gauge.labels(coin=data['s'],metric='low_price').set(low_price)
                    gauge.labels(coin=data['s'],metric='base_volume').set(base_volume)
                    gauge.labels(coin=data['s'],metric='quote_volume').set(quote_volume)
                    gauge.labels(coin=data['s'],metric='volatility').set(volatility)
                    gauge.labels(coin=data['s'],metric='abs_change').set(abs_change)

                    
                    
                
                elif 'e' in data and data['e'] == 'ping':
                    # Ignore periodic ping messages
                    pass
                
                else:
                    # Log other messages (like connection confirmation)
                    print(f"Received non-ticker message: {data}")

        except WebSocketConnectionClosedException as e:
            print(f"WebSocket closed gracefully: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            # Print the full traceback to help diagnose parsing or network errors
            print(data.keys)
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
