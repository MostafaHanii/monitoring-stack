"""
Binance Kline to Kafka Producer
Consumes kline data from Binance WebSocket and produces to Kafka topic
"""
import json
import logging
import os
import time
import traceback
from threading import Thread
from confluent_kafka import Producer
from websocket import create_connection, WebSocketConnectionClosedException

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance_kline")
KAFKA_ACKS = os.environ.get("KAFKA_ACKS" , "all")
KAFKA_RETRIES = os.environ.get("KAFKA_RETRIES",3)
KAFKA_IDEMPOTENCE = os.environ.get("KAFKA_IDEMPOTENCE",True)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Binance WebSocket URL for 5 coins on 1m kline stream
BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=solusdt@kline_1m/btcusdt@kline_1m/bnbusdt@kline_1m/xrpusdt@kline_1m/ethusdt@kline_1m/adausdt@kline_1m/bchusdt@kline_1m/dogeusdt@kline_1m/linkusdt@kline_1m/trxusdt@kline_1m"

# --- Kafka Producer Setup ---
producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "ohlc-exporter-producer",
    "acks": KAFKA_ACKS , 
    "retries": KAFKA_RETRIES ,
    "enable.idempotence": KAFKA_IDEMPOTENCE
}

producer = Producer(producer_config)

# --- Kafka Delivery Callback ---
def delivery_report(err, msg):
    """Callback for Kafka message delivery reports"""
    if err:
        logger.error(f"Delivery failed: {err}")
    # Uncomment for verbose logging:
    # else:
    #     logger.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def produce_to_kafka(message_data, symbol):
    """
    Produce message to Kafka topic
    
    Args:
        message_data: JSON data from Binance WebSocket
        symbol: Cryptocurrency symbol (e.g., "BTCUSDT")
    """
    value = json.dumps(message_data).encode('utf-8')
    
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=value,
            callback=delivery_report
        )
        # Trigger delivery report callbacks
        producer.poll(0)
        
    except BufferError:
        logger.warning(f"Local producer queue is full ({len(producer)} messages awaiting delivery)")
    except Exception as e:
        logger.error(f"Error producing message: {e}")


def websocket_client():
    """
    Continuously connects to Binance WebSocket and produces messages to Kafka
    Only produces CLOSED candles (x=true)
    """
    logger.info("Starting Binance WebSocket client...")
    
    while True:
        ws = None
        try:
            ws = create_connection(BINANCE_WS_URL, timeout=10)
            logger.info(f"Connected to Binance WebSocket: {BINANCE_WS_URL}")
            
            while True:
                # Receive message from WebSocket
                result = ws.recv()
                
                if not result:
                    logger.warning("Received empty result. Reconnecting...")
                    break
                
                # Parse JSON message
                message = json.loads(result)
                
                # Extract kline data
                stream_name = message.get("stream")
                kline_data = message.get("data", {}).get("k", {})
                
                # Skip if no kline data
                if not kline_data:
                    continue
                
                # Extract symbol
                symbol = kline_data.get("s")
                
                # Check if candle is closed
                is_closed = kline_data.get("x")
                
                if is_closed:
                    # Extract OHLC data for logging
                    candle_data = {
                        "symbol": symbol,
                        "interval": kline_data.get("i"),
                        "open_time": kline_data.get("t"),
                        "close_time": kline_data.get("T"),
                        "open": float(kline_data.get("o")),
                        "high": float(kline_data.get("h")),
                        "low": float(kline_data.get("l")),
                        "close": float(kline_data.get("c")),
                        "volume": float(kline_data.get("v"))
                    }
                    
                    logger.info(f"CLOSED CANDLE: {symbol} - Close: {candle_data['close']}")
                    
                    # Produce to Kafka
                    produce_to_kafka(message, symbol)
        
        except WebSocketConnectionClosedException as e:
            logger.warning(f"WebSocket closed: {e}. Reconnecting in 5 seconds...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Unexpected error in WebSocket loop: {e}")
            traceback.print_exc()
            logger.info("Reconnecting in 10 seconds...")
            time.sleep(10)
            
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass


# --- Main Logic ---
if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Binance Kline Kafka Producer (ohlc-exporter)")
    logger.info("=" * 60)
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"WebSocket URL: {BINANCE_WS_URL}")
    logger.info("=" * 60)
    
    # Start WebSocket client in a separate thread
    ws_thread = Thread(target=websocket_client)
    ws_thread.daemon = True
    ws_thread.start()
    
    # Keep main thread alive and ensure message delivery
    try:
        while True:
            time.sleep(0.1)  # Poll more frequently for better message delivery
            # Process delivery callbacks
            producer.poll(0)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
        logger.info("Flushing remaining messages...")
        # Flush with timeout to ensure all messages are sent
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"Warning: {remaining} messages were not delivered")
        else:
            logger.info("All messages flushed successfully")

