"""
Prometheus Kafka Consumer
Consumes kline data from Kafka and exposes Prometheus metrics
"""
import json
import logging
import os
import time
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Gauge

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance_kline")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "prometheus_consumer_group")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9001"))

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Kafka Consumer Setup ---
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "latest",
    "enable.auto.commit": False,
    "client.id": "prometheus-consumer"
}

consumer = None

def init_kafka_consumer():
    """Initialize Kafka consumer with retry logic"""
    global consumer
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = Consumer(consumer_config)
            consumer.subscribe([KAFKA_TOPIC])
            
            # Test connection by polling with short timeout
            test_msg = consumer.poll(timeout=2.0)
            logger.info("Connected to Kafka successfully!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Max retries reached. Could not connect to Kafka.")
                return False
    
    return False

# --- Prometheus Metrics ---

gauge_close = Gauge(
    'close_price_1m_stats',
    'The 1m close price stats',
    ['symbol', 'interval']
)

gauge_open = Gauge(
    'open_price_1m_stats',
    'The 1m open price stats',
    ['symbol', 'interval']
)

gauge_high = Gauge(
    'high_price_1m_stats',
    'The 1m high price stats',
    ['symbol', 'interval']
)

gauge_low = Gauge(
    'low_price_1m_stats',
    'The 1m low price stats',
    ['symbol', 'interval']
)

gauge_volume = Gauge(
    'volume_price_1m_stats',
    'The 1m volume price stats',
    ['symbol', 'interval']
)

gauge_timestamp = Gauge(
    'kline_timestamp',
    'Kline close timestamp',
    ['symbol', 'interval']
)

# --- Message Processing ---
def process_message(message_value):
    """Parse Kafka message and update Prometheus metrics"""
    symbol = "UNKNOWN"
    try:
        data = json.loads(message_value)
        kline = data.get("data", {}).get("k", {})
        
        if not kline:
            logger.warning(f"No kline data in message: {message_value[:100]}")
            return False, symbol
        
        symbol = kline.get("s", "UNKNOWN")
        interval = kline.get("i")
        
        open_price = float(kline.get("o"))
        high_price = float(kline.get("h"))
        low_price = float(kline.get("l"))
        close_price = float(kline.get("c"))
        volume = float(kline.get("v"))
        close_time = int(kline.get("T"))
        
        # Update Prometheus metrics
        gauge_close.labels(symbol=symbol, interval=interval).set(close_price)
        gauge_open.labels(symbol=symbol, interval=interval).set(open_price)
        gauge_high.labels(symbol=symbol, interval=interval).set(high_price)
        gauge_low.labels(symbol=symbol, interval=interval).set(low_price)
        gauge_volume.labels(symbol=symbol, interval=interval).set(volume)
        gauge_timestamp.labels(symbol=symbol, interval=interval).set(close_time)
        
        logger.info(f"Updated metrics: {symbol} - Close: {close_price}")
        return True, symbol
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        return False, symbol
    except (ValueError, TypeError) as e:
        logger.error(f"Data conversion error for {symbol}: {e}")
        return False, symbol
    except Exception as e:
        logger.error(f"Error processing message for {symbol}: {e}")
        return False, symbol


# --- Main Consumer Loop ---
def consume_messages():
    """Main consumer loop"""
    logger.info("Starting Kafka consumer...")

    try:
        while True:
            messages = consumer.consume(num_messages=100, timeout=1.0)
            
            for msg in messages:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                success, symbol = process_message(msg.value().decode('utf-8'))
                
                if success:
                    consumer.commit(message=msg)

    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


# --- Main Logic ---
if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("Prometheus Kafka Consumer")
    logger.info("=" * 60)
    logger.info(f"Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"Consumer Group: {KAFKA_GROUP_ID}")
    logger.info(f"Metrics Port: {METRICS_PORT}")
    logger.info("=" * 60)
    
    try:
        start_http_server(METRICS_PORT)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
    except Exception as e:
        logger.error(f"Could not start metrics server: {e}")
        exit(1)
    
    # Initialize Kafka consumer with retry logic
    if not init_kafka_consumer():
        logger.error("Failed to initialize Kafka consumer. Exiting...")
        exit(1)
    
    consume_messages()
