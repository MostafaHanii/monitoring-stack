"""
Prometheus Kafka Consumer
Consumes kline data from Kafka and exposes Prometheus metrics
"""
import json
import os
import time
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Gauge

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance_kline")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "prometheus_consumer_group")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9001"))

# --- Kafka Consumer Setup ---
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
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
            print(f"🔄 Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            consumer = Consumer(consumer_config)
            consumer.subscribe([KAFKA_TOPIC])
            
            # Test connection by polling with short timeout
            test_msg = consumer.poll(timeout=2.0)
            print(f"✅ Connected to Kafka successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Max retries reached. Could not connect to Kafka.")
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
    try:
        data = json.loads(message_value)
        kline = data.get("data", {}).get("k", {})
        
        if not kline:
            return False
        
        symbol = kline.get("s")
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
        
        print(f"✅ Updated metrics: {symbol} - Close: {close_price}")
        return True
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        return False


# --- Main Consumer Loop ---
def consume_messages():
    """Main consumer loop"""
    print("🚀 Starting Kafka consumer...")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka error: {msg.error()}")
                    continue
            
            message_value = msg.value().decode('utf-8')
            process_message(message_value)
            
    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped by user")
    finally:
        consumer.close()
        print("✅ Kafka consumer closed")


# --- Main Logic ---
if __name__ == '__main__':
    print("=" * 60)
    print("🎯 Prometheus Kafka Consumer")
    print("=" * 60)
    print(f"📡 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📥 Kafka Topic: {KAFKA_TOPIC}")
    print(f"👥 Consumer Group: {KAFKA_GROUP_ID}")
    print(f"📊 Metrics Port: {METRICS_PORT}")
    print("=" * 60)
    
    try:
        start_http_server(METRICS_PORT)
        print(f"✅ Prometheus metrics server started on port {METRICS_PORT}")
    except Exception as e:
        print(f"❌ Could not start metrics server: {e}")
        exit(1)
    
    # Initialize Kafka consumer with retry logic
    if not init_kafka_consumer():
        print("❌ Failed to initialize Kafka consumer. Exiting...")
        exit(1)
    
    consume_messages()
