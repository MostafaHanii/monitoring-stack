"""
InfluxDB Kafka Consumer
Consumes kline data from Kafka and writes to InfluxDB
"""
import json
import os
import time
from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "binance_kline")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "influxdb_consumer_group")

INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "mytoken123")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "myorg")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "crypto_data")

# --- Kafka Consumer Setup ---
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "client.id": "influxdb-consumer"
}

consumer = Consumer(consumer_config)
consumer.subscribe([KAFKA_TOPIC])

# --- InfluxDB Client Setup ---
influx_client = None
write_api = None

def init_influxdb():
    """Initialize InfluxDB client with retry logic"""
    global influx_client, write_api
    
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 Attempting to connect to InfluxDB (attempt {attempt + 1}/{max_retries})...")
            influx_client = InfluxDBClient(
                url=INFLUXDB_URL,
                token=INFLUXDB_TOKEN,
                org=INFLUXDB_ORG
            )
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
            
            # Test connection
            health = influx_client.health()
            if health.status == "pass":
                print(f"✅ Connected to InfluxDB successfully!")
                return True
            else:
                print(f"⚠️ InfluxDB health check failed: {health.message}")
                
        except Exception as e:
            print(f"❌ Failed to connect to InfluxDB: {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Max retries reached. Could not connect to InfluxDB.")
                return False
    
    return False


# --- Message Processing ---
def process_message(message_value):
    """Parse Kafka message and write to InfluxDB"""
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
        
        # Create InfluxDB point
        point = (
            Point("kline_1m")
            .tag("symbol", symbol)
            .tag("interval", interval)
            .field("open", open_price)
            .field("high", high_price)
            .field("low", low_price)
            .field("close", close_price)
            .field("volume", volume)
            .time(close_time * 1000000)  # Convert ms to ns
        )
        
        # Write to InfluxDB
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        
        print(f"✅ Written to InfluxDB: {symbol} - Close: {close_price}")
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
        if influx_client:
            influx_client.close()
        print("✅ Kafka consumer and InfluxDB client closed")


# --- Main Logic ---
if __name__ == '__main__':
    print("=" * 60)
    print("🎯 InfluxDB Kafka Consumer")
    print("=" * 60)
    print(f"📡 Kafka Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"📥 Kafka Topic: {KAFKA_TOPIC}")
    print(f"👥 Consumer Group: {KAFKA_GROUP_ID}")
    print(f"💾 InfluxDB URL: {INFLUXDB_URL}")
    print(f"🗂️  InfluxDB Bucket: {INFLUXDB_BUCKET}")
    print("=" * 60)
    
    # Initialize InfluxDB
    if not init_influxdb():
        print("❌ Failed to initialize InfluxDB. Exiting...")
        exit(1)
    
    # Start consuming
    consume_messages()
