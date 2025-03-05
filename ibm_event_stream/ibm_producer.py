from kafka import KafkaProducer
import json
import ssl
import time
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = [
        "broker-2-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093",
        "broker-5-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093",
        "broker-3-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093",
        "broker-4-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093",
        "broker-0-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093",
        "broker-1-w1zrynsnrvtccy83.kafka.svc04.us-south.eventstreams.cloud.ibm.com:9093"
    ]
API_KEY = "H5seTYdlbnaTLZIY8RxvcjX--keLYYIhKWjMabGDz32x"
TOPIC = 'trading_topic'

def createProducer(retries=5, delay=5):
    for attempt in range(retries):
        try:
            print(f"Attempt {attempt + 1}: Connecting to Kafka brokers...")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username="token",
                ssl_context=ssl._create_unverified_context(),
                sasl_plain_password=API_KEY,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("Kafka Producer connected successfully!")
            return producer
        except NoBrokersAvailable as e:
            print(f"Error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple attempts.")


message = {
    "stock": "AAPL",
    "price": 185.75,
    "volume": 100,
    "timestamp": "2025-03-01T12:00:00Z"
}

# Initialize producer
producer = createProducer()

# Send message
message = {"stock": "AAPL", "price": 185.75, "volume": 100, "timestamp": "2025-03-01T12:00:00Z"}
try:
    producer.send(TOPIC, value=message)
    producer.flush()
    print("Message sent successfully!")
except Exception as e:
    print(f"Failed to send message: {e}")
