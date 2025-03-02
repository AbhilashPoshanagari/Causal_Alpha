from kafka import KafkaConsumer
import json
import ssl
import time
from kafka.errors import NoBrokersAvailable
# IBM Event Streams Configuration
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

def createConsumer(retries=5, delay=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,  # Topic name
                bootstrap_servers=BOOTSTRAP_SERVERS,
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_plain_username="token",
                sasl_plain_password=API_KEY,  # Replace with your actual API key
                ssl_context=ssl._create_unverified_context(),
                group_id="trading_consumer_group",  # Consumer group ID
                auto_offset_reset="earliest",  # Start reading from the beginning
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))  # Deserialize JSON messages
            )
            return consumer
        except NoBrokersAvailable as e:
            print(f"Error: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple attempts.")

consumer = createConsumer()
print("Listening for messages...")
try:
    for message in consumer:
        print(f"Received: {message.value}")
except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    consumer.close()
