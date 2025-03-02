import logging
import time
from venv import logger
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook

from kafka import KafkaProducer
import json
import ssl
from kafka.errors import NoBrokersAvailable
import pendulum
from airflow.hooks.base import BaseHook

API_CONN_ID = 'twelvedata'
# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to store the producer
kafka_producer_cache = {}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "start_date": pendulum.datetime(2025, 3, 1, 0, 0),
    "end_date": pendulum.datetime(2025, 4, 28, 0, 0),  # Stop after 15 minutes
}
@dag(
    schedule="@daily",
    default_args=default_args,
    tags=["casual"],
    catchup=False
)
def casual_etl_pipeline():
    @task()
    def real_time_stock():      
        # Use Http hook to get connection details from Airflow.
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        end_point = f'/time_series?apikey=b711ee134c5f4896837a807a61537efb&interval=1min&symbol=EUR/USD&country=India,%20IN&exchange=NSEI&type=stock&outputsize=12&timezone=exchange&start_date=2025-03-01%2000:00:00&end_date=2025-03-05%2015:59:00&format=JSON'
        response = http_hook.run(end_point)
        # Check for successful response
        if response.status_code == 200:
            response_dict = response.json()  # Convert response to dictionary
            # Extract time series data
            if "values" in response_dict:
                print(response_dict["values"])
                return response_dict["values"]
            else:
                return "Error: 'values' key not found in response."
        else:
            return f"Error: {response.status_code}, {response.text}"

    @task()
    def transform_etl(stocks):
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

        producer = createProducer()
        if stocks and isinstance(stocks, list):
            for stock in stocks:
                producer.send(TOPIC, value=stock)
            producer.flush()
            print("Messages sent successfully!")
        else:
            print("No valid stock data to send.")    

    raw_data = real_time_stock()
    transform_etl(raw_data)
casual_etl_pipeline()
