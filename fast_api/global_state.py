global_state = {"is_running": False}
LOCAL_KAFKA_BROKER = 'localhost:9092'

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
path = '/home/abhilash/ML_spark/dag_spark/Causal_Alpha/'
mlflow_url:str = 'http://localhost:4300'

def set_streaming(value):
    global_state["is_running"] = value

def get_streaming():
    return global_state["is_running"]
