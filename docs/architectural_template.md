# MLOps Technical Documentation for Stock Market Forecasting

## Overview
This document outlines the MLOps pipeline for real-time stock market data ingestion, processing, model training, and live forecasting using Kafka, MongoDB, FastAPI, Airflow, MLflow, and an Angular web application.

---

## **Architecture Overview**
1. **Data Ingestion**: REST API provides stock market data to a Kafka producer.
2. **Processing & Storage**: Kafka consumer transforms the data and stores it in MongoDB or CSV. WebSocket sends real-time updates to the Angular app.
3. **Model Training & Scheduling**: Data from MongoDB is used to train models (ARIMA, LSTM, Transformers) scheduled via Airflow DAG. Models are stored using MLflow.
4. **Forecasting & Real-Time Predictions**: The Angular app displays real-time data via WebSockets, and the trained model provides forecasts via FastAPI.

---

## **Step 1: Data Ingestion (Kafka Producer)**

### **Components:**
- REST API (data source)
- Kafka Producer

### **Workflow:**
1. Fetch stock market data from the REST API.
2. Send the data to the Kafka topic.

### **Implementation (Python Kafka Producer Example):**
```python
from kafka import KafkaProducer
import json
import requests

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'
API_URL = 'https://api.example.com/stocks'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

response = requests.get(API_URL)
if response.status_code == 200:
    data = response.json()
    producer.send(KAFKA_TOPIC, data)
```

---

## **Step 2: Data Processing & Storage (Kafka Consumer)**

### **Components:**
- Kafka Consumer
- Data Transformation
- MongoDB / CSV Storage
- WebSocket for Real-Time Updates

### **Workflow:**
1. Kafka consumer fetches data from the topic.
2. Apply necessary transformations.
3. Store processed data in MongoDB or CSV.
4. Send data to Angular frontend via WebSocket.

### **Implementation (Python Kafka Consumer Example):**
```python
from kafka import KafkaConsumer
import json
from pymongo import MongoClient
import websocket

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'stock-data'
MONGO_URI = 'mongodb://localhost:27017'

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

client = MongoClient(MONGO_URI)
db = client['stock_db']
collection = db['stock_data']

def send_to_websocket(data):
    ws = websocket.create_connection('ws://localhost:8000/live')
    ws.send(json.dumps(data))
    ws.close()

for message in consumer:
    data = message.value
    collection.insert_one(data)
    send_to_websocket(data)
```

---

## **Step 3: Model Training & Scheduling with Airflow DAG**

### **Components:**
- Airflow DAG for scheduling
- ARIMA, LSTM, Transformer models
- MLflow for model tracking

### **Workflow:**
1. Retrieve training data from MongoDB.
2. Train ARIMA, LSTM, and Transformer models.
3. Log models using MLflow.
4. Schedule training jobs using Airflow.

### **Setup:**
1. **Airflow**:
    - pip install "apache-airflow[celery]==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt"
    - [Airflow Official documentation](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html)
    - export AIRFLOW_HOME=~/airflow  # Set Airflow home directory
    - airflow db init  # Initialize metadata database
    - airflow standalone
    - Access the UI at: http://localhost:8080

3. **Mlflow**:
    - pip install mlflow
    - [Mlflow Official documentation](https://mlflow.org/docs/latest/getting-started/intro-quickstart/index.html)
    - mlflow server --host 127.0.0.1 --port 4300
    - Access the UI at: http://localhost:4300

### **Implementation (Airflow DAG Example):**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mlflow
from my_ml_pipeline import train_model

def train_and_log_model():
    model, metrics = train_model()
    mlflow.log_params(metrics)
    mlflow.sklearn.log_model(model, 'model')

default_args = {'start_date': datetime(2024, 1, 1), 'schedule_interval': '@daily'}

dag = DAG('train_stock_model', default_args=default_args)
train_task = PythonOperator(task_id='train', python_callable=train_and_log_model, dag=dag)
```

---

## **Step 4: Forecasting & Real-Time Predictions**

### **Components:**
- FastAPI for Model Inference
- WebSocket for Live Forecasting
- Angular for UI Visualization

### **Workflow:**
1. Fetch real-time data from MongoDB.
2. Send data to the trained model via FastAPI.
3. WebSocket updates the Angular frontend with predictions.

### **Setup:**
1. **Fastapi**:
    - pip install fastapi uvicorn
    - uvicorn main:app --host 0.0.0.0 --port 3000
    - [Official documentation](https://fastapi.tiangolo.com/tutorial/)
    - Access the server at: http://localhost:3000
### **Implementation (FastAPI for Model Serving):**
```python
from fastapi import FastAPI
import mlflow
import json
import websocket

app = FastAPI()
model = mlflow.pyfunc.load_model('models:/latest')

@app.post('/predict')
def predict(data: dict):
    prediction = model.predict(data)
    return {'prediction': prediction.tolist()}

@app.websocket('/forecast')
def forecast_endpoint(websocket: websocket.WebSocket):
    websocket.accept()
    while True:
        data = websocket.receive_json()
        prediction = model.predict(data)
        websocket.send_json({'forecast': prediction.tolist()})
```

---

## **Step 5: Angular Web Application for Real-Time Data & Forecasting**

### **Components:**
- WebSocket Service for Real-Time Updates
- REST API for Predictions

### **Implementation (Angular WebSocket Service Example):**
```typescript
import { Injectable } from '@angular/core';
import { WebSocketSubject } from 'rxjs/webSocket';

@Injectable({ providedIn: 'root' })
export class StockService {
    private socket$ = new WebSocketSubject('ws://localhost:8000/forecast');

    sendData(data: any) {
        this.socket$.next(data);
    }

    getForecast() {
        return this.socket$;
    }
}
```

---

## **Conclusion**
This pipeline automates stock market data ingestion, processing, model training, and forecasting using Kafka, Airflow, FastAPI, MLflow, and Angular. The system ensures real-time updates and predictions via WebSockets, providing an end-to-end MLOps solution.

