# Causal_Alpha

# MLOps Pipeline for Stock Market Data Processing and Forecasting

## Overview
This document outlines the MLOps architecture for real-time stock market data processing, storage, transformation, model training, and forecasting. The pipeline leverages Kafka, MongoDB, FastAPI, WebSockets, and MLFlow for seamless integration and deployment.

## Architecture Components
  ![Causal Alpha_2](https://github.com/user-attachments/assets/a10c16f3-863a-4758-b64a-1c974b320444)
  
1. **REST API Data Ingestion**
   - A REST API fetches real-time stock market data.
   - Data is sent to a Kafka producer for streaming.

2. **Kafka-based Data Processing**
   - A Kafka consumer receives the stock market data.
   - Data transformation is applied before storing it in MongoDB or a CSV file.
   - Transformed data is sent via WebSockets for live updates to the Angular application.

3. **Model Training & Scheduling with Airflow DAG and Storage**
   - Data from MongoDB is extracted for model training.
   - Models used for forecasting:
     - **ARIMA (AutoRegressive Integrated Moving Average)**
     - **LSTM (Long Short-Term Memory Networks)**
     - **Transformers-based models**
   - The trained models are logged and stored using MLFlow.
   - Schedule training jobs using Airflow

4. **Real-time Data Visualization and Forecasting**
   - Real-time stock market data is displayed on the Angular web application.
   - The saved model is loaded via FastAPI.
   - Incoming live data is passed to the model for predictions.
   - Forecasting results are sent via WebSockets to the Angular web application for real-time visualization.

---

## Technical Implementation Details

### 1. Data Ingestion
- **Technology Stack:** Kafka
- **Workflow:**
  - A REST API endpoint fetches real-time stock data.
  - Data is pushed to Kafka via a producer.

### 2. Data Processing
- **Technology Stack:** Kafka, FastAPI, MongoDB, WebSockets
- **Workflow:**
  - Kafka consumer reads the streaming data.
  - Data transformations are applied (normalization, feature extraction, etc.).
  - Processed data is stored in MongoDB and optionally in CSV format.
  - WebSockets stream the data to the Angular app for real-time updates.

### 3. Model Training and Management
- **Technology Stack:** Airflow, MLFlow, MongoDB, Python (ARIMA, LSTM, Transformers)
- **Workflow:**
  - Data is fetched from MongoDB.
  - Training is performed using ARIMA, LSTM, and Transformer models.
  - Trained models are logged and stored in MLFlow.
  - Schedule training jobs using Airflow

### 4. Real-time Forecasting and Visualization
- **Technology Stack:** FastAPI, WebSockets, Angular, MongoDB
- **Workflow:**
  - FastAPI serves as the backend for model inference.
  - Live stock data is passed to the trained models.
  - Predictions are generated and sent to the Angular frontend via WebSockets.
  - Forecasted results are displayed in real-time.

---

## Conclusion
This MLOps pipeline enables real-time stock data processing, transformation, training, and forecasting. The integration of Kafka, MongoDB, FastAPI, MLFlow, and WebSockets ensures a seamless workflow from data ingestion to visualization.



