from airflow import DAG
from airflow.operators.python import PythonOperator
import mlflow
import mlflow.sklearn
import mlflow.tensorflow

import pendulum
import yfinance as yf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from airflow.decorators import dag, task
import statsmodels.api as sm
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import itertools
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

# Set MLflow tracking URI
mlflow.set_tracking_uri("http://localhost:4300")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "start_date": pendulum.datetime(2025, 3, 4, 0, 0),
    "end_date": pendulum.datetime(2025, 6, 16, 0, 0),  # Stop after 15 minutes
}
dag = DAG(
    'Causal_Alpha_base_models',
    default_args=default_args,
    schedule='@daily',
    tags=["Rupesh"],
    catchup=False
)

def extract_data():
    # Download NIFTY50 data
    ticker = "^NSEI"  # NIFTY50 index
    df = yf.download(ticker, start="2010-01-01", end="2025-03-01")

    # Use only the closing price
    data = df[['Close']].dropna()
    plt.figure(figsize=(12, 6))
    plt.plot(data)
    plt.title("NIFTY50 Stock Prices")
    plt.show()
    data.shape

    # Save data to csv
    data.to_csv("~/ML_spark/dag_spark/Causal_Alpha/dataset/base_models/nifty50.csv", header=["Close"], index=True)
    return "extracted successfully"

def train_arima():
            # Use last 100 days data for Test & remaining for training
        data = pd.read_csv('~/ML_spark/dag_spark/Causal_Alpha/dataset/base_models/nifty50.csv', index_col=0, parse_dates=True)
        data.index = pd.to_datetime(data.index)
        data.index = pd.date_range(start="2010-01-01", periods=len(data), freq="D")  # 'D' for daily
        data_last_100 = data.iloc[-100:]
        data = data.iloc[:-100]

        # ADF Test Function
        def adf_test(series):
            result = adfuller(series)
            print("ADF Statistic:", result[0])
            print("p-value:", result[1])
            return result[1] > 0.05  # If p > 0.05, data is non-stationary

        # Check stationarity for different differencing levels
        diff_data = data['Close'].copy()
        d = 0
        while adf_test(diff_data):
            d += 1
            diff_data = diff_data.diff().dropna()

        print(f"Optimal Differencing Order (d): {d}")
        p_range = range(0, 5)
        d_range = [d]  # From ADF test
        q_range = range(0, 5)

        # Grid search
        best_aic = float("inf")
        best_order = None
        best_model = None

        for p, d, q in itertools.product(p_range, d_range, q_range):
            try:
                model = ARIMA(data['Close'], order=(p, d, q))
                result = model.fit()
                if result.aic < best_aic:
                    best_aic = result.aic
                    best_order = (p, d, q)
                    best_model = result
            except:
                continue
                # Create a new MLflow Experiment
        mlflow.set_experiment("ARIMA")
        print(f"Best ARIMA Order: {best_order} with AIC: {best_aic}")
        with mlflow.start_run():
            mlflow.log_params({"order":model.order, "Coefficients": best_model.params, "AIC":best_model.aic, "BIC":best_model.bic })
            mlflow.sklearn.log_model(best_model, "arima_model")
            mlflow.set_tag("baseline ARIMA", "Rupesh")


def train_lstm():
        data = pd.read_csv('~/ML_spark/dag_spark/Causal_Alpha/dataset/base_models/nifty50.csv', index_col=0)
        def create_sequences(data, seq_length):
            X, y = [], []
            for i in range(len(data) - seq_length):
                X.append(data[i:i+seq_length])
                y.append(data.iloc[i+seq_length])
            return np.array(X), np.array(y)

        seq_length = 60  # 60 days window
        X, y = create_sequences(data, seq_length)

        X_final, y_final = X[:-100], y[:-100]
        # Split data into train, test
        train_size = int(0.8 * len(X_final))
        X_train, X_test = X_final[:train_size], X_final[train_size:]
        y_train, y_test = y_final[:train_size], y_final[train_size:]

        # Reshape X_train and X_test to 2D before scaling
        X_train_2D = X_train.reshape(X_train.shape[0], -1)  # Reshape to 2D for scaling
        X_test_2D = X_test.reshape(X_test.shape[0], -1)    # Reshape to 2D for scaling

        # Data Scaling
        scaler = MinMaxScaler(feature_range=(0, 1))
        X_train_scaled = scaler.fit_transform(X_train_2D)
        X_test_scaled = scaler.transform(X_test_2D)

        y_train_scaled = scaler.fit_transform(y_train.reshape(-1, 1))
        y_test_scaled = scaler.transform(y_test.reshape(-1, 1))

         # Reshape back to original shape
        X_train_scaled = X_train_scaled.reshape(X_train.shape)  # Reshape back to (2849, 60, 1)
        X_test_scaled = X_test_scaled.reshape(X_test.shape)    # Reshape back to (713, 60, 1)

        # Build LSTM Model
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=(seq_length, 1)),
            Dropout(0.2),
            LSTM(50, return_sequences=False),
            Dropout(0.2),
            Dense(25),
            Dense(1)
        ])

        model.compile(optimizer="adam", loss="mse")
        model.fit(X_train_scaled, y_train_scaled, epochs=20, batch_size=32, validation_data=(X_test_scaled, y_test_scaled))
        mlflow.set_experiment("LSTM")
        with mlflow.start_run(tags={"base_model": "Rupesh"}):
            mlflow.tensorflow.log_model(model, "lstm_model")
            mlflow.set_tag("Rupesh", "baseline LSTM")


extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
train_arima_task = PythonOperator(task_id='train_arima', python_callable=train_arima, dag=dag)
train_lstm_task = PythonOperator(task_id='train_lstm', python_callable=train_lstm, dag=dag)

extract_task >> [train_arima_task, train_lstm_task]


