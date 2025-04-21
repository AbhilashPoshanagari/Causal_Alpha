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
from mlflow.models import infer_signature
from airflow.models import TaskInstance
import logging
import os

# Configure logger
logger = logging.getLogger(__name__)
# Set MLflow tracking URI
# mlflow.set_tracking_uri("http://localhost:4300")
mlflow.set_tracking_uri("http://44.199.208.18:4300/")
# dag_folder = os.path.dirname(os.path.abspath(__name__))
dag_folder = '/home/abhilash/ML_spark/dag_spark/Causal_Alpha/dags'
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
    logger.info(dag_folder)
    csv_path = os.path.join(dag_folder,"../","dataset", "base_models","nifty50.csv")
    file_path = os.path.abspath(csv_path)
    # Save data to csv
    data.to_csv(file_path, header=["Close"], index=True)
    return "extracted successfully"

def train_arima(**kwargs):
            # Use last 100 days data for Test & remaining for training
        csv_path = os.path.join(dag_folder,"..","dataset", "base_models","nifty50.csv")
        file_path = os.path.abspath(csv_path)
        data = pd.read_csv(file_path, index_col=0, parse_dates=True)
        data.index = pd.to_datetime(data.index)
        data = data.reset_index(drop=True)
        data.index = pd.date_range(start="2010-01-01", periods=len(data), freq="D")  # 'D' for daily
        data_last_100 = data.iloc[-100:]
        data = data.iloc[:-100]

        # ADF Test Function
        def adf_test(series):
            result = adfuller(series)
            logger.info("ADF Statistic:", result[0])
            logger.info("p-value:", result[1])
            return result[1] > 0.05  # If p > 0.05, data is non-stationary

        # Check stationarity for different differencing levels
        diff_data = data['Close'].copy()
        d = 0
        while adf_test(diff_data):
            d += 1
            diff_data = diff_data.diff().dropna()

        logger.info(f"Optimal Differencing Order (d): {d}")
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
        logger.info(f"Best ARIMA Order: {best_order} with AIC: {best_aic}")
        future_steps = 100
        forecast_result = best_model.get_forecast(steps=future_steps)
        forecast_values = forecast_result.predicted_mean
        forecast_ci = forecast_result.conf_int()

        # Generate future dates
        future_dates = data_last_100.index

        # Convert forecast results to DataFrame
        forecast_df = pd.DataFrame({
            "Date": future_dates,
            "Predicted_Close": forecast_values.values,
            "Lower_Bound": forecast_ci.iloc[:, 0].values,
            "Upper_Bound": forecast_ci.iloc[:, 1].values
        })
        # Ensure 'Close' is a Series, not a DataFrame
        data['Close'] = data['Close'].squeeze()
        mlflow.set_experiment("ARIMA_baseline")
        with mlflow.start_run():
            mlflow.log_params({"order":best_order, "Coefficients": best_model.params, "AIC":best_model.aic, "BIC":best_model.bic })
            # Log the model
            predictions = best_model.predict(start=0, end=len(data)-1)
            signature = infer_signature(data['Close'].values.reshape(-1, 1), predictions.values.reshape(-1, 1))
            mlflow.set_tag("ARIMA baseline", "Rupesh")
            model_info = mlflow.statsmodels.log_model(
                best_model,
                artifact_path="arima_base_model",
                signature=signature,
                input_example=data['Close'].values.reshape(-1, 1),
                registered_model_name="arima_base_model"
            )

            logger.info("model info : ", model_info)
            # Serialize the model_info to a dictionary
        model_info_dict = {
            'model_uri': model_info.model_uri
        }
        # Push the model_info to XCom
        forecast_json = forecast_df.to_json()
        task_instance: TaskInstance = kwargs['ti']
        task_instance.xcom_push(key="model_info", value=model_info_dict)
        task_instance.xcom_push(key="forecast_df", value=forecast_json)
        return model_info_dict


def train_lstm(**kwargs):
        csv_path = os.path.join(dag_folder,"..","dataset", "base_models","nifty50.csv")
        file_path = os.path.abspath(csv_path)
        data = pd.read_csv(file_path, index_col=0)
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
    
        mlflow.set_experiment("LSTM_baseline")
        with mlflow.start_run():
            mlflow.log_params({"model_type": "LSTM", "sliding_window":60, "layer1": 50, "Dropout":0.2, "layer2": 50, 
                               "dense":25, "optimizer":"adam", "loss":"mse" })
            # Log the model
            signature = infer_signature(X_train_scaled, model.predict(X_train_scaled))
            mlflow.set_tag("LSTM baseline", "Rupesh")
            model_info = mlflow.tensorflow.log_model(
                model,
                artifact_path="lstm_base_model",
                signature=signature,
                input_example=X_train_scaled,
                registered_model_name="lstm_base_model"
            )
            logger.info("model info : ", model_info)
            # Serialize the model_info to a dictionary
        model_info_dict = {
             'model_uri': model_info.model_uri
        }
            # Push the model_info to XCom
        task_instance: TaskInstance = kwargs['ti']
        task_instance.xcom_push(key="lstm_model_info", value=model_info_dict)
        return model_info_dict
    
def evaluate_arima_model(**kwargs):
    task_instance: TaskInstance = kwargs['ti']
    model_info = task_instance.xcom_pull(task_ids='train_arima', key='model_info')
    forecast_json = task_instance.xcom_pull(task_ids='train_arima', key='forecast_df')

    forecast_df = pd.read_json(forecast_json)
    logger.info("Forecast DF Columns: %s", forecast_df.columns)
    logger.info("Forecast DF Head:\n%s", forecast_df.head(10).to_string())

    # Check if "Predicted_Close" exists
    if "Predicted_Close" not in forecast_df.columns:
        logger.error("ERROR: 'Predicted_Close' column is missing in forecast_df!")
        return
    csv_path = os.path.join(dag_folder,"..","dataset", "base_models","nifty50.csv")
    file_path = os.path.abspath(csv_path)
    data = pd.read_csv(
        file_path, 
        index_col=0, parse_dates=True
    )
    logger.info("CSV Data Preview:\n%s", data.head().to_string())
    logger.info("Original Index Range: %s to %s", data.index.min(), data.index.max())

    data = data.asfreq("D")  # Ensure daily frequency without overwriting original dates

    data_last_100 = data.iloc[-100:]
    logger.info("Missing values before fill:\n%s", data_last_100.isnull().sum())

    # Fill missing values
    data_last_100.fillna(method="ffill", inplace=True)
    data_last_100 = data_last_100[['Close']].dropna()

    logger.info("Missing values after fill:\n%s", data_last_100.isnull().sum())

    # Align indices
    forecast_df = forecast_df.reset_index(drop=True)
    data_last_100 = data_last_100.reset_index(drop=True)

    # Assign actual values
    forecast_df["Actual_Close"] = data_last_100["Close"]
    logger.info("Missing values in forecast_df after assignment:\n%s", forecast_df.isnull().sum())

    forecast_df["Error"] = forecast_df["Predicted_Close"] - forecast_df["Actual_Close"]
    forecast_df["Error_Percentage"] = (forecast_df["Error"] / forecast_df["Actual_Close"]) * 100
    csv_path = os.path.join(dag_folder,"..","dataset", "base_models","forecast_df.csv")
    file_path = os.path.abspath(csv_path)
    forecast_df.to_csv(file_path)

    logger.info(forecast_df["Actual_Close"].head(10).to_string())
    # Load the model from MLflow
    model_uri = model_info.get("model_uri")
    model = mlflow.pyfunc.load_model(model_uri)
    # Compute evaluation metrics
    mse = mean_squared_error(forecast_df["Actual_Close"], forecast_df["Predicted_Close"])
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(forecast_df["Actual_Close"], forecast_df["Predicted_Close"])
    r2 = r2_score(forecast_df["Actual_Close"], forecast_df["Predicted_Close"])

    logger.info(f"Metrics - MSE: {mse}, RMSE: {rmse}, MAE: {mae}, R2 Score: {r2}")
    # with mlflow.start_run(run_id=latest_run_id):
    run_id = model_uri.split("/")[-2] if "runs:/" in model_uri else None
    logger.info(f'model_uri : {model_uri} ---> {run_id}')
    # Ensure we log metrics to the correct run
    if run_id:
        with mlflow.start_run(run_id=run_id):
            mlflow.log_metrics({"MSE": mse, "RMSE": rmse, "MAE": mae, "R2_Score": r2})
    else:
        logger.info("Error: Could not extract run_id from model_uri. Logging to default run.")

def evaluate_lstm_model(**kwargs):
        task_instance: TaskInstance = kwargs['ti']
        model_info = task_instance.xcom_pull(task_ids='train_lstm', key='lstm_model_info')
        csv_path = os.path.join(dag_folder,"..","dataset", "base_models","nifty50.csv")
        file_path = os.path.abspath(csv_path)
        data = pd.read_csv(file_path, index_col=0)
        forecast_json = task_instance.xcom_pull(task_ids='train_arima', key='forecast_df')
        forecast_df = pd.read_json(forecast_json)
        forecast_values = forecast_df["Predicted_Close"]
        # Load the model from MLflow
        model_uri = model_info.get("model_uri")
        model = mlflow.pyfunc.load_model(model_uri)
        def create_sequences(data, seq_length):
            X, y = [], []
            for i in range(len(data) - seq_length):
                X.append(data[i:i+seq_length])
                y.append(data.iloc[i+seq_length])
            return np.array(X), np.array(y)

        seq_length = 60  # 60 days window
        X, y = create_sequences(data, seq_length)

        X_train_val, y_train_val = X[:-100], y[:-100]
        # Split data into train, test
        train_size = int(0.8 * len(y_train_val))
        X_train, X_val = X_train_val[:train_size], X_train_val[train_size:]
        y_train, y_val = y_train_val[:train_size], y_train_val[train_size:]

        # Reshape X_train and X_test to 2D before scaling
        X_train_2D = X_train.reshape(X_train.shape[0], -1)  # Reshape to 2D for scaling
        X_val_2D = X_val.reshape(X_val.shape[0], -1)    # Reshape to 2D for scaling

        X_test, Y_test = X[-100:], y[-100:]
        X_test_100_2D = X_test.reshape(X_test.shape[0], -1)
        Y_test_100_2D = Y_test.reshape(Y_test.shape[0], -1)

        scaler = MinMaxScaler(feature_range=(0, 1))
        X_train_scaled = scaler.fit_transform(X_train_2D)
        X_val_scaled = scaler.transform(X_val_2D)
        X_test_100_scaled = scaler.transform(X_test_100_2D)

        y_train_scaled = scaler.fit_transform(y_train.reshape(-1, 1))
        y_val_scaled = scaler.transform(y_val.reshape(-1, 1))
        y_test_100_scaled = scaler.transform(Y_test.reshape(-1, 1))
        logger.info(f'model uri : {model_info.get("model_uri")}')

        """#### Predict Stock Price of t+1 using LSTM"""
        # Predictions
        X_test_100_scaled = X_test_100_scaled.reshape(X_test_100_scaled.shape[0], X_test_100_scaled.shape[1], 1)
        y_pred = model.predict(X_test_100_scaled)  # X_last_100_scaled now has the correct shape
        y_pred = y_pred.reshape(-1, 1)
        y_pred_inv = scaler.inverse_transform(y_pred)

        """#### To Predict t+100 (Next 100 Days) using LSTM"""
        # Use the last available sequence from the test set as input
        last_seq = X_val_scaled[-1].astype(np.float64)  # Shape: (60, 1)
        future_predictions = []

        # Predict next 50 days
        for _ in range(100):
            next_pred = model.predict(last_seq.reshape(1, seq_length, 1))  # Predict 1 day ahead
            future_predictions.append(next_pred[0, 0])  # Save prediction

            # Update sequence: Remove first day & append new prediction
            last_seq = np.append(last_seq[1:], next_pred).reshape(seq_length, 1).astype(np.float64)

        # Inverse transform predictions to original scale
        future_predictions_inv = scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))

        # Generate date range for future predictions
        # future_dates = pd.date_range(start=df.index[-1], periods=51)[1:]
        data_last_100 = data.iloc[-100:]
        future_dates = data_last_100.index

        # logger.info Predicted Prices
        predicted_prices_df = pd.DataFrame({"Date": future_dates, "Predicted_Close": future_predictions_inv.flatten(), "Actual_Close": Y_test.flatten()})
        logger.info(predicted_prices_df.head())
        # Final Forecast DF
        final_forecast_df = pd.DataFrame({"Date": future_dates, "Actual_Close": Y_test.flatten(),
                                        "Predicted_Close_LSTM": future_predictions_inv.flatten(),
                                        "Predicted_Close_ARIMA": forecast_values})
        logger.info(final_forecast_df.head())

        # Save final_forecast_df
        csv_path = os.path.join(dag_folder,"..","dataset", "base_models","final_forecast_df.csv")
        file_path = os.path.abspath(csv_path)
        final_forecast_df.to_csv(file_path, index=False)

                # Evaluation Metrics
        def evaluate_model(y_true, y_pred):
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mae = mean_absolute_error(y_true, y_pred)
            mape = np.mean(np.abs((y_true - y_pred) / y_true)) * 100
            direction_accuracy = np.mean((np.sign(np.diff(y_true)) == np.sign(np.diff(y_pred))).astype(int)) * 100

            # Sharpe Ratio Calculation
            returns = np.diff(y_pred) / y_pred[:-1]
            sharpe_ratio = np.mean(returns) / np.std(returns) if np.std(returns) != 0 else 0

            r_squared = r2_score(y_true, y_pred)

            # Model Accuracy
            model_accuracy = 1 - (mae / np.mean(y_true))
            return {"RMSE": rmse, "MAE": mae, "MAPE": mape, "Directional Accuracy": direction_accuracy, "Sharpe Ratio": sharpe_ratio, "r_squared":r_squared, "Model Accuracy": model_accuracy}
        lstm_metrics = evaluate_model(final_forecast_df["Actual_Close"], final_forecast_df["Predicted_Close_LSTM"]) # LSTM Metrics
        
        
        run_id = model_uri.split("/")[-2] if "runs:/" in model_uri else None
        logger.info(f'model_uri : {model_uri} ---> {run_id}')
        # Ensure we log metrics to the correct run
        if run_id:
            with mlflow.start_run(run_id=run_id):
                mlflow.log_metrics(lstm_metrics)
        else:
            print("Error: Could not extract run_id from model_uri. Logging to default run.")

extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
train_arima_task = PythonOperator(task_id='train_arima', python_callable=train_arima, dag=dag )
train_lstm_task = PythonOperator(task_id='train_lstm', python_callable=train_lstm, dag=dag)
evaluate_arima = PythonOperator(task_id='ARIMA_evaluate', python_callable=evaluate_arima_model, dag=dag)
evaluate_lstm = PythonOperator(task_id="LSTM_evaluate", python_callable=evaluate_lstm_model, dag=dag)
extract_task >> [train_arima_task, train_lstm_task]
train_arima_task >> evaluate_arima  # Define explicitly
train_lstm_task >> evaluate_lstm    # Define explicitly