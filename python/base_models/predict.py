import pandas as pd
import mlflow
import numpy as np
import pickle
import os
path = '/home/abhilash/ML_spark/dag_spark/Causal_Alpha/'
mlflow.set_tracking_uri('http://localhost:4300')
def features():
    data = pd.read_csv(path + 'dataset/base_models/nifty50_test.csv',index_col=0)
    def create_sequences(data, seq_length):
        X, y = [], []
        for i in range(len(data) - seq_length):
            X.append(data[i:i+seq_length])
            y.append(data.iloc[i+seq_length])
        return np.array(X), np.array(y)
    seq_length = 60  # 60 days window
    X, y = create_sequences(data, seq_length)
    X_train_2D = X.reshape(X.shape[0], -1)  # Reshape to 2D for scaling
    # Load Scaler
    with open(path +'python/base_models/' + "scaler.pkl", "rb") as f:
        scaler = pickle.load(f)
    # Apply the same scaling transformation
    X_test_100_scaled = scaler.transform(X_train_2D)
    X_test_100_scaled = X_test_100_scaled.reshape(-1, X_test_100_scaled.shape[1],1)
    return X_test_100_scaled

def predict_single_value(feature):
    print(f'Input shape : {feature.shape}')
    # logged_model = 'runs:/07f4a9544d9d4b73a139f7de76dd3aad/lstm_model'
    logged_model = 'runs:/f1cefc2508a14252a7a54b965e6b3502/lstm_model'
    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    # Predict on a Pandas DataFrame.
    prediction = loaded_model.predict(feature)
    return {"prediction": prediction}

x = features()
prediction = predict_single_value(x)
print(f'results : {prediction}')
