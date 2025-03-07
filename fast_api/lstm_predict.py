import pandas as pd
import numpy as np
import pickle
from global_state import path

def lstm_prediction(feature, mlflow):
    print(f'Input shape : {feature.shape}')
    # Load Scaler
    with open(path +'fast_api/standardscaler/' + "y_scaler.pkl", "rb") as f:
        scaler = pickle.load(f)
    # logged_model = 'runs:/07f4a9544d9d4b73a139f7de76dd3aad/lstm_model'
    logged_model = 'runs:/f1cefc2508a14252a7a54b965e6b3502/lstm_model'
    # Load model as a PyFuncModel.
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    # Predict on a Pandas DataFrame.
    prediction = loaded_model.predict(feature)

     # Ensure correct reshaping for inverse transform
    prediction = prediction.reshape(-1, 1)  # Reshapes to (40, 1)

    # Perform inverse transformation
    future_predictions_inv = scaler.inverse_transform(prediction)
    return {"prediction": future_predictions_inv.tolist()}

