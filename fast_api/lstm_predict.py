import json
import pandas as pd
import numpy as np
import pickle
from global_state import path

def lstm_prediction(feature, mlflow, model_uri="", run_id="f1cefc2508a14252a7a54b965e6b3502"):
    print(f'Input shape : {feature.shape}')
    # Load Scaler
    with open(path +'fast_api/standardscaler/' + "y_scaler.pkl", "rb") as f:
        scaler = pickle.load(f)
  
    logged_model = model_uri if model_uri else f'runs:/{run_id}/lstm_model'
    # logged_model = 'mlflow-artifacts:/247434592383389581/ae065c24875e42f585230dd98f584198/artifacts/lstm_model'
    # Load model as a PyFuncModel.
    print(f'logged_model : {logged_model}')
    loaded_model = mlflow.pyfunc.load_model(logged_model)
    # Predict on a Pandas DataFrame.
    prediction = loaded_model.predict(feature)

     # Ensure correct reshaping for inverse transform
    prediction = prediction.reshape(-1, 1)  # Reshapes to (40, 1)

    # Perform inverse transformation
    future_predictions_inv = scaler.inverse_transform(prediction)
    return future_predictions_inv.tolist()

def get_all_registered_versions(model_name: str, client):
    print(model_name)
    # Initialize MLflow Client
    all_versions = []
    # Fetch all versions of the model
    versions = client.search_model_versions(f"name='{model_name}'")
    for v in versions:
        all_versions.append({"version":v.version, "tag": dict(v.tags), "alias": list(v.aliases), "source": v.source, "run_id": v.run_id})
    # return json.dumps(all_versions, indent=2)
    return all_versions


