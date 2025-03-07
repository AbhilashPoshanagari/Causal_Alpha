import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from streaming.kafka_consumer import consumer_msg, get_consumer
from kafka import KafkaConsumer
from global_state import set_streaming, get_streaming, mlflow_url, origins
from fastapi.middleware.cors import CORSMiddleware
from streaming.ibm_consumer import ibm_consumer, ibm_consumer_msg
from lstm_predict import lstm_prediction
from extract_features import get_features
import mlflow

app = FastAPI()

mlflow.set_tracking_uri(mlflow_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# try:
#     consumer = get_consumer()
#     server = 'local'
#     print(f'local consumer : {consumer}')
# except:
#     try:
#         consumer = ibm_consumer()
#         server = 'ibm'
#         print(f'ibm consumer : {consumer}')
#     except:
#         consumer = False
#         server = 'NoBrokersAvailable'
#         print(f'consumer : {consumer}')
    
# Store connected users
connected_clients = set()

class User(BaseModel):
    username: str
    password: str

@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: str = None):
    return {"item_id": item_id, "query": q}

@app.post("/users/")
def get_all_users(user: User):
    return {"user_count": 20, "request": user}

@app.get("/mlflow/predict")
def predict_stock_prices():
    features = get_features()
    prediction = lstm_prediction(feature=features, mlflow=mlflow)
    return {"prediction": prediction}


# @app.get('/kafka/start')
# async def start_kafka_stream():
#     """API to start WebSocket streaming."""
#     if not get_streaming():
#         is_running = True
#         set_streaming(is_running)
#         if server == 'local':
#             for ws in connected_clients:
#                 print("Starting WebSocket streaming task...")
#                 asyncio.create_task(consumer_msg(ws=ws, consumer=consumer))
#         elif server == 'ibm':
#             for ws in connected_clients:
#                 print("Starting WebSocket streaming task...")
#                 asyncio.create_task(ibm_consumer_msg(ws=ws, consumer=consumer))
#         else:
#             return {"message": "NoBrokersAvailable"}
        
#     return {"message": "WebSocket streaming started"}

# @app.get("/kafka/stop")
# async def stop_stream():
#     """API to stop WebSocket streaming."""
#     is_running = False  # Stop sending data
#     set_streaming(is_running)
#     # Notify clients that the stream has stopped
#     for ws in connected_clients:
#         await ws.send_json({"message": "WebSocket streaming stopped"})

#     return {"message": "WebSocket streaming stopped"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    print("New WebSocket client connected!")
    try:
        while True:
            data = await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        print("WebSocket client disconnected!")
        connected_clients.remove(websocket)
    finally:
        await websocket.close()

