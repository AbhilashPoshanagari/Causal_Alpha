import requests
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType        
import pendulum
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": pendulum.datetime(2025, 2, 26, 0, 0),
    "end_date": pendulum.datetime(2025, 4, 28, 0, 15),  # Stop after 15 minutes
}
@dag(
    schedule="*/1 * * * *",
    default_args=default_args,
    tags=["CDS"]
)
def ETL_task():
    @task()
    def real_time_stock():
        # API URL
        url = "https://api.twelvedata.com/time_series"
        params = {
            "apikey": "b711ee134c5f4896837a807a61537efb",  # Replace with your actual API key
            "interval": "1min",
            "format": "JSON",
            "country": "India, IN",
            "exchange": "NSEI",
            "timezone": "Asia/Kolkata",
            "start_date": "2025-02-25 17:25:00",
            "end_date": "2025-02-26 17:25:00",
            "dp": 4,
            "outputsize": 2,
            "type": "stock",
            "symbol": "EUR/USD"
        }
        # Make API request
        response = requests.get(url, params=params)
        
        # Check for successful response
        if response.status_code == 200:
            response_dict = response.json()  # Convert response to dictionary
            # Extract time series data
            if "values" in response_dict:
                return response_dict["values"]
            else:
                return "Error: 'values' key not found in response."
        else:
            return f"Error: {response.status_code}, {response.text}"

    @task()
    def publish_stack_data(df):
        spark = SparkSession.builder.master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        # Define Schema
        schema = StructType([
            StructField("datetime", StringType(), True),
            StructField("open", StringType(), True),
            StructField("high", StringType(), True),
            StructField("low", StringType(), True),
            StructField("close", StringType(), True)
        ])
        
        # Convert JSON-like list to tuples for Spark DataFrame
        tuple_format = []
        for each_obj in df:
            formatted_datetime = datetime.strptime(each_obj["datetime"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
            tuple_format.append((
                formatted_datetime,
                each_obj["open"],
                each_obj["high"],
                each_obj["low"],
                each_obj["close"]
            ))
        df = spark.createDataFrame(tuple_format, schema)
        data = tuple_format
        # Convert to Kafka-compatible format
        kafka_df = df.selectExpr(
                    "cast(datetime as string) as key",
                    """to_json(named_struct(
                        'open', cast(open as string), 
                        'high', cast(high as string), 
                        'low', cast(low as string), 
                        'close', cast(close as string)
                    )) as value"""
                )
        # Write Data to Kafka (non-streaming)
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "http://localhost:9092") \
            .option("topic", "trading-topic") \
            .save()

    raw_data = real_time_stock()
    publish_stack_data(raw_data)
ETL_task()
