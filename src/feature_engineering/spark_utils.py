# --- FILE: src/feature_engineering/spark_utils.py ---
import os
import sys
from pyspark.sql import SparkSession

# 1. Setup Python Path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Import logger
from src.utils.logger import app_logger

def get_spark_session(app_name: str, mongo_uri: str, mongo_db: str, spark_master_url: str) -> SparkSession:
    """
    Khởi tạo và cấu hình SparkSession.
    Các packages (Kafka, Mongo) sẽ được nạp thông qua lệnh spark-submit (tham số --packages).
    """
    try:
        app_logger.info(f"Initializing SparkSession: {app_name} connecting to Master at {spark_master_url}")
        
        # Cấu hình SparkSession (KHÔNG CẦN config spark.jars.packages ở đây nữa)
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(spark_master_url) \
            .config("spark.mongodb.input.uri", f"{mongo_uri}/{mongo_db}") \
            .config("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db}") 
            
        spark = builder.getOrCreate()
        
        # Set log level để tránh spam log console
        spark.sparkContext.setLogLevel("WARN")
        app_logger.info("SparkSession initialized successfully.")
        
        return spark
    except Exception as e:
        app_logger.error(f"Error initializing SparkSession: {e}")
        # Reraise exception để dừng ứng dụng
        raise