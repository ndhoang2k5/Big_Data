from pyspark.sql import SparkSession
from src.feature_engineering.config import SPARK_APP_NAME
from src.utils.logger import app_logger

def get_spark_session():
    """
    Initializes or retrieves an existing SparkSession.
    Spark will automatically connect to the Master provided by spark-submit.
    Adds Rate Limiting configuration (maxOffsetsPerTrigger) to prevent falling behind.
    """
    app_logger.info(f"Initializing Spark Session for app: {SPARK_APP_NAME}")
    spark = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    # ✅ FIX 1: Limit the number of offsets read per trigger to control backpressure.
    # At 500 msg/s, 5 seconds is 2500 messages. Setting 3000 provides a small buffer.
    .config("spark.sql.streaming.kafka.maxOffsetsPerTrigger", "3000")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    app_logger.info("Spark Session initialized successfully.")
    return spark