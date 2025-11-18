from pyspark.sql import SparkSession
from src.feature_engineering.config import SPARK_APP_NAME
from src.utils.logger import app_logger

def get_spark_session():
    """
    Khởi tạo hoặc lấy SparkSession hiện có.
    Spark sẽ tự động kết nối với Master được cung cấp bởi spark-submit.
    """
    app_logger.info(f"Initializing Spark Session for app: {SPARK_APP_NAME}") # <--- Bỏ master khỏi log
    spark = (
    SparkSession.builder
    .appName(SPARK_APP_NAME)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    app_logger.info("Spark Session initialized successfully.")
    return spark