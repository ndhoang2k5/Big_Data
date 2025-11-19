# stock_price_prediction/src/feature_engineering/pyspark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg, round, concat_ws, current_date, hour, minute
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from src.feature_engineering.spark_utils import get_spark_session

# Import config (bao gồm cả MongoDB config)
try:
    from src.feature_engineering.config import (
        KAFKA_BROKER_SERVERS,
        KAFKA_RAW_DATA_TOPIC,
        BATCH_INTERVAL_SECONDS,
        CHECKPOINT_LOCATION,
        MONGO_URI, 
        MONGO_DATABASE, 
        MONGO_COLLECTION_FEATURES 
    )
except ImportError:
    # Fallback cho môi trường dev/test
    KAFKA_BROKER_SERVERS = "localhost:9092"
    KAFKA_RAW_DATA_TOPIC = "stock_raw_data"
    BATCH_INTERVAL_SECONDS = 15 
    CHECKPOINT_LOCATION = "D:/bigdata/FINAL/tmp_checkpoint"
    MONGO_URI = "mongodb://localhost:27017/stock_data_db" 
    MONGO_DATABASE = "stock_data_db"
    MONGO_COLLECTION_FEATURES = "stock_features_realtime"
    
# Cần import logger để sử dụng trong hàm process_data
try:
    from src.utils.logger import app_logger
except ImportError:
    class MockLogger:
        def info(self, msg): print(f"[INFO] {msg}")
        def error(self, msg): print(f"[ERROR] {msg}")
    app_logger = MockLogger()


def create_raw_schema():
    """Định nghĩa schema cho dữ liệu JSON thô từ Kafka."""
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("currency", StringType(), True),
    ])

def process_data():
    """Luồng xử lý chính: đọc từ Kafka, tạo feature, ghi ra MongoDB."""
    spark = get_spark_session()
    raw_schema = create_raw_schema()

    # 1️⃣ Đọc dữ liệu từ Kafka (Streaming)
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_SERVERS) \
        .option("subscribe", KAFKA_RAW_DATA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Giải mã Value (chuỗi JSON) và thêm các trường metadata
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), raw_schema).alias("data"),
        col("timestamp").alias("kafka_ingestion_time")
    ).select(
        "data.*",
        col("kafka_ingestion_time"),
        current_timestamp().alias("processed_timestamp")
    ).withColumn(
        "ingestion_date", current_date()
    ).withColumn(
        "ingestion_hour", hour(col("processed_timestamp"))
    ).withColumn(
        "ingestion_minute", minute(col("processed_timestamp"))
    )

    app_logger.info(f"Reading from Kafka topic: {KAFKA_RAW_DATA_TOPIC} from broker: {KAFKA_BROKER_SERVERS}")
    
    # 2️⃣ Tạo Feature: Trung bình trượt (Moving Average 10s)
    # Sử dụng Watermark 1 phút để xử lý các dữ liệu đến trễ
    df_features = df_parsed.withWatermark("processed_timestamp", "1 minute") \
        .groupBy(
            col("symbol"),
            # Cửa sổ trượt 10 giây, trượt mỗi 5 giây
            window(col("processed_timestamp"), "10 seconds", "5 seconds")
        ) \
        .agg(
            round(avg("price"), 2).alias("avg_price_10s"),
            round(avg("volume"), 0).alias("avg_volume_10s")
        ) \
        .select(
            col("symbol"),
            col("window.end").alias("window_timestamp"), 
            col("avg_price_10s"),
            col("avg_volume_10s")
        )

    # 3️⃣ Chuẩn bị dữ liệu cho MongoDB
    # Tạo trường _id duy nhất bằng cách kết hợp symbol và window_timestamp
    # Điều này cần thiết cho outputMode("update") và đảm bảo tính idempotency
    df_mongo_ready = df_features.withColumn(
        "_id", 
        concat_ws("-", col("symbol"), col("window_timestamp"))
    )

    app_logger.info(f"Ready to write features to MongoDB: {MONGO_DATABASE}.{MONGO_COLLECTION_FEATURES} at {MONGO_URI}")

    # 4️⃣ Viết ra MongoDB Sink (Streaming)
    query = df_mongo_ready \
        .writeStream \
        .format("mongodb") \
        .option("uri", MONGO_URI) \
        .option("database", MONGO_DATABASE) \
        .option("collection", MONGO_COLLECTION_FEATURES) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("update") \
        .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    try:
        process_data()
    except Exception as e:
        app_logger.error(f"Spark streaming failed: {e}")
    finally:
        try:
            spark = SparkSession.builder.getOrCreate()
            spark.stop()
            app_logger.info("Spark session stopped.")
        except Exception:
            pass