# stock_price_prediction/src/feature_engineering/pyspark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg, round, lit, current_date, hour, minute, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from src.feature_engineering.spark_utils import get_spark_session

# Import config, nếu chưa có BATCH_INTERVAL_SECONDS hoặc CHECKPOINT_LOCATION thì đặt mặc định
try:
    from src.feature_engineering.config import (
        KAFKA_BROKER_SERVERS,
        KAFKA_RAW_DATA_TOPIC,
        KAFKA_FEATURES_TOPIC,
        BATCH_INTERVAL_SECONDS,
        CHECKPOINT_LOCATION
    )
except ImportError:
    # Giá trị mặc định cho môi trường dev/local nếu config không tồn tại
    KAFKA_BROKER_SERVERS = "kafka:9092"
    KAFKA_RAW_DATA_TOPIC = "stock_raw_data"
    KAFKA_FEATURES_TOPIC = "stock_features_data"
    BATCH_INTERVAL_SECONDS = 5
    CHECKPOINT_LOCATION = "/tmp/spark/checkpoint"

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
    """Luồng xử lý chính: đọc từ Kafka, tạo feature, ghi ra Kafka."""
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

    # Giải mã Value (JSON string) và thêm các trường metadata
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

    print(f"[INFO] Reading from Kafka topic: {KAFKA_RAW_DATA_TOPIC} from broker: {KAFKA_BROKER_SERVERS}")
    print("Schema after parsing:")
    df_parsed.printSchema()

    # 2️⃣ Tạo Feature: Moving Average (trung bình giá và volume trong 10 giây cuối)
    df_features = df_parsed.withWatermark("processed_timestamp", "1 minute") \
        .groupBy(
            col("symbol"),
            window(col("processed_timestamp"), "10 seconds", "5 seconds")
        ) \
        .agg(
            round(avg("price"), 2).alias("avg_price_10s"),
            round(avg("volume"), 0).alias("avg_volume_10s")
        ) \
        .select(
            col("symbol"),
            col("window.start").alias("window_start_time"),
            col("window.end").alias("window_end_time"),
            col("avg_price_10s"),
            col("avg_volume_10s")
        )

    # Xử lý giá trị NULL (nếu không có dữ liệu trong 10s đầu)
    df_features = df_features.fillna(0)

    # 4️⃣ Chuyển thành JSON để gửi Kafka
    # Dùng struct và to_json để đảm bảo cột 'value' là String (bắt buộc cho Kafka Sink)
    # Lấy tất cả các cột trừ 'symbol' làm value, và 'symbol' làm key.
    # Trong trường hợp này, ta gói gọn toàn bộ df_features thành JSON.
    df_final = df_features.select(
        col("symbol").alias("key"),
        to_json(struct(df_features["*"])).alias("value")
    )
    
    print(f"[INFO] Ready to write features to Kafka topic: {KAFKA_FEATURES_TOPIC} on broker: {KAFKA_BROKER_SERVERS}")

    # 5️⃣ Viết ra Kafka Sink
    query = df_final.select(
        col("key").cast(StringType()),  # Đảm bảo key là String
        col("value").cast(StringType()) # Đảm bảo value là String
    ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER_SERVERS) \
        .option("topic", KAFKA_FEATURES_TOPIC) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    try:
        process_data()
    except Exception as e:
        print(f"[ERROR] Spark streaming failed: {e}")
        # Đảm bảo logger được import và sử dụng đúng cách
        try:
            from src.utils.logger import app_logger
            app_logger.error(f"Spark streaming failed: {e}")
        except ImportError:
            pass
    finally:
        # Đảm bảo Spark session dừng (nếu nó được khởi tạo)
        try:
            spark = SparkSession.builder.getOrCreate()
            spark.stop()
            print("[INFO] Spark session stopped.")
        except Exception:
            pass