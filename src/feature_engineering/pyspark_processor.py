# stock_price_prediction/src/feature_engineering/pyspark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, window, avg, round, lit, current_date, hour, minute
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
    KAFKA_BROKER_SERVERS = "localhost:9092"
    KAFKA_RAW_DATA_TOPIC = "stock_raw_data"
    KAFKA_FEATURES_TOPIC = "stock_features_data"
    BATCH_INTERVAL_SECONDS = 5
    CHECKPOINT_LOCATION = "D:/bigdata/FINAL/tmp_checkpoint"

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
        StructField("currency", StringType(), True)
    ])

def process_stream_data(spark: SparkSession):
    raw_data_schema = create_raw_schema()

    # 1️⃣ Đọc dữ liệu từ Kafka
    df_raw_stream = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER_SERVERS)
            .option("subscribe", KAFKA_RAW_DATA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
    )
    print(f"[INFO] Reading from Kafka topic: {KAFKA_RAW_DATA_TOPIC}")

    # 2️⃣ Parse JSON
    df_parsed = df_raw_stream \
        .selectExpr("CAST(value AS STRING) as json_data", "timestamp as kafka_ingestion_time") \
        .select(from_json(col("json_data"), raw_data_schema).alias("data"), col("kafka_ingestion_time")) \
        .select("data.*", col("kafka_ingestion_time")) \
        .withColumn("processed_timestamp", col("timestamp").cast(TimestampType())) \
        .withColumn("ingestion_date", current_date()) \
        .withColumn("ingestion_hour", hour(current_timestamp())) \
        .withColumn("ingestion_minute", minute(current_timestamp()))

    df_parsed.printSchema()

    # 3️⃣ Tạo features đơn giản
    df_features = df_parsed \
        .withWatermark("processed_timestamp", "1 minute") \
        .groupBy(window(col("processed_timestamp"), "10 seconds", "5 seconds"), col("symbol")) \
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

    df_features = df_features.fillna(0)

    # 4️⃣ Chuyển thành JSON để gửi Kafka (hoặc debug console)
    df_final = df_features.withColumn("value", lit(
        "{" +
        "\"symbol\":\"" + col("symbol") + "\"," +
        "\"window_start_time\":\"" + col("window_start_time").cast(StringType()) + "\"," +
        "\"window_end_time\":\"" + col("window_end_time").cast(StringType()) + "\"," +
        "\"avg_price_10s\":" + col("avg_price_10s").cast(StringType()) + "," +
        "\"avg_volume_10s\":" + col("avg_volume_10s").cast(StringType()) +
        "}"
    )).select(col("symbol").alias("key"), col("value"))

    print(f"[INFO] Ready to write features to Kafka topic: {KAFKA_FEATURES_TOPIC}")

    # 5️⃣ Viết ra console để debug trước
    query = df_final.writeStream \
        .format("console") \
        .outputMode("update") \
        .trigger(processingTime=f"{BATCH_INTERVAL_SECONDS} seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    spark = get_spark_session()
    try:
        process_stream_data(spark)
    except Exception as e:
        print(f"[ERROR] Spark streaming failed: {e}")
    finally:
        spark.stop()
        print("[INFO] Spark session stopped.")
