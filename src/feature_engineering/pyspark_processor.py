from pyspark.sql import SparkSession
# THÊM: import 'coalesce' và 'lit'
from pyspark.sql.functions import col, from_json, window, avg, stddev, to_timestamp, last, count, coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# --- CẤU HÌNH ---
MONGO_URI = "mongodb://mongodb:27017/stock_db"
KAFKA_BROKERS = "kafka:29092"
KAFKA_TOPIC = "stock_data"

DB_NAME = "stock_db"
COLLECTION_NAME = "stock_derived_features" 
CHECKPOINT_LOCATION = "/app/checkpoint/stock_features_v3"


stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

def process_batch(df, batch_id):
    row_count = df.count()
    if row_count > 0:
        print(f"--- BATCH {batch_id}: Writing {row_count} features to MongoDB ---")
        try:
            (df.write
             .format("mongodb")
             .mode("append")
             .option("connection.uri", MONGO_URI)
             .option("database", DB_NAME)
             .option("collection", COLLECTION_NAME)
             .save())
            print(f"Success: Batch {batch_id} saved.")
        except Exception as e:
            print(f"Error saving batch {batch_id}: {e}")

def run_spark_job():
    print("--- Khởi động Spark với Feature Engineering (Fixed) ---")
    spark = (SparkSession.builder
             .appName("StockFeatureEngineering")
             .config("spark.mongodb.output.uri", f"{MONGO_URI}/{DB_NAME}.{COLLECTION_NAME}")
             .config("spark.sql.shuffle.partitions", "5") 
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc Kafka
    raw_df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BROKERS)
              .option("subscribe", KAFKA_TOPIC)
              .option("startingOffsets", "latest")
              # THÊM DÒNG NÀY: Bỏ qua lỗi lệch Offset, chấp nhận reset
              .option("failOnDataLoss", "false") 
              .load())

    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), stock_schema).alias("data")
    ).select(
        col("data.symbol"),
        col("data.price"),
        col("data.volume"),
        to_timestamp(col("data.timestamp")).alias("event_time")
    )

    feature_df = (parsed_df
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            window(col("event_time"), "10 seconds", "2 seconds"), 
            col("symbol")
        )
        .agg(
            avg("price").alias("MA_10s"),
            stddev("price").alias("volatility_10s"),
            last("price").alias("close_price"),
            count("price").alias("tick_count")
        )
    )

    final_df = feature_df.select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("symbol"),
        col("MA_10s"),
        # SỬA: Dùng coalesce(cột, lit(0)) thay vì .fillna(0)
        coalesce(col("volatility_10s"), lit(0.0)).alias("volatility_10s"), 
        col("close_price"),
        col("tick_count")
    )

    query = (final_df.writeStream
             .foreachBatch(process_batch)
             .outputMode("update") 
             .option("checkpointLocation", CHECKPOINT_LOCATION)
             .trigger(processingTime="2 seconds") 
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()