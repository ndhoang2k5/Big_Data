from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Cấu hình
MONGO_URI = "mongodb://mongodb:27017" 
KAFKA_BROKERS = "kafka:29092" 
KAFKA_TOPIC = "stock_data"
DB_NAME = "stock_db" 
COLLECTION_NAME = "stock_features" 
CHECKPOINT_LOCATION = "/app/checkpoint/stock_features"

# Schema khớp với Producer
stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

def process_batch(df, batch_id):
    count = df.count()
    if count > 0:
        print(f"--- BATCH {batch_id}: Processing {count} records ---")
        try:
            # Ghi vào MongoDB với cấu hình TƯỜNG MINH
            (df.write
             .format("mongodb")
             .mode("append")
             .option("connection.uri", MONGO_URI) 
             .option("database", DB_NAME)
             .option("collection", COLLECTION_NAME)
             .save())
            print(f"Success: Batch {batch_id} saved to MongoDB.")
        except Exception as e:
            print(f"Error saving batch {batch_id}: {e}")
    else:
        print(f"--- BATCH {batch_id}: Empty ---")

def run_spark_job():
    # Khởi tạo session
    spark = (SparkSession.builder
             .appName("StockProcessor")
             .config("spark.mongodb.output.uri", f"{MONGO_URI}/{DB_NAME}.{COLLECTION_NAME}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1. Đọc Kafka
    df = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BROKERS)
          .option("subscribe", KAFKA_TOPIC)
          .option("startingOffsets", "latest")
          .load())

    # 2. Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), stock_schema).alias("data"),
        col("timestamp").alias("kafka_time")
    ).select("data.*", "kafka_time")

    # 3. Chạy Stream
    query = (parsed_df.writeStream
             .foreachBatch(process_batch)
             .option("checkpointLocation", CHECKPOINT_LOCATION)
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()