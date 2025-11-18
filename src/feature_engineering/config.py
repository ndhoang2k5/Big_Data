# Kafka Configuration
KAFKA_BROKER_SERVERS = "kafka:9092"
KAFKA_RAW_DATA_TOPIC = "stock_raw_data"
KAFKA_FEATURES_TOPIC = "stock_features_data"

# Spark Configuration
SPARK_APP_NAME = "StockFeatureEngineering"

# Batch interval cho Spark Streaming (giây)
BATCH_INTERVAL_SECONDS = 5

# Đường dẫn lưu checkpoint của Spark Structured Streaming
CHECKPOINT_LOCATION = "/tmp/spark_checkpoint"
