# stock_price_prediction/src/feature_engineering/config.py

# Kafka Configuration
# SỬ DỤNG kafka:9092 vì PySpark Processor chạy bên trong container Docker
KAFKA_BROKER_SERVERS = "kafka:9092"
KAFKA_RAW_DATA_TOPIC = "stock_raw_data"
KAFKA_FEATURES_TOPIC = "stock_features_data"

# Spark Configuration
SPARK_APP_NAME = "StockFeatureEngineering"

# Batch interval cho Spark Streaming (giây)
BATCH_INTERVAL_SECONDS = 5

# Đường dẫn lưu checkpoint của Spark Structured Streaming
# Phải là đường dẫn bên trong container
CHECKPOINT_LOCATION = "/tmp/spark_checkpoint"