# stock_prediction_app/src/feature_engineering/config.py

KAFKA_BROKER_SERVERS = "kafka:9093" 
KAFKA_RAW_DATA_TOPIC = "stock_raw_data"

BATCH_INTERVAL_SECONDS = 15

CHECKPOINT_LOCATION = "/tmp/spark/checkpoint/stock_features"
SPARK_MASTER_URL = "spark://spark-master:7077"
MONGO_URI = "mongodb://mongo:27017" 
MONGO_DATABASE = "stock_data_db"
MONGO_COLLECTION_FEATURES = "stock_features_realtime"