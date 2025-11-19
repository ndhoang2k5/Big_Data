# stock_prediction_app/src/feature_engineering/config.py

# Kafka Configuration (Spark dùng tên service nội bộ)
KAFKA_BROKER = "kafka:9093" 
KAFKA_TOPIC = "stock_raw_data"

# MongoDB Configuration (Spark dùng tên service nội bộ)
# 🚨 CHẮC CHẮN DÙNG TÊN SERVICE "mongo"
MONGO_URI = "mongodb://mongo:27017" 
MONGO_DATABASE = "stock_data_db"
MONGO_COLLECTION = "stock_features_realtime"
# ...