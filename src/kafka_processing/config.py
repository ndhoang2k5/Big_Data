# stock_price_prediction/src/kafka_processing/config.py

# Kafka Broker Server
# SỬ DỤNG localhost:9093 vì Consumer chạy trên Host
KAFKA_BROKER_SERVERS = "localhost:9093" 

# Topic chứa Features Data đã được PySpark xử lý
KAFKA_FEATURES_TOPIC = "stock_features_data"

# ID của nhóm Consumer
CONSUMER_GROUP_ID = "feature-db-saver-group"