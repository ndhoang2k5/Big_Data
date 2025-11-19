# stock_price_prediction/src/data_ingestion/config.py

# Kafka Configuration
# SỬ DỤNG CỔNG BÊN NGOÀI (HOST) CHO PRODUCER (docker-compose ánh xạ 9092:9092)
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "stock_raw_data"
MESSAGE_RATE_PER_SECOND = 200

STOCK_SYMBOLS = ["VCB", "HPG", "FPT", "VIC", "VNM", "MWG", "PNJ", "SSI", "TPB", "POW"]