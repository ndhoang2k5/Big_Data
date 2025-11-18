# stock_price_prediction/src/data_ingestion/config.py

# Kafka Configuration
# SỬ DỤNG localhost:9093 vì Producer chạy trên Host và Kafka expose qua port 9093
KAFKA_BROKER = "localhost:9093"  
KAFKA_TOPIC = "stock_raw_data"
MESSAGE_RATE_PER_SECOND = 250

# Simulated Stock API Configuration
STOCK_SYMBOLS = ["VCB", "HPG", "FPT", "VIC", "VNM", "MWG", "PNJ", "SSI", "TPB", "POW"]