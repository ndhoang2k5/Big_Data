# stock_price_prediction/src/data_ingestion/config.py

# Kafka Configuration
# CHỈNH SỬA: SỬ DỤNG TÊN SERVICE CỦA KAFKA TRONG DOCKER COMPOSE NETWORK.
# Mặc định là 'kafka' và port là 9092.
KAFKA_BROKER = "localhost:9092" 
KAFKA_TOPIC = "stock_raw_data"
MESSAGE_RATE_PER_SECOND = 500

# Simulated Stock API Configuration
STOCK_SYMBOLS = ["VCB", "HPG", "FPT", "VIC", "VNM", "MWG", "PNJ", "SSI", "TPB", "POW"]