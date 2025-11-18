# stock_price_prediction/src/data_ingestion/config.py

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"  # Port 9093 là port chúng ta expose ra ngoài từ docker-compose
KAFKA_TOPIC = "stock_raw_data"
MESSAGE_RATE_PER_SECOND = 500 # Giả lập 500 message/giây

# Simulated Stock API Configuration
STOCK_SYMBOLS = ["VCB", "HPG", "FPT", "VIC", "VNM", "MWG", "PNJ", "SSI", "TPB", "POW"]