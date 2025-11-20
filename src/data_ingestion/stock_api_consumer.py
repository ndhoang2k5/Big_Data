# src/data_ingestion/stock_api_consumer.py
import time
import random
import datetime
import requests
from src.data_ingestion import config
from src.kafka_processing.kafka_producer import create_producer

# Khởi tạo Producer
producer = create_producer(config.KAFKA_BOOTSTRAP_SERVERS)

def generate_mock_data(symbol):
    """
    Hàm giả lập dữ liệu chứng khoán biến động để test hệ thống
    """
    price_base = 10000
    # Tạo dao động giá ngẫu nhiên +/- 500 đồng
    price = price_base + random.randint(-500, 500)
    return {
        "symbol": symbol,
        "price": price,
        "volume": random.randint(100, 5000),
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "mock_generator"
    }

def get_real_api_data(symbol):
    """
    Hàm lấy dữ liệu thật (Uncomment và sửa khi có API Key)
    """
    try:
        pass
    except Exception as e:
        print(f"Error fetching API: {e}")
    return None

def run():
    print(f"🚀 Bắt đầu đẩy dữ liệu chứng khoán vào topic: {config.KAFKA_TOPIC}")
    print(f"📡 Tần suất: {config.TICK_INTERVAL}s/request")

    try:
        while True:
            for symbol in config.SYMBOLS:
                stock_data = generate_mock_data(symbol)
                if stock_data:
                    # Gửi dữ liệu vào Kafka
                    producer.send(config.KAFKA_TOPIC, value=stock_data)
                    # In ra log để debug
                    print(f"Sent {symbol}: {stock_data['price']} - Vol: {stock_data['volume']}")

            producer.flush()
            time.sleep(config.TICK_INTERVAL)

    except KeyboardInterrupt:
        print("Đang dừng hệ thống...")
        producer.close()

if __name__ == "__main__":
    run()