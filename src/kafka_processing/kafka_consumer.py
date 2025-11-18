# stock_price_prediction/src/kafka_processing/kafka_consumer.py
import json
from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING
from src.kafka_processing.config import KAFKA_BROKER_SERVERS, KAFKA_FEATURES_TOPIC, CONSUMER_GROUP_ID
from src.utils.logger import app_logger

# Giả định hàm lưu vào DB
def save_to_database(feature_data):
    """Giả lập lưu dữ liệu features vào cơ sở dữ liệu."""
    # Trong môi trường thực tế, bạn sẽ dùng thư viện như psycopg2 (PostgreSQL) 
    # hoặc cassandra-driver (Cassandra) tại đây.
    app_logger.info(f" Saving features for {feature_data.get('symbol')} at {feature_data.get('window_end_time')}")
    # print(feature_data) 
    # Ví dụ: INSERT INTO features_table (symbol, avg_price_10s, ...) VALUES (...)

def consume_and_save():
    """Đọc dữ liệu features từ Kafka và lưu vào DB."""
    conf = {
        'bootstrap.servers': KAFKA_BROKER_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'latest', # Bắt đầu từ tin nhắn mới nhất
    }

    consumer = Consumer(conf)
    
    try:
        consumer.subscribe([KAFKA_FEATURES_TOPIC])

        app_logger.info(f"Starting feature consumer for topic: {KAFKA_FEATURES_TOPIC}")

        while True:
            # Poll trong 1 giây để kiểm tra tin nhắn
            msg = consumer.poll(1.0) 

            if msg is None:
                continue
            if msg.error():
                if msg.error().fatal():
                    app_logger.error(f"Fatal consumer error: {msg.error()}")
                    break
                else:
                    app_logger.warning(f"Consumer error: {msg.error()}")
                    continue

            # Xử lý tin nhắn
            try:
                feature_json = msg.value().decode('utf-8')
                feature_data = json.loads(feature_json)
                save_to_database(feature_data)
            except Exception as e:
                app_logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        app_logger.info("Consumer stopped by user.")
    finally:
        consumer.close()
        app_logger.info("Kafka Consumer closed.")

if __name__ == '__main__':
    # Bạn cần đảm bảo file config.py trong kafka_processing có các biến cần thiết
    # KAFKA_BROKER_SERVERS = "localhost:9093"
    # KAFKA_FEATURES_TOPIC = "stock_features_data"
    # CONSUMER_GROUP_ID = "feature-db-saver-group"
    consume_and_save()