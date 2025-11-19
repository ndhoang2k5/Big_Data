# stock_price_prediction/src/kafka_processing/feature_consumer.py
import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from src.utils.logger import app_logger

# Import cấu hình từ data_ingestion/config.py
try:
    from src.data_ingestion.config import KAFKA_BROKER
except ImportError:
    KAFKA_BROKER = "localhost:9093" 

# Topic chứa dữ liệu feature đã được Spark xử lý
KAFKA_FEATURES_TOPIC = "stock_features_data"

def consume_features():
    """
    Khởi tạo Kafka Consumer và lắng nghe dữ liệu feature đã xử lý từ Spark.
    """
    app_logger.info("Initializing Kafka Consumer for processed features...")

    # Cấu hình Consumer
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        # Group ID giúp Kafka biết chúng ta đang ở đâu trong luồng dữ liệu
        'group.id': 'feature_checker_group',
        # Đặt lại offset về đầu nếu không có offset đã commit (cho lần chạy đầu tiên)
        'auto.offset.reset': 'earliest', 
        # Tắt auto commit để kiểm soát vị trí đọc
        'enable.auto.commit': False 
    }

    consumer = Consumer(conf)
    
    try:
        # Đăng ký lắng nghe topic features
        consumer.subscribe([KAFKA_FEATURES_TOPIC])

        app_logger.info(f"Subscribed to topic: {KAFKA_FEATURES_TOPIC}. Press Ctrl+C to stop.")

        while True:
            # Poll (tìm nạp) 1 tin nhắn trong 1.0 giây
            msg = consumer.poll(1.0) 

            if msg is None:
                # app_logger.debug("Waiting for new messages...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Đã đến cuối partition, tiếp tục chờ
                    continue
                else:
                    # Gặp lỗi Kafka khác
                    raise KafkaException(msg.error())
            
            # Giải mã key và value
            key = msg.key().decode('utf-8') if msg.key() else None
            value = msg.value().decode('utf-8')

            # Dữ liệu feature được gửi dưới dạng JSON string
            try:
                feature_data = json.loads(value)
                
                # ✅ Hiển thị đầu ra feature đã được Spark xử lý
                print("--------------------------------------------------")
                print(f"✅ FEATURE RECEIVED (Symbol: {key}):")
                print(f"Window: {feature_data.get('window_start_time')} -> {feature_data.get('window_end_time')}")
                print(f"Avg Price (10s): {feature_data.get('avg_price_10s')}")
                print(f"Avg Volume (10s): {feature_data.get('avg_volume_10s')}")
                print("--------------------------------------------------")

            except json.JSONDecodeError as e:
                app_logger.error(f"Failed to decode JSON value: {value}. Error: {e}")

            # Commit offset để ghi lại vị trí đã đọc thành công
            consumer.commit(message=msg)

    except KeyboardInterrupt:
        app_logger.info("Consumer stopped by user.")
    except Exception as e:
        app_logger.error(f"An unexpected error occurred in consumer: {e}")
    finally:
        # Đóng consumer
        consumer.close()
        app_logger.info("Kafka Consumer closed.")

if __name__ == '__main__':
    # Đảm bảo bạn đã chạy Producer và Spark Processor trước khi chạy Consumer này.
    consume_features()