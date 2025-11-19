# stock_price_prediction/src/data_ingestion/stock_api_consumer.py
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer
# Import cấu hình từ file config.py
from src.data_ingestion.config import KAFKA_BROKER, KAFKA_TOPIC, STOCK_SYMBOLS, MESSAGE_RATE_PER_SECOND
from src.utils.logger import app_logger

class StockAPIConsumer:
    """
    Giả lập lấy dữ liệu từ một Stock API và đẩy vào Kafka.
    """
    def __init__(self, broker=KAFKA_BROKER, topic=KAFKA_TOPIC):
        self.broker = broker
        self.topic = topic
        self.producer = self._create_kafka_producer()
        app_logger.info(f"Initialized Kafka Producer for topic: {self.topic} on broker: {self.broker}")

    def _create_kafka_producer(self):
        """
        Khởi tạo và trả về một Kafka Producer.
        """
        # Cấu hình Producer để tăng hiệu suất và độ tin cậy trong môi trường Docker
        config = {
            'bootstrap.servers': self.broker,
            'acks': '0', # Không đợi xác nhận từ broker (tăng throughput)
            'compression.type': 'lz4', # Nén dữ liệu
            # ✅ CẢI TIẾN STABILITY: Timeout cho socket (10 giây) để xử lý các vấn đề mạng tạm thời
            'socket.timeout.ms': 10000 
        }
        return Producer(config)

    def _delivery_report(self, err, msg):
        """Callback được gọi sau mỗi tin nhắn được gửi (hoặc lỗi)."""
        if err is not None:
            app_logger.error(f"Message delivery failed for {msg.key().decode('utf-8')}: {err}")
        # Không cần log thành công để tránh làm đầy log

    def _simulate_stock_data(self):
        """Giả lập tạo dữ liệu chứng khoán."""
        symbol = random.choice(STOCK_SYMBOLS)
        
        # Giả lập biến động giá
        current_price = random.uniform(10.0, 100.0)
        
        # Giả lập dữ liệu OHLCV (Open, High, Low, Close, Volume)
        open_price = current_price * random.uniform(0.99, 1.01)
        high_price = max(open_price, current_price * random.uniform(1.00, 1.02))
        low_price = min(open_price, current_price * random.uniform(0.98, 1.00))
        close_price = current_price * random.uniform(0.99, 1.01)

        data = {
            "symbol": symbol,
            "price": round(current_price, 2),
            "volume": random.randint(100, 10000),
            "timestamp": datetime.now().isoformat(),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "currency": "VND" 
        }
        return data

    def start_ingestion(self):
        """Bắt đầu quá trình giả lập và đẩy dữ liệu vào Kafka."""
        app_logger.info(f"Starting simulated data ingestion at {MESSAGE_RATE_PER_SECOND} msg/s...")
        
        start_time = time.time()
        messages_sent = 0

        try:
            while True:
                # 1. Giả lập dữ liệu
                data = self._simulate_stock_data()
                message = json.dumps(data)
                
                # 2. Đẩy vào Kafka
                key = data["symbol"].encode('utf-8')

                self.producer.produce(self.topic, key=key, value=message.encode('utf-8'), callback=self._delivery_report)
                messages_sent += 1

                # 3. Đảm bảo producer gửi các tin nhắn đang chờ xử lý.
                # poll(0) giúp kích hoạt các callback báo cáo lỗi/thành công và gửi tin nhắn
                self.producer.poll(0)

                # 4. Giới hạn tốc độ gửi tin nhắn (Tỷ lệ gửi)
                elapsed_time = time.time() - start_time
                expected_messages = elapsed_time * MESSAGE_RATE_PER_SECOND

                if messages_sent > expected_messages:
                    sleep_time = (messages_sent - expected_messages) / MESSAGE_RATE_PER_SECOND
                    # Chỉ ngủ khi cần thiết
                    time.sleep(max(0, sleep_time)) 

                if messages_sent % (MESSAGE_RATE_PER_SECOND * 10) == 0: # Log mỗi 10 giây
                    current_rate = messages_sent / elapsed_time if elapsed_time > 0 else 0
                    app_logger.info(f"Sent {messages_sent} messages. Current rate: {current_rate:.2f} msg/s")

        except KeyboardInterrupt:
            app_logger.info("Ingestion stopped by user.")
        except Exception as e:
            app_logger.error(f"An error occurred during ingestion: {e}")
        finally:
            self.producer.flush() # Đảm bảo tất cả các tin nhắn còn lại được gửi
            app_logger.info("Kafka Producer flushed and closed.")


if __name__ == "__main__":
    try:
        consumer = StockAPIConsumer()
        consumer.start_ingestion()
    except Exception as e:
        app_logger.error(f"Failed to start StockAPIConsumer: {e}")