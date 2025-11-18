# stock_price_prediction/src/data_ingestion/stock_api_consumer.py
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer
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
        # Cấu hình để producer không đợi xác nhận từ broker, tăng throughput.
        # Trong môi trường production, bạn có thể muốn cân nhắc độ tin cậy.
        config = {
            'bootstrap.servers': self.broker,
            'acks': '0', # Không đợi xác nhận từ broker
            'compression.type': 'lz4' # Nén dữ liệu để giảm băng thông
        }
        return Producer(config)

    def _delivery_report(self, err, msg):
        """
        Hàm callback để xử lý kết quả gửi tin nhắn.
        """
        if err is not None:
            app_logger.error(f"Message delivery failed: {err}")
        else:
            app_logger.debug(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def _simulate_stock_data(self):
        """
        Giả lập dữ liệu giá cổ phiếu với cấu trúc cơ bản.
        """
        symbol = random.choice(STOCK_SYMBOLS)
        price = round(random.uniform(10.0, 100.0), 2)
        volume = random.randint(1000, 100000)
        timestamp = datetime.now().isoformat()

        data = {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "timestamp": timestamp,
            "open": round(price * random.uniform(0.98, 1.02), 2),
            "high": round(price * random.uniform(1.0, 1.05), 2),
            "low": round(price * random.uniform(0.95, 1.0), 2),
            "close": price, # Giá hiện tại coi như giá đóng cửa tạm thời
            "currency": "VND"
        }
        return data

    def start_ingestion(self):
        """
        Bắt đầu quá trình lấy dữ liệu và đẩy vào Kafka.
        """
        app_logger.info(f"Starting stock data ingestion at {MESSAGE_RATE_PER_SECOND} messages/second...")
        start_time = time.time()
        messages_sent = 0

        try:
            while True:
                data = self._simulate_stock_data()
                message = json.dumps(data)

                # Sử dụng key để đảm bảo các tin nhắn của cùng một mã cổ phiếu
                # sẽ đi vào cùng một partition (đảm bảo thứ tự)
                key = data["symbol"].encode('utf-8')

                self.producer.produce(self.topic, key=key, value=message.encode('utf-8'), callback=self._delivery_report)
                messages_sent += 1

                # Đảm bảo producer gửi các tin nhắn đang chờ xử lý.
                # Có thể điều chỉnh tần suất `poll` để cân bằng giữa độ trễ và throughput.
                self.producer.poll(0)

                # Giới hạn tốc độ gửi tin nhắn
                elapsed_time = time.time() - start_time
                expected_messages = elapsed_time * MESSAGE_RATE_PER_SECOND

                if messages_sent > expected_messages:
                    sleep_time = (messages_sent - expected_messages) / MESSAGE_RATE_PER_SECOND
                    time.sleep(sleep_time)

                if messages_sent % (MESSAGE_RATE_PER_SECOND * 10) == 0: # Log mỗi 10 giây
                    app_logger.info(f"Sent {messages_sent} messages. Current rate: {messages_sent / elapsed_time:.2f} msg/s")

        except KeyboardInterrupt:
            app_logger.info("Ingestion stopped by user.")
        except Exception as e:
            app_logger.error(f"An error occurred during ingestion: {e}")
        finally:
            self.producer.flush() # Đảm bảo tất cả các tin nhắn còn lại được gửi
            app_logger.info("Kafka Producer flushed and closed.")

if __name__ == "__main__":
    consumer = StockAPIConsumer()
    consumer.start_ingestion()