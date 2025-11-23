# src/kafka_processing/kafka_producer.py
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_producer(bootstrap_servers):
    """
    Khởi tạo Kafka Producer với cơ chế thử lại (retry)
    """
    retries = 0
    while retries < 10:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                # acks=0: Gửi nhanh nhất, không cần chờ Kafka xác nhận (Tốt cho HFT data tốc độ cao)
                # acks=1: Chờ Leader xác nhận (An toàn hơn) -> Chọn 1 cho cân bằng
                acks=1 
            )
            print(f"Đã kết nối thành công tới Kafka tại {bootstrap_servers}")
            return producer
        except NoBrokersAvailable:
            print(f"⚠️ Không tìm thấy Kafka broker. Đang thử lại ({retries+1}/10)...")
            time.sleep(3)
            retries += 1
    
    raise Exception("Không thể kết nối tới Kafka sau nhiều lần thử.")