# src/data_ingestion/stock_api_consumer.py
import time
import random
import datetime
import json
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_data')
TICK_INTERVAL = 1  

SYMBOLS = ["HPG", "VIC", "VNM", "FPT", "TCB"]
market_state = {}

def get_producer():
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks=1
            )
            print(f"✅ Đã kết nối thành công tới Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        except NoBrokersAvailable:
            print("⚠️ Chưa thấy Kafka broker. Đang thử lại sau 5s...")
            time.sleep(5)
    return producer

def init_market_state():
    """Khởi tạo giá khởi điểm hợp lý"""
    base_prices = {"HPG": 28000, "VIC": 44000, "VNM": 67000, "FPT": 96000, "TCB": 33500}
    for sym in SYMBOLS:
        market_state[sym] = {
            "current_price": float(base_prices.get(sym, 20000)),
            "trend": 0.0 # Xu hướng hiện tại
        }

def generate_mock_data(symbol):
    """Giả lập dữ liệu theo mô hình Random Walk mượt mà"""
    if symbol not in market_state:
        init_market_state()
        
    state = market_state[symbol]
    
    # 1. Logic biến động mượt (Smooth Volatility)
    # Trend drifting: Xu hướng thay đổi từ từ, không giật cục
    # Sigma nhỏ (0.0015) để giá đi mềm mại
    shock = random.gauss(mu=state["trend"], sigma=0.0015) 
    state["trend"] = state["trend"] * 0.95 + shock * 0.05
    
    # Tính giá mới
    new_price = state["current_price"] * (1 + shock)
    if new_price < 1000: new_price = 1000
    state["current_price"] = new_price

    # 2. Volume biến động theo biên độ giá
    base_vol = 5000
    # Giá thay đổi càng nhiều, volume càng lớn
    vol_spike = int(abs(shock) * 5000000) 
    volume = base_vol + vol_spike + random.randint(-1000, 1000)

    return {
        "symbol": symbol,
        "price": round(new_price, 1),
        "volume": abs(volume),
        # QUAN TRỌNG: Dùng UTC time chuẩn để Spark Window hoạt động chính xác
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "mock_smooth_v2"
    }

def run():
    print("🚀 Bắt đầu giả lập dữ liệu thị trường (Smart Mock)...")
    producer = get_producer()
    init_market_state()

    while True:
        try:
            for symbol in SYMBOLS:
                stock_data = generate_mock_data(symbol)
                if stock_data:
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    print(f"Sent {symbol}: {stock_data['price']}") # Uncomment nếu muốn debug
            
            producer.flush()
            time.sleep(TICK_INTERVAL)
            
        except Exception as e:
            print(f"Lỗi vòng lặp chính: {e}")
            time.sleep(5)
            # Thử kết nối lại nếu Kafka rớt
            try:
                producer = get_producer()
            except:
                pass

if __name__ == "__main__":
    run()