# src/data_ingestion/stock_api_consumer.py
import time
import random
import datetime
import json
import os
import sys
import traceback
sys.path.append('/app')

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- IMPORT MODULE REAL SOURCE (Xử lý lỗi nếu không có file) ---
get_real_data = None
try:
    from src.data_ingestion.real_source import get_real_data
    print("Đã load thành công module REAL SOURCE", flush=True)
except Exception as e:
    print(f"Không import được module real_source. Lỗi: {e}", flush=True)
    print("Hệ thống sẽ chạy chế độ MOCK (Giả lập).", flush=True)

# --- CẤU HÌNH ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'stock_data')
TICK_INTERVAL = float(os.getenv('TICK_INTERVAL', '1.0')) 
DATA_MODE = os.getenv('DATA_MODE', 'MOCK').upper()

SYMBOLS = ["HPG", "VIC", "VNM", "FPT", "TCB"]
market_state = {}

# --- HÀM KẾT NỐI KAFKA ---
def get_producer():
    print("🔌 Đang kết nối tới Kafka...", flush=True)
    producer = None
    while not producer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                acks=1
            )
            print(f"✅ Kafka Connected: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
        except NoBrokersAvailable:
            print("⚠️ Kafka chưa sẵn sàng. Thử lại sau 5s...", flush=True)
            time.sleep(5)
        except Exception as e:
            print(f"❌ Lỗi khởi tạo Producer: {e}", flush=True)
            time.sleep(5)
    return producer

# --- LOGIC GIẢ LẬP (MOCK) ---
def init_market_state():
    """Khởi tạo giá khởi điểm hợp lý"""
    # Giá tham khảo (có thể chỉnh sửa)
    base_prices = {"HPG": 28000, "VIC": 42000, "VNM": 65000, "FPT": 130000, "TCB": 24000}
    for sym in SYMBOLS:
        market_state[sym] = {
            "current_price": float(base_prices.get(sym, 20000)),
            "trend": 0.0 
        }

def generate_mock_data(symbol):
    """Giả lập dữ liệu theo mô hình Random Walk mượt mà"""
    if symbol not in market_state:
        init_market_state()
        
    state = market_state[symbol]
    
    # Logic biến động
    shock = random.gauss(mu=state["trend"], sigma=0.0005) 
    state["trend"] = state["trend"] * 0.96 + shock * 0.05
    
    new_price = state["current_price"] * (1 + shock)

    new_price = round(new_price / 50) * 50
    if new_price < 1000: new_price = 1000
    state["current_price"] = new_price

    base_vol = 5000
    vol_spike = int(abs(shock) * 5000000) 
    volume = base_vol + vol_spike + random.randint(-1000, 1000)

    return {
        "symbol": symbol,
        "price": round(new_price, 0),
        "volume": abs(volume),
        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "SMART_MOCK"
    }

def run():
    print(f"Bắt đầu Data Ingestion. Chế độ: {DATA_MODE}", flush=True)
    producer = get_producer()
    init_market_state()

    print("Bắt đầu vòng lặp gửi dữ liệu...", flush=True)
    
    while True:
        try:
            for symbol in SYMBOLS:
                stock_data = None

                # --- CƠ CHẾ SWITCH ---
                if DATA_MODE == 'REAL' and get_real_data:
                    # 1. Thử lấy dữ liệu thật
                    stock_data = get_real_data(symbol)
                    
                    # 2. Fallback: Nếu API lỗi hoặc trả về None
                    if stock_data is None:
                        stock_data = generate_mock_data(symbol)
                else:
                    stock_data = generate_mock_data(symbol)

                # --- GỬI DỮ LIỆU ---
                if stock_data:
                    producer.send(KAFKA_TOPIC, value=stock_data)
                    
                    # Log hiển thị
                    tag = "REAL" if stock_data.get('source') == 'SSI_API' else "MOCK"
                    print(f"[{tag}] {symbol}: {stock_data['price']:,.0f}", flush=True)

                # Nếu chạy REAL thì nghỉ xíu để đỡ spam API
                if DATA_MODE == 'REAL':
                    time.sleep(0.5)
            
            producer.flush()
            
            # Điều chỉnh thời gian nghỉ
            sleep_time = 5.0 if DATA_MODE == 'REAL' else TICK_INTERVAL
            time.sleep(sleep_time)
            
        except Exception as e:
            print(f"Lỗi vòng lặp chính: {e}", flush=True)
            time.sleep(5)
            try:
                producer = get_producer()
            except:
                pass

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Đã dừng thủ công.")