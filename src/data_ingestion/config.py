# --- START OF FILE config.py ---
import os

KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
# SỬA: Đổi tên topic trùng khớp với docker-compose và spark
KAFKA_TOPIC = 'stock_data' 

STOCK_API_URL = "https://api.example.com/v1/market/ticks"
API_KEY = os.getenv("STOCK_API_KEY", "your_api_key_here")

SYMBOLS = ["HPG", "VIC", "VNM", "FPT", "TCB"]
TICK_INTERVAL = 0.5