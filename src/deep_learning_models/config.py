import os

# MongoDB Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "stock_db"
SOURCE_COLLECTION = "stock_derived_features"  # Dữ liệu đầu vào (Feature từ Spark)
TARGET_COLLECTION = "stock_predictions"       # Dữ liệu đầu ra (Dự đoán)

# Model Config
SYMBOLS = ["HPG", "VIC", "VNM", "FPT", "TCB"]
LOOKBACK_WINDOW = 60  # Nhìn lại 60 điểm dữ liệu quá khứ để học
PREDICT_STEPS = 10    # Dự đoán 10 điểm tương lai
TRAIN_INTERVAL = 5    # Train lại mô hình mỗi 5 giây (Online Learning)

# Deep Learning Config (MLP)
HIDDEN_LAYERS = (100, 50) # 2 lớp ẩn (100 nơ-ron và 50 nơ-ron)
MAX_ITER = 500
LEARNING_RATE = 0.001