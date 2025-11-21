import time
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from src.deep_learning_models import config
from src.deep_learning_models.model_trainer import StockPredictor

def get_mongo_client():
    return MongoClient(config.MONGO_URI)

def run_inference():
    print("🤖 AI Inference Service Started...")
    client = get_mongo_client()
    db = client[config.DB_NAME]
    source_col = db[config.SOURCE_COLLECTION]
    target_col = db[config.TARGET_COLLECTION]

    # Mỗi mã chứng khoán sẽ có một mô hình riêng (Optional)
    # Ở đây ta khởi tạo mới mỗi vòng lặp để đơn giản hóa việc cập nhật xu hướng mới nhất (Online Learning)
    
    while True:
        for symbol in config.SYMBOLS:
            try:
                # 1. Lấy dữ liệu lịch sử từ MongoDB
                cursor = source_col.find({"symbol": symbol}).sort("end_time", -1).limit(config.LOOKBACK_WINDOW)
                data = list(cursor)
                
                # Cần ít nhất 20 điểm để train
                if len(data) < 20:
                    continue

                # Xử lý dữ liệu
                df = pd.DataFrame(data)
                if 'end_time' not in df.columns: continue
                
                # Sắp xếp theo thời gian cũ -> mới
                df['timestamp'] = pd.to_datetime(df['end_time'])
                df = df.sort_values('timestamp')

                # 2. Khởi tạo và Train Model
                predictor = StockPredictor()
                success = predictor.train(df)

                if success:
                    # 3. Dự đoán tương lai
                    future_prices = predictor.predict_future(df)

                    # 4. Tạo timestamps cho tương lai (Giả sử mỗi tick cách nhau 2 giây)
                    last_time = df.iloc[-1]['timestamp']
                    future_times = [last_time + timedelta(seconds=2 * i) for i in range(1, config.PREDICT_STEPS + 1)]

                    # 5. Chuẩn bị dữ liệu lưu vào MongoDB
                    predictions = []
                    for t, p in zip(future_times, future_prices):
                        predictions.append({
                            "symbol": symbol,
                            "prediction_time": t,
                            "predicted_price": float(p),
                            "created_at": datetime.utcnow()
                        })

                    # Xóa dự đoán cũ, lưu dự đoán mới
                    target_col.delete_many({"symbol": symbol})
                    target_col.insert_many(predictions)
                    
                    print(f"✅ {symbol}: Predicted {config.PREDICT_STEPS} steps. Next: {future_prices[0]:.0f}")

            except Exception as e:
                print(f"Error predicting {symbol}: {e}")
        
        # Nghỉ trước khi train lại
        time.sleep(config.TRAIN_INTERVAL)

if __name__ == "__main__":
    run_inference()