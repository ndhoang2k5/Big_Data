# stock_price_prediction/src/deep_learning_models/model_trainer.py
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from src.deep_learning_models.lstm_model import build_lstm_model, train_lstm_model
from src.utils.logger import app_logger
# from src.utils.helper import load_data_from_db # Bạn cần bổ sung hàm này để tải features

# Giả lập hàm tải data (thay thế cho load_data_from_db)
def mock_load_data_from_db(symbol, time_window=1000):
    """Tạo dữ liệu giả lập để huấn luyện."""
    app_logger.info(f"Mock loading {time_window} rows of data for {symbol}")
    # Giả định features là giá và volume
    prices = np.sin(np.linspace(0, 100, time_window)) * 10 + 50 # Giá theo hình sin
    volumes = np.random.randint(5000, 15000, time_window) # Volume ngẫu nhiên
    data = np.stack([prices, volumes], axis=1) # Shape: (time_window, 2)
    return data

def create_sequences(data, time_steps):
    """Chuyển đổi dữ liệu chuỗi thời gian thành các cặp (X, y) cho LSTM."""
    X, y = [], []
    for i in range(len(data) - time_steps):
        # X: 10 bước thời gian trước
        X.append(data[i:(i + time_steps)])
        # y: giá trị tiếp theo (cột 0 là giá)
        y.append(data[i + time_steps, 0]) 
    return np.array(X), np.array(y)

def run_training_pipeline(symbol="HPG", time_steps=10):
    app_logger.info(f"--- Starting Training Pipeline for {symbol} ---")
    
    # 1. Tải và tiền xử lý dữ liệu
    raw_data = mock_load_data_from_db(symbol)
    
    # Chuẩn hóa (Scaling)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(raw_data)
    
    # Tạo chuỗi (sequences)
    X, y = create_sequences(scaled_data, time_steps)
    
    # Chia tập train/val
    train_size = int(len(X) * 0.8)
    X_train, X_val = X[:train_size], X[train_size:]
    y_train, y_val = y[:train_size], y[train_size:]
    
    app_logger.info(f"Train/Val shapes: X_train {X_train.shape}, y_train {y_train.shape}")
    
    # 2. Xây dựng và Huấn luyện mô hình
    n_features = X_train.shape[2]
    model = build_lstm_model(input_shape=(time_steps, n_features))
    
    train_lstm_model(model, X_train, y_train, X_val, y_val)
    
    # 3. Lưu mô hình (và scaler)
    model_path = f"models/lstm_model_{symbol}.keras"
    model.save(model_path)
    app_logger.info(f"Model saved to {model_path}")

if __name__ == '__main__':
    run_training_pipeline()