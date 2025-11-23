import numpy as np
import pandas as pd
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import MinMaxScaler
from src.deep_learning_models import config

class StockPredictor:
    def __init__(self):
        # Khởi tạo mô hình Deep Learning (Multi-layer Perceptron)
        self.model = MLPRegressor(
            hidden_layer_sizes=config.HIDDEN_LAYERS,
            activation='relu',
            solver='adam',
            learning_rate_init=config.LEARNING_RATE,
            max_iter=config.MAX_ITER,
            random_state=42
        )
        self.scaler_X = MinMaxScaler()
        self.scaler_y = MinMaxScaler()
        self.is_trained = False

    def prepare_data(self, df):
        """
        Chuyển đổi DataFrame thành dạng X (Time Index) và y (Price)
        """
        # Tạo index thời gian ảo: 0, 1, 2, ...
        df['time_idx'] = np.arange(len(df)).reshape(-1, 1)
        
        X = df[['time_idx']].values
        y = df['close_price'].values.reshape(-1, 1)
        
        return X, y

    def train(self, df):
        """Huấn luyện mô hình"""
        X, y = self.prepare_data(df)
        
        # Scale dữ liệu về khoảng [0, 1] giúp mô hình học nhanh hơn
        X_scaled = self.scaler_X.fit_transform(X)
        y_scaled = self.scaler_y.fit_transform(y)
        
        # Train model
        try:
            self.model.fit(X_scaled, y_scaled.ravel())
            self.is_trained = True
            return True
        except Exception as e:
            print(f"Train error: {e}")
            return False

    def predict_future(self, df):
        """Dự đoán n bước tương lai"""
        if not self.is_trained:
            return []

        # Lấy chỉ số thời gian cuối cùng
        last_idx = len(df) - 1
        
        # Tạo mảng các bước thời gian tương lai
        future_indices = np.arange(last_idx + 1, last_idx + 1 + config.PREDICT_STEPS).reshape(-1, 1)
        
        # Scale input tương lai
        future_scaled = self.scaler_X.transform(future_indices)
        
        # Predict
        predictions_scaled = self.model.predict(future_scaled)
        
        # Inverse Scale để ra giá thật (VNĐ)
        predictions = self.scaler_y.inverse_transform(predictions_scaled.reshape(-1, 1)).ravel()
        
        return predictions