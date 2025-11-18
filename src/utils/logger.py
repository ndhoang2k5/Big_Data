# stock_price_prediction/src/utils/logger.py
import logging
import os
from datetime import datetime

def setup_logger(name, log_file='app.log', level=logging.INFO):
    """
    Thiết lập logger cho ứng dụng.
    """
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Tạo thư mục logs nếu chưa tồn tại
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Đặt tên file log theo ngày
    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{log_file}")

    handler = logging.FileHandler(log_filename)
    handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logger.addHandler(console_handler) # In ra console
    return logger

# Logger mặc định cho toàn bộ ứng dụng
app_logger = setup_logger('stock_prediction_app')