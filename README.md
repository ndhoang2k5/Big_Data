# 📈 Real-time Stock Prediction System (Hệ thống Dự đoán Giá Cổ phiếu Thời gian thực)

Dự án xây dựng một **Big Data Pipeline** khép kín (End-to-End) để thu thập, xử lý và dự đoán xu hướng giá cổ phiếu Việt Nam theo thời gian thực. Hệ thống sử dụng kiến trúc Microservices, kết hợp giữa xử lý luồng (Stream Processing) và Trí tuệ nhân tạo (Deep Learning).

![Mô hình dự án](images/architecture_diagram.png) 
*(Thay ảnh kiến trúc hệ thống của bạn vào đây)*

## 🚀 Tính năng chính

*   **Data Ingestion:** Giả lập dữ liệu thị trường (Market Data) với mô hình *Random Walk* có xu hướng và biến động (Smart Mock), thay thế cho API thực tế để đảm bảo tính ổn định khi demo.
*   **Stream Processing:** Sử dụng **Apache Spark Structured Streaming** để tính toán các chỉ số kỹ thuật (MA10, Volatility) theo thời gian thực với cơ chế Windowing và Watermarking.
*   **Message Queue:** Sử dụng **Apache Kafka** để vận chuyển dữ liệu với độ trễ thấp và khả năng chịu lỗi cao.
*   **AI Prediction:** Mô hình **Deep Learning (MLPRegressor)** tự động học (Online Learning) từ dữ liệu quá khứ và đưa ra dự báo xu hướng cho 10 bước thời gian tiếp theo.
*   **Real-time Dashboard:** Giao diện **Streamlit** tương tác, hiển thị biểu đồ giá, đường dự báo của AI và độ chính xác của mô hình theo thời gian thực.

## 🛠 Công nghệ sử dụng

| Thành phần | Công nghệ |
| :--- | :--- |
| **Containerization** | Docker, Docker Compose |
| **Message Broker** | Apache Kafka, Zookeeper |
| **Processing Engine** | Apache Spark (PySpark) |
| **Database** | MongoDB (NoSQL) |
| **AI/ML Core** | Scikit-learn (Neural Network), NumPy, Pandas |
| **Visualization** | Streamlit, Plotly |

## 📂 Cấu trúc dự án

```text
FINAL_PROJECT/
├── checkpoint/                 # Thư mục lưu trạng thái của Spark (Tự động sinh ra)
├── src/
│   ├── dashboard/              # Code giao diện Streamlit
│   │   └── app.py
│   ├── data_ingestion/         # Code sinh dữ liệu giả lập (Producer)
│   │   └── stock_api_consumer.py
│   ├── deep_learning_models/   # Code mô hình AI (Training & Inference)
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── model_trainer.py
│   │   └── model_inference.py
│   └── feature_engineering/    # Code xử lý Spark Streaming
│       └── pyspark_processor.py
├── docker-compose.yml          # File cấu hình toàn bộ hệ thống
├── Dockerfile.dashboard        # File build image cho Dashboard
├── Dockerfile.data-ingestion   # File build image cho Producer
├── Dockerfile.predictor        # File build image cho AI Model
└── README.md                   # Hướng dẫn sử dụng
'''


# 📈 Real-time Stock Prediction System (Hệ thống Dự đoán Giá Cổ phiếu Thời gian thực)

Dự án xây dựng một **Big Data Pipeline** khép kín (End-to-End) để thu thập, xử lý và dự đoán xu hướng giá cổ phiếu Việt Nam theo thời gian thực. Hệ thống sử dụng kiến trúc Microservices, kết hợp giữa xử lý luồng (Stream Processing) và Trí tuệ nhân tạo (Deep Learning).

![Mô hình dự án](images/architecture_diagram.png) 
*(Thay ảnh kiến trúc hệ thống của bạn vào đây)*

## 🚀 Tính năng chính

*   **Data Ingestion:** Giả lập dữ liệu thị trường (Market Data) với mô hình *Random Walk* có xu hướng và biến động (Smart Mock), thay thế cho API thực tế để đảm bảo tính ổn định khi demo.
*   **Stream Processing:** Sử dụng **Apache Spark Structured Streaming** để tính toán các chỉ số kỹ thuật (MA10, Volatility) theo thời gian thực với cơ chế Windowing và Watermarking.
*   **Message Queue:** Sử dụng **Apache Kafka** để vận chuyển dữ liệu với độ trễ thấp và khả năng chịu lỗi cao.
*   **AI Prediction:** Mô hình **Deep Learning (MLPRegressor)** tự động học (Online Learning) từ dữ liệu quá khứ và đưa ra dự báo xu hướng cho 10 bước thời gian tiếp theo.
*   **Real-time Dashboard:** Giao diện **Streamlit** tương tác, hiển thị biểu đồ giá, đường dự báo của AI và độ chính xác của mô hình theo thời gian thực.

## 🛠 Công nghệ sử dụng

| Thành phần | Công nghệ |
| :--- | :--- |
| **Containerization** | Docker, Docker Compose |
| **Message Broker** | Apache Kafka, Zookeeper |
| **Processing Engine** | Apache Spark (PySpark) |
| **Database** | MongoDB (NoSQL) |
| **AI/ML Core** | Scikit-learn (Neural Network), NumPy, Pandas |
| **Visualization** | Streamlit, Plotly |

## 📂 Cấu trúc dự án

```text
FINAL_PROJECT/
├── checkpoint/                 # Thư mục lưu trạng thái của Spark (Tự động sinh ra)
├── src/
│   ├── dashboard/              # Code giao diện Streamlit
│   │   └── app.py
│   ├── data_ingestion/         # Code sinh dữ liệu giả lập (Producer)
│   │   └── stock_api_consumer.py
│   ├── deep_learning_models/   # Code mô hình AI (Training & Inference)
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── model_trainer.py
│   │   └── model_inference.py
│   └── feature_engineering/    # Code xử lý Spark Streaming
│       └── pyspark_processor.py
├── docker-compose.yml          # File cấu hình toàn bộ hệ thống
├── Dockerfile.dashboard        # File build image cho Dashboard
├── Dockerfile.data-ingestion   # File build image cho Producer
├── Dockerfile.predictor        # File build image cho AI Model
└── README.md                   # Hướng dẫn sử dụng
```

readme_content = """# ⚙️ Hướng dẫn Cài đặt và Chạy

## 1. Yêu cầu hệ thống (Prerequisites)

  * Đã cài đặt **Docker Desktop** và **Docker Compose**.
  * **RAM:** Tối thiểu 4GB (Khuyến nghị 8GB trở lên để chạy mượt Kafka và Spark).

## 2. Các bước chạy (Step-by-step)

### Bước 1: Khởi động hệ thống

Mở Terminal (hoặc CMD/Powershell) tại thư mục gốc của dự án và chạy lệnh sau:

```bash  
docker-compose up -d --build
```

Lệnh này sẽ tự động tải image, build code và khởi chạy 6 containers dưới nền.

### Bước 2: Chờ khởi tạo

Hệ thống cần khoảng 30-60 giây để các dịch vụ như Kafka, Zookeeper và Spark khởi động hoàn tất và kết nối với nhau.

### Bước 3: Truy cập Dashboard

Mở trình duyệt web và truy cập địa chỉ:
👉 **http://localhost:8051**

# 📊 Hướng dẫn đọc Dashboard

Giao diện được chia thành 3 biểu đồ chính để hỗ trợ ra quyết định:

### Diễn biến thị trường (Real-time Flow):

  * **Đường xanh:** Giá khớp lệnh thực tế.
  * **Đường chấm vàng:** Đường trung bình động MA10 (Xu hướng ngắn hạn).
  * **Thao tác:** Kéo thanh trượt bên dưới để xem lại lịch sử giá quá khứ.

### Xu hướng tương lai (AI Forecast):

  * **Đường tím (AI Predict):** Giá dự báo cho 10 bước thời gian tiếp theo.
  * Nếu đường tím hướng lên ↗️: Dự báo Tăng.
  * Nếu đường tím hướng xuống ↘️: Dự báo Giảm.

### Độ ổn định mô hình (Accuracy Tracking):

  * Hiển thị sai số tuyệt đối (Absolute Error) giữa giá thực và giá dự đoán.
  * Đường càng thấp (sát trục 0) chứng tỏ mô hình đang dự đoán chính xác.

# 🐛 Xử lý lỗi thường gặp (Troubleshooting)

### 1. Lỗi Spark: \"Partition offset was changed...\"

  * **Nguyên nhân:** Do tắt/bật Docker nhiều lần, Kafka bị reset dữ liệu về 0 nhưng Spark vẫn nhớ vị trí đọc cũ (Checkpoint).
  * **Khắc phục:**
    1.  Chạy lệnh: `docker-compose down`
    2.  Xóa thư mục `checkpoint` nằm trong thư mục dự án trên máy của bạn.
    3.  Chạy lại: `docker-compose up -d`

### 2. Dashboard quay mãi không hiện dữ liệu

  * **Nguyên nhân:** Có thể Producer (nguồn dữ liệu) bị lỗi hoặc chưa chạy.
  * **Kiểm tra:** Chạy lệnh `docker-compose logs -f data-ingestion`.
  * **Khắc phục:** Nếu thấy lỗi, chạy `docker-compose restart data-ingestion`.

### 3. Biểu đồ bị giật hoặc mất kết nối

  * Nhấn F5 (Refresh) lại trang trình duyệt. Hệ thống đã được tối ưu để tự động kết nối lại.
"""
