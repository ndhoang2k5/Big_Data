import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time
import os
import plotly.graph_objects as go
from datetime import datetime
from collections import deque

# --- 1. CẤU HÌNH ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "stock_db"
COLLECTION_REAL = "stock_derived_features"
COLLECTION_PRED = "stock_predictions"

# --- 2. HÀM HỖ TRỢ CHẠY LẠI (AUTO-REFRESH) ---
def rerun_script():
    """Hàm này đảm bảo Streamlit luôn tự refresh dù ở phiên bản nào"""
    try:
        st.rerun() # Cho bản mới (1.27+)
    except AttributeError:
        try:
            st.experimental_rerun() # Cho bản cũ
        except:
            pass # Nếu không rerun được thì thôi

# --- 3. KẾT NỐI DB ---
@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

def fetch_new_data(client, symbol):
    db = client[DB_NAME]
    
    # Lấy 20 điểm mới nhất
    cursor_real = db[COLLECTION_REAL].find({"symbol": symbol}).sort("end_time", -1).limit(20)
    df_new = pd.DataFrame(list(cursor_real))
    
    cursor_pred = db[COLLECTION_PRED].find({"symbol": symbol}).sort("prediction_time", 1)
    df_pred = pd.DataFrame(list(cursor_pred))

    if not df_new.empty and 'end_time' in df_new.columns:
        df_new['timestamp'] = pd.to_datetime(df_new['end_time'])
        df_new = df_new.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
    
    if not df_pred.empty and 'prediction_time' in df_pred.columns:
        df_pred['timestamp'] = pd.to_datetime(df_pred['prediction_time'])
        df_pred = df_pred.sort_values('timestamp')

    return df_new, df_pred

# --- 4. KHỞI TẠO TRẠNG THÁI (SESSION STATE) ---
if 'history_df' not in st.session_state:
    st.session_state.history_df = pd.DataFrame()
if 'accuracy_history' not in st.session_state:
    st.session_state.accuracy_history = deque(maxlen=200)
if 'last_symbol' not in st.session_state:
    st.session_state.last_symbol = None

# --- 5. GIAO DIỆN (UI) ---
st.set_page_config(page_title="AI Stock Monitor", layout="wide", page_icon="📉")
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 1rem;}
    div[data-testid="metric-container"] {
        background-color: #1E1E1E; border: 1px solid #444; padding: 10px; border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.title("📉 AI Deep Learning Market Monitor")

# Sidebar
SYMBOLS = ['HPG', 'VIC', 'VNM', 'FPT', 'TCB']
symbol = st.sidebar.selectbox("Mã Cổ Phiếu", SYMBOLS)

# Logic Reset
if st.session_state.last_symbol != symbol:
    st.session_state.history_df = pd.DataFrame()
    st.session_state.accuracy_history.clear()
    st.session_state.last_symbol = symbol

if st.sidebar.button("Xóa Lịch sử"):
    st.session_state.history_df = pd.DataFrame()
    st.session_state.accuracy_history.clear()
    rerun_script()

# Hiển thị giờ cập nhật để biết dashboard đang sống
st.sidebar.caption(f"Last Update: {datetime.now().strftime('%H:%M:%S')}")

# --- 6. XỬ LÝ DỮ LIỆU ---
client = get_client()
df_new, df_pred = fetch_new_data(client, symbol)

# Cộng dồn lịch sử
if not df_new.empty:
    if st.session_state.history_df.empty:
        st.session_state.history_df = df_new
    else:
        combined = pd.concat([st.session_state.history_df, df_new])
        st.session_state.history_df = combined.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp')
        if len(st.session_state.history_df) > 2000: # Giữ 2000 điểm
            st.session_state.history_df = st.session_state.history_df.tail(2000)

plot_df = st.session_state.history_df

# --- 7. VẼ BIỂU ĐỒ ---
if not plot_df.empty:
    current_price = plot_df.iloc[-1]['close_price']
    
    model_error = 0
    next_price = 0
    if not df_pred.empty:
        next_price = df_pred.iloc[-1]['predicted_price']
        model_error = abs(current_price - df_pred.iloc[0]['predicted_price'])
        st.session_state.accuracy_history.append({"timestamp": datetime.now(), "error": model_error})

    # Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("💰 Giá Hiện Tại", f"{current_price:,.0f} ₫")
    c2.metric("🔮 Dự Báo (Tương lai)", f"{next_price:,.0f} ₫" if next_price > 0 else "N/A")
    c3.metric("📉 Sai Số (Error)", f"{model_error:,.0f} ₫")

    # === CHART 1: REAL-TIME HISTORY ===
    # Tính toán trục Y CỐ ĐỊNH để chống nhảy
    y_min = plot_df['close_price'].min()
    y_max = plot_df['close_price'].max()
    y_padding = (y_max - y_min) * 0.1 if y_max != y_min else 100

    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(
        x=plot_df['timestamp'], y=plot_df['close_price'],
        mode='lines', name='Giá Thật',
        line=dict(color='#00CC96', width=2)
    ))
    fig1.add_trace(go.Scatter(
        x=plot_df['timestamp'], y=plot_df['MA_10s'],
        mode='lines', name='MA10',
        line=dict(color='#FFFF00', width=1, dash='dot')
    ))

    fig1.update_layout(
        title="1. Diễn biến thị trường (Chạy theo thời gian thực)",
        height=400, margin=dict(t=30, b=0), template="plotly_dark",
        # QUAN TRỌNG: Cố định trục Y
        yaxis=dict(range=[y_min - y_padding, y_max + y_padding], fixedrange=True),
        # QUAN TRỌNG: range slider và uirevision giúp giữ vị trí khi refresh
        xaxis=dict(rangeslider=dict(visible=True), type="date"),
        uirevision='constant_value' 
    )
    # KEY CỐ ĐỊNH: Không bị lỗi duplicate key vì script chạy lại từ đầu
    st.plotly_chart(fig1, use_container_width=True, key="chart_history_fixed")

    # === CHART 2: PREDICTION ===
    if not df_pred.empty:
        fig2 = go.Figure()
        # Nối điểm
        fig2.add_trace(go.Scatter(
            x=[plot_df.iloc[-1]['timestamp'], df_pred.iloc[0]['timestamp']],
            y=[plot_df.iloc[-1]['close_price'], df_pred.iloc[0]['predicted_price']],
            mode='lines', showlegend=False, line=dict(color='#AB63FA', width=2, dash='dot')
        ))
        fig2.add_trace(go.Scatter(
            x=df_pred['timestamp'], y=df_pred['predicted_price'],
            mode='lines+markers', name='AI Dự báo', line=dict(color='#AB63FA', width=3)
        ))
        
        # Tính range cho Chart 2 dựa trên Chart 1 để đồng bộ
        pred_min = min(y_min, df_pred['predicted_price'].min())
        pred_max = max(y_max, df_pred['predicted_price'].max())
        p_pad = (pred_max - pred_min) * 0.1 if pred_max != pred_min else 100

        fig2.update_layout(
            title="2. Xu hướng tương lai", height=300, margin=dict(t=30, b=0), template="plotly_dark",
            yaxis=dict(range=[pred_min - p_pad, pred_max + p_pad], fixedrange=True),
            uirevision='constant_value'
        )
        st.plotly_chart(fig2, use_container_width=True, key="chart_prediction_fixed")

    # === CHART 3: ACCURACY ===
    if len(st.session_state.accuracy_history) > 1:
        df_acc = pd.DataFrame(st.session_state.accuracy_history)
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=df_acc.index, y=df_acc['error'],
            mode='lines', name='Sai số', fill='tozeroy', line=dict(color='#EF553B')
        ))
        fig3.update_layout(
            title="3. Độ ổn định mô hình", height=250, margin=dict(t=30, b=0), template="plotly_dark",
            yaxis_title="Độ lệch (VND)",
            uirevision='constant_value'
        )
        st.plotly_chart(fig3, use_container_width=True, key="chart_accuracy_fixed")

else:
    st.info(f"⏳ Đang kết nối và tải dữ liệu cho mã {symbol}...")

# --- 8. TỰ ĐỘNG CHẠY LẠI (LOOP) ---
time.sleep(1) # Đợi 1 giây
rerun_script() # Buộc dashboard chạy lại