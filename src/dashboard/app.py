# src/dashboard/app.py
import streamlit as st
import pandas as pd
from pymongo import MongoClient
import time
import os
import plotly.graph_objects as go
import uuid

# --- CẤU HÌNH ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = "stock_db"
COLLECTION_REAL = "stock_derived_features"
COLLECTION_PRED = "stock_predictions"

@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI)

def fetch_data(client, symbol):
    db = client[DB_NAME]
    
    # Lấy Data Thật
    df_real = pd.DataFrame(list(db[COLLECTION_REAL].find({"symbol": symbol}).sort("end_time", -1).limit(80)))
    
    # Lấy Data Dự đoán
    df_pred = pd.DataFrame(list(db[COLLECTION_PRED].find({"symbol": symbol}).sort("prediction_time", 1)))

    # Xử lý Data
    if not df_real.empty and 'end_time' in df_real.columns:
        df_real['timestamp'] = pd.to_datetime(df_real['end_time'])
        df_real = df_real.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='first')
    
    if not df_pred.empty and 'prediction_time' in df_pred.columns:
        df_pred['timestamp'] = pd.to_datetime(df_pred['prediction_time'])
        df_pred = df_pred.sort_values('timestamp')

    return df_real, df_pred

# --- UI ---
st.set_page_config(page_title="AI Stock Prediction", layout="wide", page_icon="🤖")
st.title("AI Stock Prediction (Deep Learning)")

SYMBOLS = ['HPG', 'VIC', 'VNM', 'FPT', 'TCB']
symbol = st.sidebar.selectbox("Chọn Mã:", SYMBOLS)
client = get_client()
placeholder = st.empty()

while True:
    df_real, df_pred = fetch_data(client, symbol)
    
    with placeholder.container():
        if not df_real.empty:
            last_price = df_real.iloc[-1]['close_price']
            
            # Metrics
            c1, c2 = st.columns(2)
            c1.metric("Giá Hiện Tại", f"{last_price:,.0f} ₫")
            
            if not df_pred.empty:
                next_price = df_pred.iloc[-1]['predicted_price']
                delta = next_price - last_price
                c2.metric("AI Dự Báo (10 tick tới)", f"{next_price:,.0f} ₫", 
                          delta=f"{delta:,.0f} ₫", delta_color="normal")

            # Chart
            fig = go.Figure()

            # Đường giá thật
            fig.add_trace(go.Scatter(
                x=df_real['timestamp'], y=df_real['close_price'],
                mode='lines', name='Thực tế', line=dict(color='#00CC96', width=3)
            ))

            # Đường dự đoán (Nối tiếp)
            if not df_pred.empty:
                # Điểm nối
                connect_x = [df_real.iloc[-1]['timestamp'], df_pred.iloc[0]['timestamp']]
                connect_y = [df_real.iloc[-1]['close_price'], df_pred.iloc[0]['predicted_price']]
                
                # Vẽ đường nối (để biểu đồ không bị đứt đoạn)
                fig.add_trace(go.Scatter(
                    x=connect_x, y=connect_y,
                    mode='lines', showlegend=False,
                    line=dict(color='#AB63FA', width=3, dash='dot')
                ))
                
                # Vẽ đường dự đoán chính
                fig.add_trace(go.Scatter(
                    x=df_pred['timestamp'], y=df_pred['predicted_price'],
                    mode='lines', name='AI Dự đoán',
                    line=dict(color='#AB63FA', width=3, dash='dot')
                ))

            fig.update_layout(
                title=f"Dự báo xu hướng {symbol}", height=500,
                xaxis_title="Thời gian", yaxis_title="Giá",
                hovermode="x unified"
            )
            
            st.plotly_chart(fig, use_container_width=True, key=f"ai_chart_{uuid.uuid4()}")
        else:
            st.info("Đang chờ dữ liệu để train mô hình...")
            
    time.sleep(2)