import requests
import datetime
import json

# API VNDirect
VNDIRECT_URL = "https://finfo-api.vndirect.com.vn/v4/stock_prices"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Origin": "https://dchart.vndirect.com.vn",
    "Referer": "https://dchart.vndirect.com.vn/"
}

def get_real_data(symbol):
    try:
        params = {"sort": "date", "q": f"code:{symbol}", "size": "1"}
        # Giảm timeout xuống 2s để nếu mạng lag thì chuyển Mock ngay cho nhanh
        response = requests.get(VNDIRECT_URL, params=params, headers=HEADERS, timeout=2)
        
        if response.status_code == 200:
            data = response.json()
            if data and 'data' in data and len(data['data']) > 0:
                stock_info = data['data'][0]
                price = float(stock_info.get('close') or stock_info.get('average')) * 1000
                volume = int(stock_info.get('nmVolume') or stock_info.get('accumulatedVolume'))
                current_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

                return {
                    "symbol": symbol,
                    "price": price,
                    "volume": volume,
                    "timestamp": current_time,
                    "source": "VNDIRECT_API"
                }
    except Exception:
        # Không in lỗi chi tiết nữa để log sạch đẹp
        return None
    
    return None