import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import re
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from math import radians, cos, sin, asin, sqrt
from itertools import combinations
from dateutil.relativedelta import relativedelta

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新")
sys.path.append(str(custom_path))
import common as kd  

class TrainingConfig:
    HEAVY_ORDER_LIMIT = 1400  
    NEG_SAMPLE_RATIO = 3      
target_date = date.today() - timedelta(days=1)
target_ts = pd.Timestamp(target_date)

print(f"正在讀取資料，截止日期: {target_ts} ...")

total_order = kd.get_data_from_EWMS(f'''
        SELECT
        order_info.erp_NO        AS 訂單編號,
        order_info.KUNNR         AS 客戶編號,
        order_info.SHORTNAME     AS 客戶簡稱,
        order_info.ADDRES        AS 地址,
        order_info.TO_DRIVER     AS 備註,
        order_info.LONGITUDE     AS 收貨_lon,
        order_info.LATITUDE      AS 收貨_lat,
        order_info.[AERA]        AS 區域代碼,
        order_info.[AREACODE]    AS 區域名稱,
        order_info.LOGISTICS     AS 出貨倉,
        order_info.SHIP_DATE     AS 出貨日期,
        log_ctr.LATITUDE         AS 出貨_lat,
        log_ctr.LONGITUDE        AS 出貨_lon,
        ow.訂單總重量,
        ld.排單號,
        ld.車牌號
        FROM [EWMS].[dbo].[SYS_SALES_ORDER] order_info
        LEFT JOIN [EWMS].[dbo].[BIZ_LOGISTICS_CENTRE] log_ctr
        ON order_info.LOGISTICS = log_ctr.LOGISTICS
        LEFT JOIN (
                        SELECT
                                od.ERP_NO,
                                SUM(
                                ISNULL(od.A_OUT_QUANTITY, 0)
                                * ISNULL(mm.NETWEIGHT, 0)
                                ) AS 訂單總重量
                        FROM [EWMS].[dbo].[SYS_SALES_ORDER_DETAILS] od
                        LEFT JOIN [EWMS].[dbo].[SYS_MATERIAL_MASTER] mm
                                ON od.MATNR = mm.MATNR
                        WHERE LEN(od.MATNR) > 0
                        GROUP BY od.ERP_NO
        ) ow
        ON order_info.erp_NO = ow.ERP_NO
        LEFT JOIN (
                        SELECT
                                aa.ERP_NO,
                                MAX(aa.SCHEDULING_NO) AS 排單號,
                                MAX(bb.NO) AS 車牌號
                        FROM [EWMS].[dbo].[BIZ_LOADING_LIST] aa
                        LEFT JOIN [EWMS].[dbo].[BIZ_DISPATCH_LIST] bb
                                ON aa.SCHEDULING_NO = bb.SCHEDULING_NO
                        GROUP BY aa.ERP_NO
        ) ld
        ON order_info.erp_NO = ld.ERP_NO
        where ld.排單號 is not null and order_info.SHIP_DATE <='{target_ts}'
''')

print("正在清洗資料與解析時間...")

total_order['備註'] = (total_order['備註'].replace('', np.nan).fillna('').astype(str).str.strip())
total_order['訂單總重量'] = total_order['訂單總重量'].fillna(0).astype(float)

def normalize_time(t: str) -> str | None:
    if not t:
        return None
    t = t.replace('：', ':').strip()
    if ':' in t:
        h, m = t.split(':', 1)
        if h.isdigit() and m.isdigit():
            h = int(h)
            m = int(m)
            # 90:00 / 090:00 → 9:00
            if h > 23 and h < 100:
                h = h // 10
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f'{h:02d}:{m:02d}'
        return None
    if t.isdigit():
        n = int(t)
        if n < 100:
            h = n // 10 if n >= 24 else n
            if 0 <= h <= 23:
                return f'{h:02d}:00'
        if len(t) == 3:
            h = int(t[0])
            m = int(t[1:])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f'{h:02d}:{m:02d}'
        if len(t) == 4:
            h = int(t[:2])
            m = int(t[2:])
            if 0 <= h <= 23 and 0 <= m <= 59:
                return f'{h:02d}:{m:02d}'
    return None

def parse_delivery_window(note: str):
    default_start, default_end = '08:00', '17:00'
    if not note:
        return default_start, default_end
    if note.count('/') >= 2:
        parts = note.split('/')
        note = parts[0] + '/' + parts[1]
    m = re.search( r'指[:：]?\s*([0-9：:]{1,5})\s*[~～]\s*([0-9：:]{1,5})',note)
    if m:
        start = normalize_time(m.group(1))
        end = normalize_time(m.group(2))
        if start and end:
            return start, end
    if '上' in note:
        return '08:00', '12:00'
    if '下' in note:
        return '12:00', '17:00'
    if '整' in note:
        return default_start, default_end
    return default_start, default_end

total_order[['最早派送時間', '最晚派送時間']] = (total_order['備註'].apply(lambda x: pd.Series(parse_delivery_window(x))))

def combine_date_time(d, t):
    if pd.isna(d) or not t:
        return pd.NaT
    try:
        return datetime.combine(d, datetime.strptime(t, '%H:%M').time())
    except ValueError:
        return pd.NaT

total_order['最早派送datetime'] = total_order.apply(lambda r: combine_date_time(r['出貨日期'], r['最早派送時間']), axis=1)
total_order['最晚派送datetime'] = total_order.apply(lambda r: combine_date_time(r['出貨日期'], r['最晚派送時間']), axis=1)

print(f"資料前處理完成，總筆數: {len(total_order)}")



def haversine(lon1, lat1, lon2, lat2):
    """計算兩點間的經緯度距離 (km)"""
    # 處理空值
    if pd.isna([lon1, lat1, lon2, lat2]).any(): 
        return 999.0
    try:
        lon1, lat1, lon2, lat2 = map(float, [lon1, lat1, lon2, lat2])
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        return c * 6371
    except: 
        return 999.0

def calculate_time_diff_hours(t1, t2):
    """計算兩個 datetime 的差距 (小時)"""
    if pd.isna(t1) or pd.isna(t2): 
        return -1
    return abs((t1 - t2).total_seconds()) / 3600.0

def generate_pairs(df, is_train=True):
    """
    生成訂單對 (Pairs)
    is_train=True: 會根據排單號生成 label (1=同車, 0=不同車)
    """
    pairs = []
    grouped = df.groupby(['出貨日期', '出貨倉'])
    
    total_groups = len(grouped)
    processed_count = 0
    
    print(f"開始生成 Pair 特徵 (共 {total_groups} 個分組)...")

    for _, group in grouped:
        if len(group) < 2: continue
        records = group.to_dict('records')
        for a, b in combinations(records, 2):
            dist = haversine(a['收貨_lon'], a['收貨_lat'], b['收貨_lon'], b['收貨_lat'])
            weight_sum = a['訂單總重量'] + b['訂單總重量']
            same_area = 1 if a['區域代碼'] == b['區域代碼'] else 0
            time_gap = calculate_time_diff_hours(a.get('最晚派送datetime'), b.get('最晚派送datetime'))
            
            feat = {
                'order_a': a['訂單編號'],
                'order_b': b['訂單編號'],
                'dist_km': dist,
                'weight_sum': weight_sum,
                'same_area': same_area,
                'time_gap_hours': time_gap,
            }
            
            if is_train:
                if pd.notna(a['排單號']) and pd.notna(b['排單號']) and (a['排單號'] == b['排單號']):
                    feat['label'] = 1
                else:
                    feat['label'] = 0
            
            pairs.append(feat)
        
        processed_count += 1
        if processed_count % 100 == 0:
            print(f"已處理 {processed_count}/{total_groups} 組...")
            
    return pd.DataFrame(pairs)


def main():
    print(f"\n[Step 1] 過濾超過 {TrainingConfig.HEAVY_ORDER_LIMIT}kg 的大單...")
    train_df = total_order[total_order['訂單總重量'] <= TrainingConfig.HEAVY_ORDER_LIMIT].copy()
    print(f"過濾後剩餘訂單數: {len(train_df)}")
    train_pairs = generate_pairs(train_df, is_train=True)
    
    if train_pairs.empty:
        print("錯誤：生成樣本為空，請檢查輸入數據或日期範圍。")
        return
    print("\n[Step 2] 進行樣本平衡...")
    pos = train_pairs[train_pairs['label'] == 1]
    neg = train_pairs[train_pairs['label'] == 0]
    print(f"原始樣本 - 正樣本(同車): {len(pos)}, 負樣本(不同車): {len(neg)}")

    n_neg = min(len(neg), len(pos) * TrainingConfig.NEG_SAMPLE_RATIO)
    if n_neg > 0:
        neg = neg.sample(n=n_neg, random_state=42)
    
    train_data = pd.concat([pos, neg])
    print(f"訓練樣本 - 正樣本: {len(pos)}, 負樣本: {len(neg)} (Total: {len(train_data)})")
    features = ['dist_km', 'weight_sum', 'same_area', 'time_gap_hours']
    X = train_data[features]
    y = train_data['label']

    print("\n[Step 3] 開始訓練 LightGBM 模型...")
    model = lgb.LGBMClassifier(
        n_estimators=100, 
        learning_rate=0.1, 
        random_state=42,
        n_jobs=-1 
    )
    model.fit(X, y)

    print("訓練完成！特徵重要性:")
    print(dict(zip(features, model.feature_importances_)))
    model_filename = 'lgb_dispatch_model.pkl'
    joblib.dump(model, model_filename)
    print(f"\n[Step 4] 模型已保存至: {model_filename}")
    print("現在可以載入此模型進行派車預測。")

if __name__ == "__main__":
    main()