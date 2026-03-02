
import pandas as pd
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil
from pathlib import Path
import sys

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新")
sys.path.append(str(custom_path))
import common as kd
import numpy as np
import re
import joblib


target_date = date.today() - timedelta(days=1)
target_ts = pd.Timestamp(target_date)


total_order = kd.get_data_from_EWMS(f'''
    -- 1. 處理 SYS_SALES_ORDER 的明細數據 (重量與作業時間)
    WITH SalesOrderMetrics AS (
        SELECT
            od.ERP_NO,
            SUM(ISNULL(od.A_OUT_QUANTITY, 0) * ISNULL(mm.NETWEIGHT, 0)) AS 總重量,
            SUM(ISNULL(jt.TIME, 0)) AS 總作業時間
        FROM [EWMS].[dbo].[SYS_SALES_ORDER_DETAILS] od
        LEFT JOIN [EWMS].[dbo].[SYS_MATERIAL_MASTER] mm ON od.MATNR = mm.MATNR
        LEFT JOIN [EWMS].[dbo].[BIZ_LOGISTICS_JOBTIME] jt ON od.MCLASS_CODE = jt.JOB_TYPE 
            AND od.A_OUT_QUANTITY >= jt.MIN 
            AND od.A_OUT_QUANTITY <= jt.MAX
            AND jt.IS_ENABLE = 1
        WHERE LEN(od.MATNR) > 0
        GROUP BY od.ERP_NO
    ),

    -- 2. 處理 SYS_OUT_CONTACT 的明細數據 (作業時間)
    ContactOrderMetrics AS (
        SELECT
            cd.ERP_NO,
            SUM(ISNULL(cd.A_OUT_QUANTITY, 0)) AS 總數量, -- 備用，若聯絡單也需要算重量可在此擴充
            SUM(ISNULL(jt.TIME, 0)) AS 總作業時間
        FROM [EWMS].[dbo].[SYS_OUT_CONTACT_DETAILS] cd
        LEFT JOIN [EWMS].[dbo].[BIZ_LOGISTICS_JOBTIME] jt ON cd.MCLASS_CODE = jt.JOB_TYPE 
            AND cd.A_OUT_QUANTITY >= jt.MIN 
            AND cd.A_OUT_QUANTITY <= jt.MAX
            AND jt.IS_ENABLE = 1
        WHERE LEN(cd.MATNR) > 0
        GROUP BY cd.ERP_NO
    )

    -- 3. 合併數據
    SELECT 
        er.erp_NO as 訂單編號, er.KUNNR as 客戶編號, er.SHORTNAME as 客戶簡稱,
        er.ADDRES as 地址, er.LOGISTICS as 分區, er.TO_DRIVER as 備註,
        er.LONGITUDE as 收貨_lon, er.LATITUDE as 收貨_lat, er.SHIPMENTDATE as 出貨日期,
        'SYS_OUT_CONTACT' as 數據來源,
        CASE WHEN er.TYPE = '1' THEN '退貨' ELSE '賣貨' END as 訂單類型,
        NULL as 訂單總重量,
        cm.總作業時間 as 作業時間
    FROM [EWMS].[dbo].[SYS_OUT_CONTACT] er
    LEFT JOIN ContactOrderMetrics cm ON er.erp_NO = cm.ERP_NO
    WHERE er.SHIPMENTDATE = '{target_date}' and er.KUNNR like '%TW%'

    UNION ALL

    SELECT 
        so.erp_NO as 訂單編號, so.KUNNR as 客戶編號, so.SHORTNAME as 客戶簡稱,
        so.ADDRES as 地址, so.LOGISTICS as 分區, so.TO_DRIVER as 備註,
        so.LONGITUDE as 收貨_lon, so.LATITUDE as 收貨_lat, so.SHIP_DATE as 出貨日期,
        'SYS_SALES_ORDER' as 數據來源,
        '賣貨' as 訂單類型,
        sm.總重量 as 訂單總重量,
        sm.總作業時間 as 作業時間
    FROM [EWMS].[dbo].[SYS_SALES_ORDER] so
    LEFT JOIN SalesOrderMetrics sm ON so.erp_NO = sm.ERP_NO
    WHERE so.SHIP_DATE = '{target_date}' and so.KUNNR like '%TW%'
''')


print(total_order.replace('', np.nan).isna().sum())
total_order['備註'] = (  total_order['備註']  .replace('', np.nan) .fillna('') .astype(str).str.strip())
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
total_order[['最早派送時間', '最晚派送時間']] = (total_order['備註'] .apply(lambda x: pd.Series(parse_delivery_window(x))))
def combine_date_time(d, t):
    if pd.isna(d) or not t:
        return pd.NaT
    try:
        return datetime.combine(d,datetime.strptime(t, '%H:%M').time())
    except ValueError:
        return pd.NaT


total_order['最早派送datetime'] = total_order.apply( lambda r: combine_date_time(r['出貨日期'], r['最早派送時間']),axis=1)
total_order['最晚派送datetime'] = total_order.apply( lambda r: combine_date_time(r['出貨日期'], r['最晚派送時間']), axis=1)





import re
import numpy as np

def extract_weight_with_log(remark):
    if not remark or str(remark) == 'nan' or str(remark).strip() == '':
        return 0.0, "無資料"
    
    # ==========================================
    # 0. 基礎清洗 (新增去除 '約', '大約')
    # ==========================================
    # 統一全形半形，將 ? 轉為 5，並移除模糊量詞
    raw_text = str(remark).replace('：', ':').replace('～', '~').replace('?', '5')
    raw_text = raw_text.replace('大約', '').replace('約', '') # <--- 新增這行
    
    # ==========================================
    # 1. 分段清洗 (新增 whole 識別)
    # ==========================================
    segments = raw_text.split('/')
    valid_segments = []
    
    # 雜訊特徵：只包含 數字、空白、符號、"整"、"whole"
    noise_pattern = re.compile(r'^[\d\s\.\-\~\*\+\(\)整whole:：]+$', re.IGNORECASE)
    
    for seg in segments:
        seg = seg.strip()
        if not seg: continue
        
        # 移除電話/統編
        seg = re.sub(r'\d{4}-\d{6}|\d{8,10}', ' ', seg)
        
        # 過濾雜訊段落 (例如 "1/30 whole")
        if noise_pattern.match(seg):
            continue 
            
        valid_segments.append(seg)
    
    text = " ".join(valid_segments)
    text = text.replace('*', ' ') 

    # ==========================================
    # 2. 特殊固定重量
    # ==========================================
    total_w = 0.0
    logs = []
    
    if '強化木芯板' in text:
        total_w += 30.0
        logs.append("強化木芯板:1片(30.0kg)")
        text = text.replace('強化木芯板', ' ')

    # ==========================================
    # 3. 代幣化 (Tokenization)
    # ==========================================
    # 新增 '薄' 以及英文關鍵字，防止數字被錯誤的 Token 搶走
    token_map = [
        (['厚板', '厚', 'thick'], 30.0, "厚板", "TOKEN_A"), 
        (['薄板', '板', '薄', 'thin'], 10.0, "薄板", "TOKEN_B"),
        (['布', 'cloth'], 0.5, "布", "TOKEN_C")
    ]
    
    for keywords, unit_w, label, token in token_map:
        # 排序：長的關鍵字優先
        keywords = sorted(keywords, key=len, reverse=True)
        kw_regex = '|'.join(map(re.escape, keywords))
        
        # 使用 (?i) 開啟不區分大小寫
        text = re.sub(fr'(?i)({kw_regex})', f' {token} ', text)

    # ==========================================
    # 4. 提取數量
    # ==========================================
    # 模式：(Token + 數字) 或 (數字 + Token)
    pattern = re.compile(r'(TOKEN_[ABC])\s*(\d+)|(\d+)\s*(TOKEN_[ABC])')
    token_info = {t[3]: (t[1], t[2]) for t in token_map}

    matches = pattern.finditer(text)
    
    for m in matches:
        g1_token, g1_num, g2_num, g2_token = m.groups()
        
        # 判斷是哪一種組合抓到了
        if g1_token: # Token 在前 (厚 5)
            target_token = g1_token
            num = int(g1_num)
        else:        # Token 在後 (8 厚)
            target_token = g2_token
            num = int(g2_num)
            
        unit_w, label = token_info[target_token]
        w = num * unit_w
        total_w += w
        logs.append(f"{label}:{num}({w}kg)")

    # 5. 總結
    log_str = " + ".join(logs) if logs else "未匹配到重量關鍵字"
    
    # 檢查殘留文字 (除錯用)
    remaining = re.sub(r'TOKEN_[ABC]|\d|\s', '', text)
    if total_w == 0.0 and remaining:
         log_str += f" (剩餘: {remaining})"

    return total_w, log_str

# 應用到 DataFrame
total_order['解析詳情'] = ""
total_order['備註'] = (total_order['備註'].replace('', np.nan).fillna('').astype(str).str.strip())

# 測試執行
mask = total_order['數據來源'] == 'SYS_OUT_CONTACT'
if mask.any():
    results = total_order.loc[mask, '備註'].apply(extract_weight_with_log)
    total_order.loc[mask, '訂單總重量'] = results.apply(lambda x: x[0])
    total_order.loc[mask, '解析詳情'] = results.apply(lambda x: x[1])
    
total_order.loc[~mask, '解析詳情'] = "SQL 系統計算重量"




def calculate_return_job_time(row):
    # 只針對退貨且作業時間尚未由 SQL 算出的資料
    if pd.notna(row['作業時間']) :
        return row['作業時間']
    
    details = str(row['解析詳情'])
    
    # 1. 基礎時間：只要有這一單，起跳就是 10 分鐘
    base_time = 10.0
    
    # 提取數量
    thin_match = re.search(r'薄板:(\d+)', details)
    thick_match = re.search(r'厚板:(\d+)', details)
    
    thin_count = int(thin_match.group(1)) if thin_match else 0
    thick_count = int(thick_match.group(1)) if thick_match else 0
    
    # 2. 計算超額加成時間
    extra_time = 0
    
    # 薄板：超過 10 塊的部分，每 10 塊加 5 分鐘
    if thin_count > 10:
        extra_time += ceil((thin_count - 10) / 10) * 5
        
    # 厚板：超過 6 塊的部分，每 5 塊加 5 分鐘
    if thick_count > 6:
        extra_time += ceil((thick_count - 6) / 5) * 5
        
    # 3. 總合並套用上限 140 分鐘
    final_time = min(140.0, base_time + extra_time)
    
    return final_time

# 執行更新
total_order['作業時間'] = total_order.apply(calculate_return_job_time, axis=1)





























print("正在載入區域和出貨倉的模型...")
clf_wh = joblib.load('model_warehouse.pkl')
clf_ar = joblib.load('model_area.pkl')
rule_map = joblib.load('map_rules.pkl')

new_df = total_order.copy()
new_df['收貨_lon'] = pd.to_numeric(new_df['收貨_lon'], errors='coerce')
new_df['收貨_lat'] = pd.to_numeric(new_df['收貨_lat'], errors='coerce')
valid_mask = (new_df['收貨_lon'].notna()) & (new_df['收貨_lat'].notna())
df_predict = new_df[valid_mask].copy()
print(f"待預測筆數: {len(df_predict)}")


def hybrid_predict_batch(df, threshold=0.75):
    TW_MIN_LAT, TW_MAX_LAT = 21.0, 26.0  
    TW_MIN_LON, TW_MAX_LON = 119.0, 123.0
    X = df[['收貨_lon', '收貨_lat']].values

    out_of_bounds_mask = ((df['收貨_lat'] < TW_MIN_LAT) | (df['收貨_lat'] > TW_MAX_LAT) |
        (df['收貨_lon'] < TW_MIN_LON) | (df['收貨_lon'] > TW_MAX_LON))
    
    pred_wh = clf_wh.predict(X)
    probs = clf_wh.predict_proba(X)
    conf_wh = np.max(probs, axis=1)
    pred_area = clf_ar.predict(X)
    probs_area = clf_ar.predict_proba(X)
    conf_area = np.max(probs_area, axis=1)

    final_wh = pred_wh.copy()
    decision_notes = []
    
    for i in range(len(df)):
        if out_of_bounds_mask.iloc[i]:
            final_wh[i] = "人工確認"  
            decision_notes.append(f"座標異常: 飛到台灣範圍外 ({df['收貨_lat'].iloc[i]:.2f}, {df['收貨_lon'].iloc[i]:.2f})")
            continue  

        p_wh = pred_wh[i]
        c_wh = conf_wh[i]
        p_area = pred_area[i]
        c_area = conf_area[i]
        rule_wh = rule_map.get(p_area, None)
        note = "系統自動分派"
        if c_wh >= threshold:
            note = f"主模型確信 ({c_wh:.0%})"
        else:
            if rule_wh is None:
                note = f"主模型信心低({c_wh:.0%})且無區域規則"
            elif rule_wh == p_wh:
                note = f"雙模型一致 ({c_wh:.0%})"
            else:
                if c_area > 0.8:
                    final_wh[i] = rule_wh
                    note = f"已修正: 依區域 {p_area} 規則"
                else:
                    note = f"需要人工: 衝突且信心皆低"
        decision_notes.append(note)
    return final_wh, pred_area, decision_notes

print("正在進行智慧運算...")
final_wh, pred_area, notes = hybrid_predict_batch(df_predict)
df_predict['建議_出貨倉'] = final_wh
df_predict['預測_區域'] = pred_area
df_predict['系統備註'] = notes



# 獲取出貨倉經緯度
outer= kd.get_data_from_EWMS(f'''
        SELECT [LOGISTICS] 建議_出貨倉,[LOGISTICS_NAME] 出貨倉名稱,[LATITUDE] 出貨_lat ,[LONGITUDE] 出貨_lon
        FROM [EWMS].[dbo].[BIZ_LOGISTICS_CENTRE]
        where IS_ENABLE = 1
''')


df_predict = pd.merge(df_predict,outer, on = '建議_出貨倉', how ='left' )


















import pandas as pd
import numpy as np
import joblib
import lightgbm as lgb
from datetime import datetime, date
from math import radians, cos, sin, asin, sqrt
from itertools import combinations
import warnings
import random
import requests
import time
import folium

warnings.filterwarnings('ignore')

# ==============================================================================
# 1. 全域配置
# ==============================================================================
class Config:
    MAX_TRUCK_WEIGHT = 1400      # 車輛最大載重
    HEAVY_ORDER_LIMIT = 1400     # 大單門檻
    MERGE_PROB_THRESHOLD = 0.3   # 併車門檻
    MAX_STOPS_PER_TRIP = 30      # [新增] 單車最大站點數限制

# ==============================================================================
# 2. 工具函數
# ==============================================================================
def haversine(lon1, lat1, lon2, lat2):
    """計算經緯度距離 (km)"""
    if pd.isna([lon1, lat1, lon2, lat2]).any(): return 999.0
    try:
        lon1, lat1, lon2, lat2 = map(float, [lon1, lat1, lon2, lat2])
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        return c * 6371
    except: return 999.0

def calculate_time_diff_hours(t1, t2):
    if pd.isna(t1) or pd.isna(t2): return -1
    return abs((t1 - t2).total_seconds()) / 3600.0

def generate_pairs(df):
    """生成配對特徵"""
    pairs = []
    grouped = df.groupby(['出貨日期', '出貨倉'])
    
    for _, group in grouped:
        if len(group) < 2: continue
        records = group.to_dict('records')
        
        for a, b in combinations(records, 2):
            dist = haversine(a['收貨_lon'], a['收貨_lat'], b['收貨_lon'], b['收貨_lat'])
            weight_sum = a['訂單總重量'] + b['訂單總重量']
            same_area = 1 if a['區域代碼'] == b['區域代碼'] else 0
            time_gap = calculate_time_diff_hours(a.get('最晚派送datetime'), b.get('最晚派送datetime'))
            
            pairs.append({
                'order_a': a['訂單編號'],
                'order_b': b['訂單編號'],
                'dist_km': dist,
                'weight_sum': weight_sum,
                'same_area': same_area,
                'time_gap_hours': time_gap
            })
    return pd.DataFrame(pairs)

# ==============================================================================
# 3. 核心派車邏輯 (升級版：加入站點數限制)
# ==============================================================================
def run_dispatch(daily_orders, model, warehouse_name):
    """
    核心派車邏輯 - 支援「退貨分離」、「雙重載重檢查」、「最大站點限制」
    """
    if '訂單類型' not in daily_orders.columns:
        daily_orders['訂單類型'] = '賣貨'
    
    daily_orders['is_return'] = daily_orders['訂單類型'] == '退貨'
    daily_orders['w_send'] = daily_orders.apply(lambda x: x['訂單總重量'] if not x['is_return'] else 0, axis=1)
    daily_orders['w_ret'] = daily_orders.apply(lambda x: x['訂單總重量'] if x['is_return'] else 0, axis=1)

    limit = Config.HEAVY_ORDER_LIMIT
    pending_heavy = daily_orders[daily_orders['訂單總重量'] > limit].copy()
    active_orders = daily_orders[daily_orders['訂單總重量'] <= limit].copy()
    
    if active_orders.empty:
        if not pending_heavy.empty:
            pending_heavy['預測_車次ID'] = f"{warehouse_name}_大單待排"
            pending_heavy['派送順序'] = 1
        return pd.concat([active_orders, pending_heavy])
    
    pairs_pred = generate_pairs(active_orders)
    
    cluster_map = {oid: oid for oid in active_orders['訂單編號']}
    
    # [修改] 這裡加入 'count' 來追蹤站點數
    cluster_stats = {}
    for _, row in active_orders.iterrows():
        cluster_stats[row['訂單編號']] = {
            'send': row['w_send'], 
            'ret': row['w_ret'],
            'count': 1  # 初始每個 cluster 只有 1 個點
        }
    
    if not pairs_pred.empty:
        features = ['dist_km', 'weight_sum', 'same_area', 'time_gap_hours']
        for f in features: 
            if f not in pairs_pred.columns: pairs_pred[f] = 0
            
        pairs_pred['probability'] = model.predict_proba(pairs_pred[features])[:, 1]
        sorted_pairs = pairs_pred.sort_values(by='probability', ascending=False)
        
        for _, row in sorted_pairs.iterrows():
            if row['probability'] < Config.MERGE_PROB_THRESHOLD: break
                
            oid_a, oid_b = row['order_a'], row['order_b']
            
            root_a = cluster_map[oid_a]
            while cluster_map[root_a] != root_a: root_a = cluster_map[root_a]
            root_b = cluster_map[oid_b]
            while cluster_map[root_b] != root_b: root_b = cluster_map[root_b]
            
            if root_a != root_b:
                stats_a = cluster_stats[root_a]
                stats_b = cluster_stats[root_b]
                
                new_send_w = stats_a['send'] + stats_b['send']
                new_ret_w = stats_a['ret'] + stats_b['ret']
                # [新增] 計算合併後的總站點數
                new_count = stats_a['count'] + stats_b['count']
                
                # [修改] 規則：檢查 重量 AND 站點數
                is_weight_ok = (new_send_w <= Config.MAX_TRUCK_WEIGHT) and (new_ret_w <= Config.MAX_TRUCK_WEIGHT)
                is_stops_ok = (new_count <= Config.MAX_STOPS_PER_TRIP)
                
                if is_weight_ok and is_stops_ok:
                    cluster_stats[root_a]['send'] = new_send_w
                    cluster_stats[root_a]['ret'] = new_ret_w
                    cluster_stats[root_a]['count'] = new_count # 更新站點數
                    del cluster_stats[root_b]
                    
                    for k, v in cluster_map.items():
                        if v == root_b: cluster_map[k] = root_a

    active_orders['raw_root'] = active_orders['訂單編號'].map(cluster_map).apply(lambda x: cluster_map.get(x, x))
    unique_roots = active_orders['raw_root'].unique()
    root_to_id = {root: i+1 for i, root in enumerate(unique_roots)}
    active_orders['預測_車次ID'] = active_orders['raw_root'].apply(lambda x: f"{warehouse_name}_{root_to_id[x]:03d}")
    
    if not pending_heavy.empty:
        pending_heavy['預測_車次ID'] = f"{warehouse_name}_大單待排"

    result_df = pd.concat([active_orders, pending_heavy])
    return result_df

# ==============================================================================
# [NEW] 強力後處理：貪婪孤兒收養機制 (加入站點限制)
# ==============================================================================
def optimize_orphans_greedy(df_dispatched, warehouse_limit=1400, max_stops=30):
    """
    貪婪收養機制：
    無視 AI 機率，只要「不超重」且「不超點」且「距離最近」，就強制合併。
    """
    df = df_dispatched.copy()
    
    if 'w_send' not in df.columns:
        df['is_return'] = df['訂單類型'] == '退貨'
        df['w_send'] = df.apply(lambda x: x['訂單總重量'] if not x['is_return'] else 0, axis=1)
        df['w_ret'] = df.apply(lambda x: x['訂單總重量'] if x['is_return'] else 0, axis=1)
        
    trip_stats = {}
    trip_ids = df['預測_車次ID'].unique()
    
    for tid in trip_ids:
        rows = df[df['預測_車次ID'] == tid]
        if "大單" in str(tid): continue
            
        coords = rows[['收貨_lat', '收貨_lon']].dropna().to_numpy()
        if len(coords) == 0: continue
        
        center = coords.mean(axis=0)
        trip_stats[tid] = {
            'w_send': rows['w_send'].sum(),
            'w_ret': rows['w_ret'].sum(),
            'count': len(rows),
            'center': center,
            'coords': coords
        }
        
    orphans = []
    hosts = []
    
    for tid, stats in trip_stats.items():
        # 提高門檻到 800kg，盡可能消滅未滿載車次
        is_light = (stats['w_send'] + stats['w_ret']) < 800
        # 站點數很少的也算孤兒
        is_few_stops = stats['count'] <= 3 
        
        if is_light and is_few_stops:
            orphans.append(tid)
        else:
            # 只有當 Host 站點數還沒爆的時候，才有資格當養父母
            if stats['count'] < max_stops:
                hosts.append(tid)
            
    if not orphans:
        return df
        
    print(f"  >>> [優化中] 啟動強力貪婪收養：發現 {len(orphans)} 個孤兒車次...")
    
    orphans.sort(key=lambda x: trip_stats[x]['w_send'] + trip_stats[x]['w_ret'])
    
    merge_count = 0
    
    for orphan_id in orphans:
        o_stat = trip_stats[orphan_id]
        o_center = o_stat['center']
        
        best_host = None
        min_dist = float('inf')
        
        for host_id in hosts:
            h_stat = trip_stats[host_id]
            
            # [檢查 1] 雙重載重限制
            new_send = h_stat['w_send'] + o_stat['w_send']
            new_ret = h_stat['w_ret'] + o_stat['w_ret']
            if new_send > warehouse_limit or new_ret > warehouse_limit:
                continue 
            
            # [檢查 2] 站點數限制 (關鍵修改)
            if h_stat['count'] + o_stat['count'] > max_stops:
                continue

            # [檢查 3] 距離 (最近鄰)
            dists = np.sum((h_stat['coords'] - o_center)**2, axis=1)
            closest_d = np.min(dists)
            
            if closest_d < min_dist:
                min_dist = closest_d
                best_host = host_id
        
        if best_host:
            df.loc[df['預測_車次ID'] == orphan_id, '預測_車次ID'] = best_host
            
            trip_stats[best_host]['w_send'] += o_stat['w_send']
            trip_stats[best_host]['w_ret'] += o_stat['w_ret']
            trip_stats[best_host]['count'] += o_stat['count']
            trip_stats[best_host]['coords'] = np.vstack([trip_stats[best_host]['coords'], o_stat['coords']])
            
            # 如果養父母也滿了 (超過站點限制)，就從候選名單移除，避免下一輪又被塞
            if trip_stats[best_host]['count'] >= max_stops:
                if best_host in hosts:
                    hosts.remove(best_host)

            merge_count += 1

    print(f"  >>> [優化完成] 成功消滅 {merge_count} 個浪費車次！")
    return df

# ==============================================================================
# 4. 路徑排序 (2-Opt) & 載重計算 (修正版)
# ==============================================================================
def calculate_dist_sq(p1, p2):
    return (p1['收貨_lat'] - p2['收貨_lat'])**2 + (p1['收貨_lon'] - p2['收貨_lon'])**2

def path_length(path, wh_lat, wh_lon):
    """
    計算路徑總權重：點對點距離 + 最後一點回倉庫的距離
    """
    dist = 0
    for i in range(len(path) - 1):
        dist += calculate_dist_sq(path[i], path[i+1])
    
    # 加上「最後一點回到倉庫」的距離
    if len(path) > 0:
        last_stop = path[-1]
        warehouse_point = {
            '收貨_lat': wh_lat, 
            '收貨_lon': wh_lon
        }
        dist += calculate_dist_sq(last_stop, warehouse_point)
        
    return dist

# [修正 1] 函式定義增加 wh_lat, wh_lon 參數
def solve_2opt(route, wh_lat, wh_lon, max_iterations=50):
    best_route = route.copy()
    improved = True
    count = 0
    
    if len(route) <= 3: return route

    while improved and count < max_iterations:
        improved = False
        count += 1
        
        # [修正 2] 呼叫 path_length 時，把 wh_lat, wh_lon 傳進去
        current_len = path_length(best_route, wh_lat, wh_lon)
        
        for i in range(1, len(best_route) - 2):
            for k in range(i + 1, len(best_route) - 1):
                new_route = best_route[:i] + best_route[i:k+1][::-1] + best_route[k+1:]
                
                # [修正 3] 這裡也要傳
                new_len = path_length(new_route, wh_lat, wh_lon)
                
                if new_len < current_len:
                    best_route = new_route
                    current_len = new_len
                    improved = True
                    break 
            if improved: break
    return best_route

def sort_route_stops(df_trip, warehouse_lat, warehouse_lon):
    df_trip = df_trip.copy()
    try:
        df_trip['收貨_lat'] = pd.to_numeric(df_trip['收貨_lat'], errors='coerce')
        df_trip['收貨_lon'] = pd.to_numeric(df_trip['收貨_lon'], errors='coerce')
        curr_lat = float(warehouse_lat)
        curr_lon = float(warehouse_lon)
    except:
        return df_trip

    valid_points = df_trip.dropna(subset=['收貨_lat', '收貨_lon']).to_dict('records')
    if not valid_points: return df_trip

    deliveries = [p for p in valid_points if p['訂單類型'] != '退貨']
    returns = [p for p in valid_points if p['訂單類型'] == '退貨']
    
    final_route = []

    if deliveries:
        nearest_idx = -1
        min_dist = float('inf')
        for i, p in enumerate(deliveries):
            d = (p['收貨_lat'] - curr_lat)**2 + (p['收貨_lon'] - curr_lon)**2
            if d < min_dist:
                min_dist = d
                nearest_idx = i
        
        first_stop = deliveries.pop(nearest_idx)
        final_route.append(first_stop)
        curr_lat, curr_lon = first_stop['收貨_lat'], first_stop['收貨_lon']

    remaining_pool = deliveries + returns
    while remaining_pool:
        nearest_idx = -1
        min_dist = float('inf')
        for i, p in enumerate(remaining_pool):
            d = (p['收貨_lat'] - curr_lat)**2 + (p['收貨_lon'] - curr_lon)**2
            if d < min_dist:
                min_dist = d
                nearest_idx = i
        
        next_point = remaining_pool.pop(nearest_idx)
        final_route.append(next_point)
        curr_lat, curr_lon = next_point['收貨_lat'], next_point['收貨_lon']

    if len(final_route) > 2:
        start_node = final_route[0]
        rest_nodes = final_route[1:]
        
        # [修正 4] 呼叫 solve_2opt 時，必須把倉庫座標傳進去
        optimized_rest = solve_2opt(rest_nodes, warehouse_lat, warehouse_lon)
        
        final_route = [start_node] + optimized_rest

    result_df = pd.DataFrame(final_route)
    result_df['建議_配送順序'] = range(1, len(result_df) + 1)
    return result_df

def calculate_dynamic_loads(trip_df):
    trip_df = trip_df.sort_values('建議_配送順序').copy()
    initial_load = trip_df[trip_df['訂單類型'] != '退貨']['訂單總重量'].sum()
    current_load = initial_load
    
    arrival_loads = []
    departure_loads = []
    
    for _, row in trip_df.iterrows():
        weight = row['訂單總重量']
        is_return = (row['訂單類型'] == '退貨')
        arrival_loads.append(round(current_load, 1))
        
        if is_return:
            current_load += weight
        else:
            current_load -= weight
            
        departure_loads.append(round(current_load, 1))
        
    trip_df['抵達載重(kg)'] = arrival_loads
    trip_df['離開載重(kg)'] = departure_loads
    trip_df['車次初始總重'] = initial_load
    return trip_df

def analyze_short_trips(df_result):
    print("\n========== 車次健康度診斷 (優化後) ==========")
    
    trip_stats = df_result.groupby('預測_車次ID').agg({
        '訂單編號': 'count',
        '訂單總重量': 'sum',
        '出貨倉': 'first',
        '抵達載重(kg)': 'max', 
        '車次初始總重': 'max'
    }).rename(columns={'訂單編號': '站點數', '訂單總重量': '貨物總重'})
    
    short_trips = trip_stats[trip_stats['站點數'] <= 2].copy()
    
    print(f"總車次: {len(trip_stats)}")
    print(f"短車次 (<=2站): {len(short_trips)} 趟 (佔 {len(short_trips)/len(trip_stats):.1%})")
    
    if short_trips.empty: return

    full_load_threshold = 1400 * 0.8
    def classify_reason(row):
        if "大單" in str(row.name): return "特殊規則 (大單直發)"
        if row['貨物總重'] >= full_load_threshold: return "物理限制 (車已滿載)"
        return "AI 優化空間 (車未滿載)"
    
    short_trips['原因'] = short_trips.apply(classify_reason, axis=1)
    print("\n[短車次原因分析]")
    print(short_trips['原因'].value_counts())

def get_osrm_route(coordinates):
    if len(coordinates) < 2: return coordinates
    locs = [f"{lon:.5f},{lat:.5f}" for lat, lon in coordinates]
    loc_str = ";".join(locs)
    url = f"http://router.project-osrm.org/route/v1/driving/{loc_str}?overview=full&geometries=geojson"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data['code'] == 'Ok':
                geometry = data['routes'][0]['geometry']['coordinates']
                return [[lat, lon] for lon, lat in geometry]
    except: pass
    return coordinates

# ==============================================================================
# 5. 主執行區塊
# ==============================================================================

try:
    lgb_dispatch = joblib.load('lgb_dispatch_model.pkl') 
except:
    print("找不到模型檔案，請確認路徑")
    lgb_dispatch = None 

if 'df_predict' not in locals():
    print(" 警告：df_predict 變數不存在，請先執行資料獲取與清洗段落。")
else:
    dispatch_input_df = df_predict.copy()
    dispatch_input_df['出貨倉'] = dispatch_input_df['建議_出貨倉']
    dispatch_input_df['區域代碼'] = dispatch_input_df['預測_區域']

    final_results = []

    print("\n開始進行派車與路徑排序...")

    for warehouse in dispatch_input_df['出貨倉'].unique():
        if warehouse == '人工確認': continue
        
        wh_info = dispatch_input_df[dispatch_input_df['出貨倉'] == warehouse].iloc[0]
        try:
            wh_lat = float(wh_info['出貨_lat'])
            wh_lon = float(wh_info['出貨_lon'])
        except (ValueError, TypeError):
            wh_lat, wh_lon = 0.0, 0.0
        
        subset = dispatch_input_df[dispatch_input_df['出貨倉'] == warehouse].copy()
        
        # [Step 1] AI 初步分車 (加入站點數限制)
        dispatched = run_dispatch(subset, lgb_dispatch, warehouse_name=str(warehouse))
        
        # [Step 2] 貪婪孤兒收養機制 (加入站點數限制)
        # 注意這裡傳入 Config.MAX_STOPS_PER_TRIP
        dispatched = optimize_orphans_greedy(dispatched, 
                                           warehouse_limit=Config.MAX_TRUCK_WEIGHT,
                                           max_stops=Config.MAX_STOPS_PER_TRIP)
        
        # [Step 3] 排序與載重計算
        for trip_id in dispatched['預測_車次ID'].unique():
            trip_data = dispatched[dispatched['預測_車次ID'] == trip_id].copy()
            
            # A. 排序
            if '大單' in str(trip_id):
                trip_data['建議_配送順序'] = 1
                sorted_trip = trip_data
            else:
                sorted_trip = sort_route_stops(trip_data, wh_lat, wh_lon)
                
            # B. 計算載重
            final_trip = calculate_dynamic_loads(sorted_trip)
            final_results.append(final_trip)

    if final_results:
        final_df = pd.concat(final_results)
        export_df = final_df.sort_values(['出貨倉', '預測_車次ID', '建議_配送順序'])
        
        filename = 'AI_智慧派車_含排序_詳細版.xlsx'
        export_df.to_excel(filename, index=False)
        print(f"\n Excel 已匯出：{filename}")
        
        analyze_short_trips(final_df)
        
        # [Step 4] 繪製 HTML 地圖
        print("\n開始繪製地圖...")
        plot_df = final_df.sort_values(['出貨倉', '預測_車次ID', '建議_配送順序']).copy()
        center_lat = plot_df['收貨_lat'].mean()
        center_lon = plot_df['收貨_lon'].mean()
        m = folium.Map(location=[center_lat, center_lon], zoom_start=13, tiles="CartoDB positron")

        def get_color(trip_id):
            if "大單" in str(trip_id): return "#000000"
            random.seed(str(trip_id))
            return "#{:06x}".format(random.randint(0, 0xFFFFFF))

        unique_trips = plot_df['預測_車次ID'].unique()
        total_trips = len(unique_trips)
        
        for idx, trip_id in enumerate(unique_trips):
            print(f"繪圖中 ({idx+1}/{total_trips}): 車次 {trip_id}")
            trip_data = plot_df[plot_df['預測_車次ID'] == trip_id]
            color = get_color(trip_id)
            
            try:
                wh_lat = float(trip_data.iloc[0]['出貨_lat'])
                wh_lon = float(trip_data.iloc[0]['出貨_lon'])
            except: 
                wh_lat = trip_data.iloc[0]['收貨_lat']
                wh_lon = trip_data.iloc[0]['收貨_lon']

            waypoints = [[wh_lat, wh_lon]]
            final_return_load = 0
            
            for _, row in trip_data.iterrows():
                try:
                    p_lat, p_lon = float(row['收貨_lat']), float(row['收貨_lon'])
                    waypoints.append([p_lat, p_lon])
                except: continue
                
                arr_w = row.get('抵達載重(kg)', 0)
                dep_w = row.get('離開載重(kg)', 0)
                is_return = (row['訂單類型'] == '退貨')
                type_str = "<span style='color:red;'><b>[退貨]</b></span> (▲)" if is_return else "[賣貨] (●)"
                final_return_load = dep_w
                
                popup_html = f"""
                <div style="font-family: Arial; width: 200px;">
                    <b>車次: {trip_id}</b><br>順序: #{row['建議_配送順序']}<br>
                    <hr>
                    類型: {type_str}<br>
                    客戶: {row['客戶簡稱']}<br>
                    重量: {row['訂單總重量']} kg<br>
                    <hr>
                    抵達載重: {arr_w} kg<br>
                    離開載重: {dep_w} kg
                </div>
                """
                
                tooltip_txt = f"#{row['建議_配送順序']} {row['訂單類型']}"
                
                if is_return:
                    folium.RegularPolygonMarker(
                        location=[p_lat, p_lon],
                        number_of_sides=3, radius=10, rotation=30,
                        color=color, fill=True, fill_opacity=0.9,
                        popup=folium.Popup(popup_html, max_width=300),
                        tooltip=tooltip_txt
                    ).add_to(m)
                else:
                    folium.CircleMarker(
                        location=[p_lat, p_lon],
                        radius=7, color=color, fill=True, fill_opacity=0.7,
                        popup=folium.Popup(popup_html, max_width=300),
                        tooltip=tooltip_txt
                    ).add_to(m)

            if len(waypoints) > 1:
                waypoints.append([wh_lat, wh_lon])
                road_path = get_osrm_route(waypoints)
                folium.PolyLine(
                    locations=road_path,
                    color=color,
                    weight=3,
                    opacity=0.7,
                    tooltip=f"車次: {trip_id}"
                ).add_to(m)
                
                if final_return_load > 0:
                    folium.Marker(
                        location=[wh_lat, wh_lon],
                        icon=folium.Icon(color='green', icon='flag', prefix='fa'),
                        popup=f"車次 {trip_id} 返倉卸貨: {final_return_load} kg",
                        tooltip=f"{trip_id} 返倉"
                    ).add_to(m)
            
            time.sleep(0.1)

        unique_wh = plot_df[['出貨倉', '出貨_lat', '出貨_lon']].drop_duplicates()
        for _, row in unique_wh.iterrows():
            try:
                folium.Marker(
                    location=[float(row['出貨_lat']), float(row['出貨_lon'])],
                    popup=f"倉庫: {row['出貨倉']}",
                    icon=folium.Icon(color='red', icon='home', prefix='fa'),
                    z_index_offset=1000
                ).add_to(m)
            except: pass

        output_file = "dispatch_map_final.html"
        m.save(output_file)
        print(f" 地圖生成完畢！請打開: {output_file}")

