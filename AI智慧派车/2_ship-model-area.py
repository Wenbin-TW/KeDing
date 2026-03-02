
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys
import joblib

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新")
sys.path.append(str(custom_path))
import common as kd
from datetime import date, timedelta
import numpy as np
import re

target_date = date.today() - timedelta(days=1)

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

''')


print(total_order.replace('', np.nan).isna().sum())








total_order['備註'] = (
    total_order['備註']
    .replace('', np.nan)
    .fillna('')
    .astype(str)
    .str.strip()
)


def normalize_time(t: str) -> str | None:
    if not t:
        return None

    t = t.replace('：', ':').strip()

    if ':' in t:
        h, m = t.split(':', 1)

        if h.isdigit() and m.isdigit():
            h = int(h)
            m = int(m)

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

    m = re.search(
        r'指[:：]?\s*([0-9：:]{1,5})\s*[~～]\s*([0-9：:]{1,5})',
        note
    )
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


total_order[['最早派送時間', '最晚派送時間']] = (
    total_order['備註']
    .apply(lambda x: pd.Series(parse_delivery_window(x)))
)


def combine_date_time(d, t):
    if pd.isna(d) or not t:
        return pd.NaT
    try:
        return datetime.combine(
            d,
            datetime.strptime(t, '%H:%M').time()
        )
    except ValueError:
        return pd.NaT


total_order['最早派送datetime'] = total_order.apply(
    lambda r: combine_date_time(r['出貨日期'], r['最早派送時間']),
    axis=1
)

total_order['最晚派送datetime'] = total_order.apply(
    lambda r: combine_date_time(r['出貨日期'], r['最晚派送時間']),
    axis=1
)








df_clean = total_order.copy()

df_clean['收貨_lon'] = pd.to_numeric(df_clean['收貨_lon'], errors='coerce')
df_clean['收貨_lat'] = pd.to_numeric(df_clean['收貨_lat'], errors='coerce')

cols_to_clean = ['區域代碼', '出貨倉']
for col in cols_to_clean:
    df_clean[col] = df_clean[col].astype(str).str.strip()
    df_clean[col] = df_clean[col].replace({'nan': np.nan, 'None': np.nan, '': np.nan})

mask_geo_valid = (df_clean['收貨_lon'].notna()) & (df_clean['收貨_lat'].notna()) & \
                 (df_clean['收貨_lon'] != 0) & (df_clean['收貨_lat'] != 0)

mask_area_valid = df_clean['區域代碼'].notna()

df_final = df_clean[mask_geo_valid | mask_area_valid].copy()

print(f"原始筆數: {len(total_order)}")
print(f"清洗後筆數: {len(df_final)}")
print(f"剔除無效數據: {len(total_order) - len(df_final)} 筆")




df_mapping = df_final.dropna(subset=['區域代碼', '出貨倉'])

area_rules = df_mapping.groupby('區域代碼')['出貨倉'].agg(lambda x: x.mode()[0]).reset_index()
area_to_warehouse_map = area_rules.set_index('區域代碼')['出貨倉'].to_dict()

print("已建立區域歸屬規則表，共含", len(area_to_warehouse_map), "個區域規則。")




from sklearn.ensemble import RandomForestClassifier
train_data = df_final[mask_geo_valid].copy()
train_data = train_data.dropna(subset=['出貨倉', '區域代碼'])
X = train_data[['收貨_lon', '收貨_lat']]
y_log = train_data['出貨倉'].fillna(method='ffill') 
clf_warehouse = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
clf_warehouse.fit(X, y_log)

y_area = train_data['區域代碼'].fillna(method='ffill')
clf_area = RandomForestClassifier(n_estimators=100, random_state=42, n_jobs=-1)
clf_area.fit(X, y_area)
print("雙模型訓練完成！")


def smart_predict(lon, lat, threshold=0.75):

    coords = np.array([[lon, lat]])
    
    pred_wh = clf_warehouse.predict(coords)[0]
    probs = clf_warehouse.predict_proba(coords)
    conf_wh = np.max(probs) # 取得最高機率
    
    if conf_wh >= threshold:
        return {
            '出貨倉': pred_wh,
            '信心度': f"{conf_wh:.2%}",
            '決策來源': '主模型(高信心)',
            '輔助資訊': None
        }

    pred_area = clf_area.predict(coords)[0]
    conf_area = np.max(clf_area.predict_proba(coords))
    suggested_wh = area_to_warehouse_map.get(pred_area, None)
    
    decision = ""
    final_result = pred_wh
    
    if suggested_wh is None:
        decision = f"主模型信心低({conf_wh:.2%})且查無區域規則，維持原判"
    elif suggested_wh == pred_wh:
        decision = f"雙模型一致(主模型{conf_wh:.2%} + 區域推算)，結果可信"
    else:
        if conf_area > 0.8: 
            final_result = suggested_wh
            decision = f"採納區域規則(修正主模型)，因區域特徵明顯({pred_area})"
        else:
            decision = f"嚴重衝突且雙方信心皆低，建議人工審核 (主:{pred_wh} vs 區:{suggested_wh})"

    return {
        '出貨倉': final_result,
        '信心度': f"{conf_wh:.2%} (原始)",
        '決策來源': decision,
        '輔助資訊': f"預測區域:{pred_area}({conf_area:.2%}) -> 歸屬:{suggested_wh}"
    }





joblib.dump(clf_warehouse, 'model_warehouse.pkl')
joblib.dump(clf_area, 'model_area.pkl')
joblib.dump(area_to_warehouse_map, 'map_rules.pkl')

print("模型與規則已保存至硬碟！")