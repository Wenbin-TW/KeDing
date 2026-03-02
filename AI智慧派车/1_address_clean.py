
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


target_date = date.today() - timedelta(days=9999)
target_ts = pd.Timestamp(target_date)


total_order = kd.get_data_from_EWMS(f'''
    -- 1. 先計算好每張訂單的總重量
    WITH OrderWeight AS (
        SELECT
            od.ERP_NO,
            SUM(ISNULL(od.A_OUT_QUANTITY, 0) * ISNULL(mm.NETWEIGHT, 0)) AS 訂單總重量
        FROM [EWMS].[dbo].[SYS_SALES_ORDER_DETAILS] od
        LEFT JOIN [EWMS].[dbo].[SYS_MATERIAL_MASTER] mm ON od.MATNR = mm.MATNR
        WHERE LEN(od.MATNR) > 0
        GROUP BY od.ERP_NO
    )

    -- 2. 合併數據
    SELECT 
        er.erp_NO as 訂單編號, er.KUNNR as 客戶編號, er.SHORTNAME as 客戶簡稱,er.ADDRES sap地址, er.CORRECTED_ADDRESS 修改地址,
        er.LOGISTICS as 分區, er.TO_DRIVER as 備註,
        er.LONGITUDE as 收貨_lon, er.LATITUDE as 收貨_lat, er.SHIPMENTDATE as 出貨日期,
        'SYS_OUT_CONTACT' as 數據來源,
        CASE WHEN er.TYPE = '1' THEN '退貨' ELSE '賣貨' END as 訂單類型,
        NULL as 訂單總重量  -- SYS_OUT_CONTACT 全都為空
    FROM [EWMS].[dbo].[SYS_OUT_CONTACT] er
    WHERE er.SHIPMENTDATE >= '{target_date}' and er.KUNNR like '%TW%'

    UNION ALL

    SELECT 
        so.erp_NO as 訂單編號, so.KUNNR as 客戶編號, so.SHORTNAME as 客戶簡稱,so.ADDRES sap地址, so.CORRECTED_ADDRESS 修改地址,
        so.LOGISTICS as 分區, so.TO_DRIVER as 備註,
        so.LONGITUDE as 收貨_lon, so.LATITUDE as 收貨_lat, so.SHIP_DATE as 出貨日期,
        'SYS_SALES_ORDER' as 數據來源,
        '賣貨' as 訂單類型,
        w.訂單總重量 as 訂單總重量
    FROM [EWMS].[dbo].[SYS_SALES_ORDER] so
    LEFT JOIN OrderWeight w ON so.erp_NO = w.ERP_NO  
    WHERE so.SHIP_DATE >= '{target_date}' and so.KUNNR like '%TW%'
''')


out_df = total_order[["客戶編號", "sap地址", "修改地址"]].drop_duplicates()

out_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\客戶地址清單.csv")
out_df.to_csv(out_path, index=False, encoding="utf-8-sig")






from opencc import OpenCC
import pandas as pd
import re
from difflib import SequenceMatcher
converter = OpenCC('s2t')


def fix_english_style_address(addr):
    """
    修復 Google 回傳的倒裝地址 (Small-to-Big -> Big-to-Small)。
    解決方案：利用 Regex 貪婪匹配，從尾部精確提取行政區，避免路名沾黏。
    """
    if not isinstance(addr, str) or 'no.' not in addr.lower():
        return addr
        
    # 1. 提取門牌與前綴 (包含 No. 及其周邊的數字/樓層)
    # 使用非貪婪 (.*?) 抓取開頭到 No. 的前綴，確保不會吃掉後面的中文地址
    match_no = re.match(r'^(.*?No\.?\s*[0-9a-zA-Z\-\.之]+[號樓Ff]?)(.*)', addr, re.IGNORECASE)
    
    if not match_no:
        return addr
        
    prefix_number = match_no.group(1) # e.g. "16, No. 69號"
    rest_address = match_no.group(2)  # e.g. "光復路二段三重區新北市台灣 241"
    
    # 清洗門牌部分
    clean_number = re.sub(r'No\.?\s*', '', prefix_number, flags=re.IGNORECASE)
    clean_number = clean_number.replace(',', '').replace(' ', '').replace('号', '').replace('號', '')
    
    # 補回 "號" (若開頭是數字)
    if re.match(r'^\d', clean_number): 
         if not clean_number.endswith("號"):
            clean_number += "號"

    # 移除台灣與郵編，準備提取行政區
    rest_address = re.sub(r'台灣\s*\d*$', '', rest_address).strip()
    
    city = ""
    dist = ""
    
    # 2. 利用 (.*) 的貪婪特性，強迫匹配 "最後一個" 符合的行政區詞彙
    # 這能有效防止 "二段" 的 "段" 被誤認為行政區的一部分
    
    # 提取縣市 (City)
    match_city = re.match(r'(.*)([\u4e00-\u9fa5]{2}[縣市])$', rest_address)
    if match_city:
        city = match_city.group(2)
        rest_address = match_city.group(1).strip()
        
    # 提取鄉鎮市區 (District)
    match_dist = re.match(r'(.*)([\u4e00-\u9fa5]{2,3}[鄉鎮市區])$', rest_address)
    if match_dist:
        dist = match_dist.group(2)
        rest_address = match_dist.group(1).strip()

    # 剩下的就是路名
    road = rest_address.strip().replace(',', '').replace(' ', '')
    
    # 3. 重組為中文順序
    return f"{city}{dist}{road}{clean_number}"


def normalize_address(addr):
    if not isinstance(addr, str) or pd.isna(addr) or str(addr).lower() == 'nan':
        return ""
    
    # [Step 1] 優先執行倒裝修復 (處理 Google 回傳格式)
    addr = fix_english_style_address(addr)
    
    # [Step 2] 雜訊過濾
    # 移除括號標記與座標 (這部分在轉繁體前做，避免編碼影響 Regex)
    addr = re.sub(r'[(\uff08][專店][)\uff09]', '', addr)
    gps_pattern = r'(\d+[°\.]\d+)'
    has_gps = re.search(gps_pattern, addr) is not None
    addr = re.sub(r'\d+°\d+[\'\"]?\d*\.?\d*\"?[NS]\s*\d+°\d+[\'\"]?\d*\.?\d*\"?[EW]?', '', addr)
    addr = re.sub(r'\d+\.\d+\s*,\s*\d+\.\d+', '', addr)
    addr = re.sub(r'\d+\.\d+', '', addr) 

    if has_gps:
        addr = addr.replace('(', '').replace(')', '').replace('（', '').replace('）', '')
    else:
        addr = re.sub(r'\(.*?\)', '', addr)
        addr = re.sub(r'（.*?）', '', addr)

    # 移除業務雜訊 (維持原始錄入字元以確保命中)
    noise_keywords = [
        '工廠', '倉庫', '自宅裝修', '隔壁', '附近', '對面', '旁', 
        '待確認', '待建', '卸貨', '車位', '密碼', '進料', '施工', '洪文東','北區','南區','北区','南区',
        '管理室', '借鑰匙', '透天', '接待中心', '公設', '舊阿米哥', '盧總','西區','東區','西区','东区',
        '海運', '碼頭', '鐵皮屋', '樣品屋', '工地', '預售屋', '服務中心','自宅案'
    ]
    for kw in noise_keywords:
        addr = addr.replace(kw, '')

    # [Step 3] 結構化清洗
    section_map = {'一段': '1段', '二段': '2段', '三段': '3段', '四段': '4段', '五段': '5段', '六段': '6段', '七段': '7段', '八段': '8段', '九段': '9段', '十段': '10段'}
    for k, v in section_map.items():
        addr = addr.replace(k, v)

    addr = re.sub(r'No\.?', '', addr, flags=re.IGNORECASE)
    addr = re.sub(r'[0-9]+F', '', addr, flags=re.IGNORECASE) 
    addr = re.sub(r'[^\u4e00-\u9fa5a-zA-Z0-9]', '', addr)
    
    # 截斷邏輯：保留到號或樓為止
    match_floor = re.search(r'(.*?\d+樓(之\d+)?)', addr)
    if match_floor:
        addr = match_floor.group(1)
    else:
        match_no = re.search(r'(.*?\d+號(之\d+)?)', addr)
        if match_no:
            addr = match_no.group(1)

    addr = re.sub(r'^\d+', '', addr)
    addr = addr.replace("台灣省", "").replace("台灣", "")

    # [Step 4] 最後執行繁簡與異體字轉化
    # 此時 addr 已經是乾淨的地址核心資訊
    addr = converter.convert(addr.strip())
    addr = addr.replace("台", "臺")
    
    return addr

def get_similarity(s1, s2):
    if not s1 or not s2: return 0.0
    return SequenceMatcher(None, s1, s2).ratio()

# 載入資料
df = out_df.copy()

# 執行
df['sap_clean'] = df['sap地址'].apply(normalize_address)
df['mod_clean'] = df['修改地址'].apply(normalize_address)
df['similarity'] = df.apply(lambda x: get_similarity(str(x['sap_clean']), str(x['mod_clean'])), axis=1)

print(df['similarity'].describe())


# 分級標準
def categorize(score):
    if score >= 0.95: return '極高 (基本一致)'
    if score >= 0.85: return '高 (格式差異)'
    if score >= 0.75: return '中 (部分資訊遺失)'
    return '低 (潛在定位錯誤)'


df['match_level'] = df['similarity'].apply(categorize)

# 輸出統計摘要
print(df['match_level'].value_counts())

# 將需要覆核的低相似度地址存為獨立檔案

import pandas as pd
import re








def get_admin_info(addr):
    """
    精確提取縣與區，並排除「縣道」雜訊
    """
    if not addr:
        return {"county": "", "district": ""}
    
    # 1. 提取「縣」的核心字，但排除「縣道」
    # 使用負向先行斷言 (?!道)，確保「縣」後面接的不是「道」
    county_match = re.search(r'(.{2})縣(?!道)', addr)
    county = county_match.group(1) if county_match else ""
    
    # 2. 提取「區」的核心字
    district_match = re.search(r'(.{2})區', addr)
    district = district_match.group(1) if district_match else ""
    
    return {"county": county, "district": district}

def check_admin_consistency(row):
    addr_sap = row['sap_clean']
    addr_mod = row['mod_clean']
    
    info_sap = get_admin_info(addr_sap)
    info_mod = get_admin_info(addr_mod)

    # 縣級比對
    s_c = info_sap['county']
    m_c = info_mod['county']
    if s_c or m_c:
        # 兩邊都有標籤但內容不同
        if s_c and m_c and s_c != m_c:
            return f"縣不符：SAP【{s_c}縣】VS 谷歌【{m_c}縣】"
        # 只有一方有標籤，檢查另一方是否包含該核心字
        if s_c and s_c not in addr_mod:
            return f"縣不符：SAP【{s_c}縣】VS 谷歌【未標註或不符】"
        if m_c and m_c not in addr_sap:
            return f"縣不符：SAP【未標註或不符】VS 谷歌【{m_c}縣】"

    # 區級比對
    s_d = info_sap['district']
    m_d = info_mod['district']
    if s_d or m_d:
        # 兩邊都有標籤但內容不同
        if s_d and m_d and s_d != m_d:
            return f"區不符：SAP【{s_d}區】VS 谷歌【{m_d}區】"
        # 只有一方有標籤，檢查另一方是否包含該核心字
        if s_d and s_d not in addr_mod:
            return f"區不符：SAP【{s_d}區】VS 谷歌【未標註或不符】"
        if m_d and m_d not in addr_sap:
            return f"區不符：SAP【未標註或不符】VS 谷歌【{m_d}區】"

    return "匹配"
# --- 整合應用流程 ---

# 1. 執行基礎清洗 (使用先前優化過的順序：先清雜訊，最後轉繁體)
df['sap_clean'] = df['sap地址'].apply(normalize_address)
df['mod_clean'] = df['修改地址'].apply(normalize_address)

# 2. 執行行政區硬核校驗
df['admin_check'] = df.apply(check_admin_consistency, axis=1)

# 3. 計算字串相似度比對
df['similarity'] = df.apply(lambda x: get_similarity(x['sap_clean'], x['mod_clean']), axis=1)

# 4. 判定最終狀態
# 即使相似度高，行政區不符也視為需要覆核
def final_decision(row):
    if row['admin_check'] == "匹配":
        if row['similarity'] >= 0.95: return "自動通過"
        if row['similarity'] >= 0.85: return "建議通過"
    return "人工覆核"

df['final_status'] = df.apply(final_decision, axis=1)



df.to_excel('address_review_list.xlsx', index=False)
