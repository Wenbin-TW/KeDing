
import pytz
from datetime import datetime, timedelta
import sys
from sqlalchemy import create_engine, text
import pandas as pd
from dateutil.relativedelta import relativedelta
from typing import Optional
from pathlib import Path

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd


yeaterday = pytz.timezone('Asia/Taipei').localize(datetime.now() - timedelta(days=1))
last_month_str = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')


def map_label_contain(label_str: str):
    if pd.isna(label_str):
        return None
    
    label_str = str(label_str)

    if "開發客戶" in label_str:
        return 1
    elif "經營客戶" in label_str:
        return 2
    elif "開發中" in label_str:
        return 3
    elif "沉默客戶" in label_str:
        return 4
    elif "無" in label_str:
        return 5   
    elif "暫封存客戶" in label_str:
        return 6

    elif "开发客户" in label_str:
        return 1
    elif "开发中" in label_str:
        return 2
    elif "经营客户" in label_str:
        return 3
    elif "沉默客户" in label_str:
        return 4
    elif "无" in label_str:
        return 5
    else:
        return None


target_cust = kd.get_data_from_MSSQL('SELECT  [area] 地區 ,[company_id] 公司代號 ,[label]  FROM [bi_ready].[dbo].[target_account_tag]')

target_cust["目標客戶標籤"] = target_cust["label"].apply(lambda x: str(x).strip() + "客戶" if pd.notna(x) and str(x).strip() != "" else "")

TW_info = kd.get_data_from_CRM( f'''select id, accountCode__c 公司代號, dimDepart 所屬部門 , ownerId 所有人,customItem322__c 原始標籤 from account''',location = "TW")
ML_info = kd.get_data_from_CRM( f'''select id, accountCode__c 公司代號, dimDepart.departName 所屬部門 , ownerId.name 所有人,customItem324__c 原始標籤 from account''',location = "ML")
full_info = pd.concat([TW_info, ML_info], ignore_index=True)


merged_df = pd.merge(target_cust, full_info, on=["公司代號"], how="left")
merged_df['區域分類'] = merged_df['地區'].apply(lambda x: '台灣' if str(x).startswith('TW') else '大陸' if str(x).startswith('CN') else '海外')
merged_df['標籤代碼'] = merged_df['原始標籤'].apply(map_label_contain).astype('Int64')  

df_tw_overseas = merged_df[merged_df["區域分類"].isin(["台灣", "海外"])].copy()
df_tw_overseas['業務類型'] = '3370547862542831'
df_tw_overseas['公司代號(文本)'] = df_tw_overseas['公司代號'] 
df_tw_overseas["建檔日期"] = int(datetime.today().timestamp() * 1000)

df_tw_overseas["目標客戶標籤代號"] = df_tw_overseas["目標客戶標籤"].replace({"開發客戶": 1, "經營客戶": 2, "開發中客戶": 3, "沉默客戶": 4, "": 5, "暫封存客戶": 6})
df_tw_overseas["目標客戶標籤歷程代號"] = df_tw_overseas["目標客戶標籤"].replace({"經營客戶": 1, "開發客戶": 2, "開發中客戶": 3, "沉默客戶": 4, "": 5, "暫封存客戶": 6})

df_tw_overseas_tag = df_tw_overseas.copy()
df_tw_overseas = df_tw_overseas[['id', "公司代號", "目標客戶標籤", "業務類型", "所有人", "所屬部門", "公司代號(文本)", "建檔日期", "目標客戶標籤歷程代號",]]


df_china = merged_df[merged_df["區域分類"] == "大陸"].copy()
df_china = df_china.rename(columns={
    "公司代號": "公司代号","目標客戶標籤": "目标客户标签",
    "區域分類": "区域分类","所屬部門": "所属部门"})

df_china['业务类型'] = '默认业务类型'
df_china['公司代号(文本)'] = df_china['公司代号'] 

df_china["建档日期"] = datetime.today().date()

df_china["目标客户标签代号"] = df_china["目标客户标签"].replace({"開發客戶": 1, "開發中客戶": 2, "經營客戶": 3, "沉默客戶": 4, "": 5})
df_china["目标客户标签历程代号"] = df_china["目标客户标签"].replace({"開發客戶": 1, "開發中客戶": 2, "經營客戶": 3, "沉默客戶": 4, "": 5})
df_china_diff = df_china[df_china['標籤代碼'] != df_china['目标客户标签代号']]

df_china = df_china[[ "公司代号", "目标客户标签", "业务类型", "所有人", "所属部门", "公司代号(文本)", "建档日期"]]
df_china["目标客户标签"] = df_china["目标客户标签"].replace({"開發客戶": '开发客户', "開發中客戶":'开发中', "經營客戶": '经营客户', "沉默客戶": '沉默客户','':'无'})
df_china = df_china.rename(columns={"公司代号": "公司名称"})

df_tw_overseas_tag = df_tw_overseas_tag[['id', "目標客戶標籤代號"]].rename(columns={"目標客戶標籤代號": "customItem322__c"})


df = df_tw_overseas_tag.loc[:, ["id", "customItem322__c"]].copy()

df["id"] = df["id"].astype("string").str.strip().str.extract(r"(\d{16})")[0]
df = df.dropna(subset=["id"]).drop_duplicates("id", keep="last")

df["customItem322__c"] = pd.to_numeric(df["customItem322__c"], errors="coerce").astype("Int64")

bulk_id = kd.ask_bulk_id(operation="update", object_name="account") 
kd.insert_to_CRM(bulk_id, df)

df_china_diff = df_china_diff[['id', "目标客户标签代号"]].rename(columns={"目标客户标签代号": "customItem324__c"})
bulk_id = kd.ask_bulk_id(operation="update", object_name="account",location = "ML") 
kd.insert_to_CRM(bulk_id, df_china_diff, location="ML")


history_tw = df_tw_overseas.rename(columns={
    "公司代號": "customItem2__c",
    "目標客戶標籤歷程代號": "customItem5__c",
    "業務類型": "entityType",
    "所有人": "ownerId",
    "所屬部門": "dimDepart",
    "公司代號(文本)": "customItem8__c",
    "建檔日期": "customItem6__c",
    "id": "id"
})[["id", "customItem2__c", "customItem5__c", "entityType", "ownerId", "dimDepart", "customItem8__c", "customItem6__c"]]
history_tw['customItem1__c'] = history_tw['id'] 

ac_token = kd.get_access_token()
bulk_id = kd.ask_bulk_id(object_name="customEntity61__c") 
kd.insert_to_CRM(bulk_id, history_tw)


import pandas as pd
from pathlib import Path
from datetime import datetime
import sys, io
if getattr(sys.stdout, "buffer", None) is not None:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if getattr(sys.stderr, "buffer", None) is not None:
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import os
from pathlib import Path

UNC_ROOT = r"\\192.168.1.218\KD共用"
def to_unc(path_str: str) -> str:
    """把以 Z: 開頭的路徑穩定轉成 UNC；其餘維持原樣"""
    if not path_str:
        return path_str
    p = path_str.strip()

    p = p.replace("/", "\\")
    if p[:2].lower() == "z:":

        sub = p[2:].lstrip("\\/")
        return UNC_ROOT + "\\" + sub
    return p

def ensure_exists(path_str: str, must_exist=True):
    p = to_unc(path_str)
    if must_exist and not Path(p).exists():
        raise FileNotFoundError(f"檔案不存在：{p}")
    return p


chunk_size = 49000
base_dir   = Path(ensure_exists(r"Z:\18_各部門共享區\15_數據中心課\文斌\目標客戶標簽曆程"))
date_str   = datetime.today().strftime("%Y%m%d")
out_dir    = base_dir / f"標簽曆程_{date_str}"
out_dir.mkdir(parents=True, exist_ok=True)


for i, start in enumerate(range(0, len(df_china), chunk_size), 1):
    df_chunk = df_china.iloc[start:start+chunk_size]
    file = out_dir / f"大陸標簽曆程_{date_str}_{i}.xlsx"
    df_chunk.to_excel(file, index=False)