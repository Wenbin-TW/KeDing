
import os
import sys
import re
import json
import ast
import time
import random
import logging
import threading
from pathlib import Path
from datetime import datetime as dt, timedelta
from typing import List, Optional, Tuple, Dict, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import pyodbc
import requests
import win32com.client
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# 1. 環境配置與全域常數
load_dotenv()
DB_CONFIG = {
    "TW": {
        "host": os.getenv("DB_HOST", "192.168.1.119"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
    },
    "EWMS": {
        "host": os.getenv("DB_EWMS_HOST", "192.168.1.222"),
        "user": os.getenv("DB_EWMS_USER"),
        "password": os.getenv("DB_EWMS_PASSWORD"),
        "driver": os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server"),
    }
}

CREDENTIALS = {
    "WB": {
        "username": os.getenv("CRM_USER_WB"),
        "password": os.getenv("CRM_PWD_WB"),
    },
    "BI": {
        "username": os.getenv("CRM_USER_BI"),
        "password": os.getenv("CRM_PWD_BI"),
    },
    "ML_DEFAULT": {
        "username": os.getenv("CRM_USER_ML"),
        "password": os.getenv("CRM_PWD_ML"),
    }
}

CLIENT_ID = os.getenv("CRM_CLIENT_ID")
CLIENT_SECRET = os.getenv("CRM_CLIENT_SECRET")
CLIENT_ID_ML = os.getenv("CRM_ML_CLIENT_ID")
CLIENT_ML_SECRET = os.getenv("CRM_ML_CLIENT_SECRET")

LOGIN_URL_TW = "https://login-p10.xiaoshouyi.com/auc/oauth2/token"
LOGIN_URL_ML = "https://login.xiaoshouyi.com/auc/oauth2/token"
API_BASE_TW = "https://api-p10.xiaoshouyi.com/rest"
API_BASE_ML = "https://api-scrm.xiaoshouyi.com/rest"

WEBHOOK_JARVIS = os.getenv("WECHAT_WEBHOOK_JARVIS")


# 2. 網路與認證基礎建設
def get_retry_session(retries: int = 5, backoff_factor: float = 0.5) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("POST", "GET", "PUT", "DELETE")
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_access_token(account: str = "WB") -> str:
    cred = CREDENTIALS.get(account)
    if not cred:
        raise ValueError(f"Account '{account}' not configured in CREDENTIALS.")

    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "username": cred["username"],
        "password": cred["password"],
    }
    
    try:
        session = get_retry_session()
        resp = session.post(LOGIN_URL_TW, data=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except requests.RequestException as e:
        print(f"[Auth Error] Failed to get TW token: {e}")
        raise

def get_access_token_ml(account: str = "ML_DEFAULT") -> str:
    cred = CREDENTIALS.get(account, CREDENTIALS["ML_DEFAULT"])
    
    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID_ML,
        "client_secret": CLIENT_ML_SECRET,
        "redirect_uri": "https://api-scrm.xiaoshouyi.com/",
        "username": cred["username"],
        "password": cred["password"],
    }
    
    try:
        session = get_retry_session()
        resp = session.post(LOGIN_URL_ML, data=payload, timeout=10)
        resp.raise_for_status()
        return resp.json()["access_token"]
    except requests.RequestException as e:
        print(f"[Auth Error] Failed to get ML token: {e}")
        raise


# 3. CRM 數據操作 (Extract / Load)

def get_data_from_CRM(xoql_query: str, location: str = "TW", account: str = "WB") -> pd.DataFrame:
    if location == "TW":
        token = get_access_token(account)
        url = f"{API_BASE_TW}/data/v2.0/query/xoqlScroll"
    elif location == "ML":
        token = get_access_token_ml(account)
        url = f"{API_BASE_ML}/data/v2.0/query/xoqlScroll"
    else:
        raise ValueError("Location must be 'TW' or 'ML'")

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    select_match = re.search(r"select(.*?)from", xoql_query, re.DOTALL | re.IGNORECASE)
    alias_map = {}
    if select_match:
        fields_str = select_match.group(1)
        for field in fields_str.strip().split(','):
            parts = field.strip().split()
            if len(parts) >= 2:
                alias_map[parts[0]] = parts[-1]
            elif len(parts) == 1:
                alias_map[parts[0]] = parts[0]
    
    expected_cols = list(alias_map.values())
    session = get_retry_session()
    
    all_records = []
    query_locator = ""
    batch_count = 2000

    print(f"[CRM Query] Start fetching: {xoql_query[:50]}...")
    start_time = time.time()

    while True:
        payload = {
            "xoql": xoql_query,
            "batchCount": batch_count,
            "queryLocator": query_locator,
        }

        try:
            resp = session.post(url, headers=headers, data=payload, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"[CRM Query Error] Retrying... {e}")
            time.sleep(2)
            continue

        records = data.get("data", {}).get("records", [])
        if not records:
            break
            
        all_records.extend(records)
        
        query_locator = data.get("queryLocator", "")
        if not query_locator:
            break

    df = pd.DataFrame(all_records)
    
    if not df.empty:
        rename_dict = {k: v for k, v in alias_map.items() if k in df.columns}
        df = df.rename(columns=rename_dict)
        
        for col in expected_cols:
            if col not in df.columns:
                df[col] = None

    print(f"[CRM Query] Fetched {len(df)} rows in {time.time() - start_time:.2f}s.")
    return df

def ask_bulk_id(operation: str = "insert", object_name: str = "customEntity14__c", location: str = "TW") -> str:
    if location == "TW":
        token = get_access_token()
        url = f"{API_BASE_TW}/bulk/v2/job"
    else:
        token = get_access_token_ml()
        url = f"{API_BASE_ML}/bulk/v2/job"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    payload = {
        "data": {
            "operation": operation,
            "object": object_name
        }
    }
    if operation in ["insert", "delete"]:
        payload["data"]["execOption"] = ["CHECK_RULE", "CHECK_DUPLICATE"]

    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    job_id = resp.json()["result"]["id"]
    print(f"[Bulk API] Job Created: {job_id} ({operation} on {object_name})")
    return job_id

def insert_to_CRM(bulk_id: str, df: pd.DataFrame, location: str = "TW"):
    if df.empty:
        print("[Bulk API] DataFrame is empty, skipping.")
        return
    df = df.astype(str)
    
    batch_size = 5000
    total_batches = (len(df) + batch_size - 1) // batch_size
    
    if location == "TW":
        token = get_access_token()
        url = f"{API_BASE_TW}/bulk/v2/batch"
    else:
        token = get_access_token_ml()
        url = f"{API_BASE_ML}/bulk/v2/batch"

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    session = get_retry_session()

    print(f"[Bulk API] Uploading {len(df)} records in {total_batches} batches...")
    
    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        batch_data = df.iloc[start_idx:end_idx].to_dict(orient='records')
        
        payload = {
            "data": {
                "jobId": bulk_id,
                "datas": batch_data
            }
        }
        
        try:
            resp = session.post(url, headers=headers, json=payload, timeout=120)
            if resp.status_code == 200:
                print(f"  - Batch {i+1}/{total_batches}: Success")
            else:
                print(f"  - Batch {i+1}/{total_batches}: Failed - {resp.text}")
        except Exception as e:
            print(f"  - Batch {i+1}/{total_batches}: Error - {e}")


# 4. 資料庫操作 (MSSQL)

def get_db_engine(db_key: str = "TW", db_name: str = "raw_data"):
    conf = DB_CONFIG.get(db_key)
    if not conf:
        raise ValueError(f"DB Config '{db_key}' not found.")
        
    conn_str = (
        f"mssql+pyodbc://{conf['user']}:{conf['password']}@"
        f"{conf['host']}/{db_name}?driver={conf['driver'].replace(' ', '+')}"
    )
    return create_engine(conn_str, fast_executemany=True)

def get_data_from_MSSQL(sql_query: str, db_key: str = "TW", db_name: str = "raw_data") -> pd.DataFrame:
    engine = get_db_engine(db_key, db_name)
    try:
        with engine.connect() as conn:
            return pd.read_sql(sql_query, conn)
    except Exception as e:
        print(f"[DB Error] Query failed: {e}")
        return pd.DataFrame()

def write_to_sql(df: pd.DataFrame, db_name: str, table_name: str, if_exists: str = 'append', dedup_keys: List[str] = None):
    if df.empty:
        return

    engine = get_db_engine("TW", db_name)
    if if_exists in ['replace', 'append']:
        with engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)
        print(f"[DB Write] {if_exists} {len(df)} rows to {table_name}.")
        return
    if if_exists == 'update':
        if not dedup_keys:
            raise ValueError("Must provide 'dedup_keys' for update mode.")
        temp_table = f"{table_name}_TEMP_{int(time.time())}"
        
        with engine.begin() as conn:
            df.to_sql(name=temp_table, con=conn, if_exists='replace', index=False)
            join_condition = " AND ".join([f"Target.[{k}] = Source.[{k}]" for k in dedup_keys])
            update_cols = [c for c in df.columns if c not in dedup_keys]
            update_clause = ", ".join([f"Target.[{c}] = Source.[{c}]" for c in update_cols])
            
            insert_cols = ", ".join([f"[{c}]" for c in df.columns])
            insert_vals = ", ".join([f"Source.[{c}]" for c in df.columns])

            merge_sql = f"""
            MERGE INTO {table_name} AS Target
            USING {temp_table} AS Source
            ON {join_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals});
            """
            
            conn.execute(text(merge_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
            
        print(f"[DB Write] Merged (Upsert) {len(df)} rows into {table_name}.")


def get_data_from_EWMS(sql_query: str) -> pd.DataFrame:
    """
    從 EWMS (1.222) 數據庫讀取數據。
    """
    conf = DB_CONFIG["EWMS"]
    
    conn_str = (
        f"mssql+pyodbc://{conf['user']}:{conf['password']}@"
        f"{conf['host']}/EWMS?driver={conf['driver'].replace(' ', '+')}"
    )
    
    try:
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            df = pd.read_sql(sql_query, connection)
            return df
    except Exception as e:
        print(f"[EWMS Error] Query failed: {e}")
        return pd.DataFrame()



# 5. 業務邏輯與清洗 (Business Logic)
def clean_invalid_entries_MRK(df: pd.DataFrame
                            ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    依序套用各個『移除規則』（固定 12 步）：
      0. 公司型態等於 KD
      1. 特定區域群組排除 (TW-888 | TW-Z | TW-LB | TW-CPT | TW-PD)
      2. 公司簡稱包含「測試｜兔兔｜員邦｜峻佳」等關鍵字
      3. 職務類別過濾（僅保留 001~007、010、011、015 類別）
      4. 聯絡人勿擾選項（包含「勿電訪」）
      5. 聯絡人資料無效（值為「否」才保留）
      6. 聯絡人姓名包含「過世／退休／往生／離世／歿／逝世／去世」者剔除
      7. 公司與聯絡人電話皆為空（原為空值）
      8. 聯絡人電話無效，且公司電話也無效
      9. 聯絡人離職，且手機號碼非 09 開頭（但手機有值）
      10. 審批狀態未通過（僅保留審批狀態含「审批通过」者）
      11. 公司電話無效 且 手機號為空
    """

    df = df.copy()
    n0 = len(df)

    removed_so_far = pd.Series(False, index=df.index)
    first_reason   = pd.Series('', index=df.index)
    rows = []

    def _s(col, default=''):
        return df.get(col, default).astype(str).str.strip().replace('nan', '')

    #  0~2 公司層面 
    m_kd_del  = _s('公司型態').str.fullmatch(r'KD', na=False)
    m_grp_del     = _s('資料區域群組名稱').str.contains(r'TW-888|TW-Z|TW-LB|TW-CPT|TW-PD', na=False)
    m_compkw_del  = _s('公司簡稱').str.contains(r'測試|兔兔|員邦|峻佳', na=False)

    #  3~6 聯絡人層面 
    m_role_del        = ~_s('職務類別').str.contains(r'001|002|003|004|005|006|007|010|011|015', na=False)
    m_contact_dnc_del = _s('聯絡人勿擾選項').str.contains('勿電訪', na=False)
    m_invalid_del     = ~_s('連絡人資料無效').str.contains('否', na=False)
    m_name_del        = _s('連絡人').str.contains(r'過世|退休|往生|離世|歿|逝世|去世', na=False)

    #  7 電話皆空 
    phone_series = _s('手機號碼')
    comp_series  = _s('公司電話')

    phone_empty = phone_series == ''
    comp_empty  = comp_series == ''
    m_both_empty = phone_empty & comp_empty

    valid_format = phone_series.str.match(r'^09\d{8}$', na=False)
    phone_has_value = ~phone_empty

    #  9 聯絡人離職且手機非09 
    m_leave_and_badphone = (
        _s('關係狀態').str.contains('離職', na=False)
        & phone_has_value
        & ~valid_format
    )

    #  聯絡人電話無效 
    m_phone_invalid = (
        (df.get('空號', 0).fillna(0).astype(int) == 1) |
        (df.get('停機', 0).fillna(0).astype(int) == 1) |
        (_s('號碼錯誤非本人').isin(["['是']", '是'])) |
        (~valid_format)
    )

    #  公司電話無效 
    comp_is_dnc        = _s('公司勿擾選項').str.contains('勿電訪', na=False)
    comp_only_zero     = comp_series.str.fullmatch(r'0+', na=False)
    comp_only_hyphen   = comp_series.str.fullmatch(r'[-\s]+', na=False)
    comp_has_text      = comp_series.str.contains(r'[A-Za-z\u4e00-\u9fff]', na=False)
    comp_closed        = _s('倒閉').str.contains('是', na=False)
    comp_group_inv     = _s('資料區域群組名稱').str.contains('INV', case=False, na=False)

    m_company_invalid = (
        comp_empty | comp_is_dnc | comp_only_zero | comp_only_hyphen |
        comp_has_text | comp_closed | comp_group_inv
    )

    #  8 聯絡人電話無效且公司電話也無效 
    m_phone_invalid_and_company_invalid = (
        phone_has_value & m_phone_invalid & m_company_invalid
    )

    #  10 審批未通過 
    m_not_approved = ~_s('審批狀態').str.contains('审批通过', na=False)

    #  11 公司電話無效且手機為空 
    m_company_invalid_and_phone_empty = m_company_invalid & phone_empty

    #  全部規則集中（0–11 完全對齊） 
    steps = [
        ('公司層面', '0.公司型態等於 KD', m_kd_del),
        ('公司層面', '1.特定區域群組排除(TW-888|TW-Z|TW-LB|TW-CPT|TW-PD)', m_grp_del),
        ('公司層面', '2.公司簡稱包含「測試|兔兔|員邦|峻佳」', m_compkw_del),
        ('聯絡人層面', '3.職務類別過濾', m_role_del),
        ('聯絡人層面', '4.聯絡人勿擾選項', m_contact_dnc_del),
        ('聯絡人層面', '5.聯絡人資料無效（值為「否」才保留）', m_invalid_del),
        ('聯絡人層面', '6.聯絡人姓名剔除（死亡/退休）', m_name_del),
        ('聯絡人層面', '7.公司與聯絡人電話皆為空', m_both_empty),
        ('聯絡人層面', '8.聯絡人電話無效，且公司電話也無效', m_phone_invalid_and_company_invalid),
        ('聯絡人層面', '9.聯絡人離職，且手機號碼非09開頭', m_leave_and_badphone),
        ('聯絡人層面', '10.審批狀態未通過', m_not_approved),
        ('聯絡人層面', '11.公司電話無效 且 手機號為空', m_company_invalid_and_phone_empty),
    ]

    remain = n0
    for layer, rule, mask in steps:
        newly_hit = mask & ~removed_so_far
        idx = newly_hit & (first_reason == '')
        first_reason.loc[idx] = rule

        removed_so_far |= mask
        remain -= int(newly_hit.sum())

        rows.append({
            '層級': layer,
            '規則': rule,
            '本步驟新剔除筆數': int(newly_hit.sum()),
            '剔除後剩餘筆數': remain
        })

    summary = pd.DataFrame(rows, index=range(1, len(rows) + 1))
    removed_df = df[removed_so_far].copy().reset_index(drop=True)
    cleaned_df = df[~removed_so_far].copy().reset_index(drop=True)

    if not removed_df.empty:
        removed_df['剔除原因'] = first_reason[removed_so_far].reset_index(drop=True)

    return summary, cleaned_df, removed_df



# 6. 其他工具

def 賈維斯1號(text: str):
    """
    發送企業微信機器人通知 (Jarvis)
    """
    if not WEBHOOK_JARVIS:
        print("[Webhook] No URL configured, skipping notification.")
        return
        
    payload = {
        "msgtype": "text",
        "text": {
            "content": f"【系統通知】{dt.now().strftime('%Y-%m-%d %H:%M')}\n{text}"
        }
    }
    try:
        requests.post(WEBHOOK_JARVIS, json=payload, timeout=5)
    except Exception as e:
        print(f"[Webhook Error] {e}")


def convert_to_date(df, column_name, new_column_name=None):
    col = new_column_name or column_name
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    df.loc[df[column_name] == 0, column_name] = pd.NA
    df[col] = pd.to_datetime(df[column_name], unit='ms', errors='coerce', utc=True)\
                 .dt.tz_convert('Asia/Taipei')\
                 .dt.strftime('%Y-%m-%d')
    return df




def convert_to_datetime(df, column_name, new_column_name=None):
    col = new_column_name or column_name
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
    df.loc[df[column_name] == 0, column_name] = pd.NA
    df[col] = pd.to_datetime(df[column_name], unit='ms', errors='coerce', utc=True)\
                 .dt.tz_convert('Asia/Taipei')\
                 .dt.strftime('%Y-%m-%d %H:%M:%S')
    return df




def dump_terminal_to_file(log_dir="logs", filename="run_log", add_date=True):
    """
    重定向 stdout/stderr 到日誌檔案，同時保留終端輸出。
    """
    class LoggerWriter:
        def __init__(self, terminal, logfile):
            self.terminal = terminal
            self.logfile = logfile
        def write(self, message):
            self.terminal.write(message)
            self.logfile.write(message)
        def flush(self):
            self.terminal.flush()
            self.logfile.flush()

    os.makedirs(log_dir, exist_ok=True)
    suffix = f"_{dt.now().strftime('%Y%m%d')}" if add_date else ""
    log_path = os.path.join(log_dir, f"{filename}{suffix}.txt")
    
    f = open(log_path, "a", encoding="utf-8")
    sys.stdout = LoggerWriter(sys.stdout, f)
    sys.stderr = LoggerWriter(sys.stderr, f)
    print(f"\n[Logger] Output redirected to {log_path}")








def process_waiting_list(waiting_list: pd.DataFrame,
                         sys_kicked: pd.DataFrame,
                         six_months_ago,
                         today_driver: pd.DataFrame,
                         gift_df: pd.DataFrame,
                         waiting_list2: pd.DataFrame):

    waiting_list = waiting_list.copy()
    waiting_list["剔除原因"] = ""

    # 建立 mapping：主要客關連 → 多筆交辦編號（逗號串）
    case_map = (sys_kicked.groupby("客戶關係連絡人")["交辦編號"]
                .apply(lambda x: ",".join(str(i) for i in x.unique()))
                .to_dict())

    # 工具函式：新增剔除原因（可累加）
    def add_reason(df, condition, reason):
        idx = df.index[condition]
        df.loc[idx, "剔除原因"] = df.loc[idx, "剔除原因"].apply(
            lambda x: reason if x == "" else (x if reason in x else f"{x}，{reason}")
        )

    # 規則 1：間接客戶
    add_reason(waiting_list,
               waiting_list["公司公用標籤"].str.contains("間接", na=False),
               "間接客戶")

    # 規則 2：主要聯絡人離職
    add_reason(waiting_list,
               waiting_list["主要客關連關係狀態"].str.contains("離職", na=False),
               "主要客關連離職")

    # 規則 3：主要聯絡人為空
    add_reason(waiting_list,
               waiting_list["customItem42__c"].isna(),
               "主要客關連為空")

    # 規則 4：半年內有交易
    waiting_list["同關聯公司近3年最近發貨日期"] = pd.to_datetime(
        waiting_list["同關聯公司近3年最近發貨日期"], errors="coerce"
    )
    six_months_ago = pd.to_datetime(six_months_ago)

    cond_no_transaction = (
        waiting_list["同關聯公司近3年最近發貨日期"] >= six_months_ago
    )
    add_reason(waiting_list, cond_no_transaction, "半年內有交易")

    # 規則 5：主要聯絡人在 sys_kicked 名單中
    waiting_list["temp_case_list"] = waiting_list["主要客關連"].map(case_map)

    def append_case_ids(row):
        if pd.isna(row["temp_case_list"]):
            return row["剔除原因"]
        case_str = f"交辦編號：{row['temp_case_list']}"
        if row["剔除原因"] == "":
            return case_str
        return f"{row['剔除原因']}，{case_str}"

    waiting_list["剔除原因"] = waiting_list.apply(append_case_ids, axis=1)
    waiting_list = waiting_list.drop(columns=["temp_case_list"])


    # 規則 6：主要客關連在今日司機名單中
    driver_contacts = today_driver["客戶關係連絡人"].astype(str).unique()
    add_reason(waiting_list,
               waiting_list["主要客關連"].astype(str).isin(driver_contacts),
               "主要客關連在今日司機名單中")

    # 規則 7：主要客關連在今日派樣名單中
    gift_contacts = gift_df["客戶關係連絡人"].astype(str).unique()
    add_reason(waiting_list,
               waiting_list["主要客關連"].astype(str).isin(gift_contacts),
               "主要客關連在今日派樣名單中")


    # 規則 8：主要客關連在今日14天前拒K名單中
    waiting_list2 = waiting_list2["客戶關係連絡人"].astype(str).unique()
    add_reason(waiting_list,
               waiting_list["主要客關連"].astype(str).isin(gift_contacts),
               "主要客關連在14天前拒K名單中")
    
    # 最終輸出
    df_removed = waiting_list[waiting_list["剔除原因"] != ""].copy()
    df_kept    = waiting_list[waiting_list["剔除原因"] == ""].sort_values(
                    by="同關聯公司近3年最近發貨日期", na_position="last")
    df_all     = waiting_list.sort_values(
                    by="同關聯公司近3年最近發貨日期", na_position="last")

    return df_removed, df_kept, df_all






def apply_contact_reasons(df, sys_kicked, today_driver, gift_df):

    df = df.copy()

    if "去除原因" not in df.columns:
        df["去除原因"] = ""
    else:
        df["去除原因"] = df["去除原因"].fillna("").astype(str)

    kicked_set = set(sys_kicked["客戶關係連絡人"].astype(str))
    driver_set = set(today_driver["客戶關係連絡人"].astype(str))
    gift_set   = set(gift_df["客戶關係連絡人"].astype(str))

    def add_reason(current, new):
        if not current:
            return new
        if new in current:
            return current
        return f"{current}，{new}"

    def process_row(row):
        contact = str(row["客戶關係連絡人"])
        reason = row["去除原因"]

        if contact in kicked_set:
            reason = add_reason(reason, "3月內已完成")

        if contact in driver_set:
            reason = add_reason(reason, "客關連在今日司機名單中")

        if contact in gift_set:
            reason = add_reason(reason, "客關連在今日派樣名單中")

        return reason

    df["去除原因"] = df.apply(process_row, axis=1)
    return df









def screen_by_exclusion_sources(df: pd.DataFrame, 承攬_target: str) -> tuple[pd.DataFrame, pd.DataFrame]:

    df_work = df.copy()
    summary = []

    # 定義剔除原因的順序與對應名稱
    exclusion_keywords = [
        '近三個月K大 / 已預約或已到訪(且上線≥8分)',
        'C池名單-暫不觸達',
        '寄後電訪名單-暫不觸達',
        '近3個月外勤成功拜訪',
        '近3個月展館到訪 / 或已預約參觀',
        '兩個月內拒K（公司型態=C/DC/DD）',
        '兩個月內拒K（公司型態≠C/DC/DD）',
        '已有「等待回應」交辦',
        '近3天有K大未完成(退休)任務',
        '派給個人CRM',
        '派給GC',
        f'派給承攬{承攬_target}'  # 動態變數
    ]

    # 階段 0：起始
    summary.append({
        '階段': 0,
        '起始': '手機號去重',
        '剔除': '',
        '剩餘': len(df_work),
        '操作步驟': '所有數據'
    })

    step = 1
    for reason in exclusion_keywords:
        before = len(df_work)
        mask = df_work['剔除原因'].astype(str).str.contains(reason, na=False, regex=False)
        df_work = df_work[~mask].copy()
        removed = before - len(df_work)

        summary.append({
            '階段': step,
            '起始': reason,
            '剔除': removed,
            '剩餘': len(df_work),
            '操作步驟': f'剔除含「{reason}」的客戶'
        })
        step += 1

    #  最後一步：統計會被觸達的名單 
    if '是否會被觸達' in df_work.columns:
        before = len(df_work)
        touched_df = df_work[df_work['是否會被觸達'] == True].copy()
        remaining = len(touched_df)
        removed = before - remaining

        summary.append({
            '階段': step,
            '起始': '無手機號或該公司今日已觸達',
            '剔除': removed,
            '剩餘': remaining,
            '操作步驟': '篩選會被觸達名單'
        })

        # 更新 df_work 為真正「今日觸達名單」
        df_work = touched_df
    else:
        summary.append({
            '階段': step,
            '起始': '無手機號或該公司今日已觸達',
            '剔除': '',
            '剩餘': len(df_work),
            '操作步驟': '欄位不存在，跳過'
        })

    #  組合結果 
    summary_df = pd.DataFrame(summary, columns=['階段', '起始', '剔除', '剩餘', '操作步驟'])
    return df_work.reset_index(drop=True), summary_df








API_BASE = "https://api-p10.xiaoshouyi.com/rest/data/v2.0"
DELEGATE_USER_ID = "2544824133310816"  # 固定代理人 ID

def build_session_with_token(token: str, method="GET"):
    sess = requests.Session()
    sess.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=(method,)
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    return sess

def get_delegate_token(delegate_user_id: str, base_token: str) -> str:
    url = f"{API_BASE}/oauth/token/actions/getDelegateToken"
    h = {"Authorization": f"Bearer {base_token}", "Content-Type": "application/json"}
    resp = requests.get(url, headers=h, params={"delegateUserId": delegate_user_id}, timeout=30)
    resp.raise_for_status()
    j = resp.json()
    tok = (j.get("result") or {}).get("access_token")
    if not tok:
        raise RuntimeError(f"無法取得代理 token：{str(j)[:300]}")
    return tok

def withdraw_with_delegate(df: pd.DataFrame, get_access_token_fn, *, max_workers=6):
    """
    使用固定代理 ID 撤回交辦。
    df: 必須含有 'id'
    get_access_token_fn: 例如 kd.get_access_token
    """
    assert "id" in df.columns, "DataFrame 必須包含欄位 'id'"

    base_token = get_access_token_fn()
    delegate_token = get_delegate_token(DELEGATE_USER_ID, base_token)
    sess_get = build_session_with_token(delegate_token, method="GET")
    sess_post = build_session_with_token(delegate_token, method="POST")

    results = []
    lock = threading.Lock()
    counter = {"done": 0, "total": len(df)}

    def fetch_proc_inst(row):
        data_id = row["id"]
        url = f"{API_BASE}/creekflow/history/filter"
        params = {"entityApiKey": "customEntity14__c", "dataId": data_id, "stageFlg": "false"}
        try:
            resp = sess_get.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            proc_id = data[-1]["procInstId"] if data else None
        except Exception as e:
            proc_id = None
            print(f" 取 procInstId 失敗 id={data_id} | {e}")
        with lock:
            counter["done"] += 1
            print(f"🔹 {counter['done']}/{counter['total']} | id={data_id} | procInstId={proc_id}")
            results.append({"dataId": data_id, "procInstId": proc_id})

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        pool.map(fetch_proc_inst, (r for _, r in df.iterrows()))

    proc_df = pd.DataFrame(results)

    # 確保欄位存在 (避免 KeyError)
    if "procInstId" not in proc_df.columns:
        proc_df["procInstId"] = None

    valid = proc_df[proc_df["procInstId"].notna()].copy()

    if valid.empty:
        print(" 沒有可撤回的任務，流程將繼續。")
        return pd.DataFrame({
            "dataId": pd.Series(dtype=object),
            "procInstId": pd.Series(dtype=object),
            "code": pd.Series(dtype=object),
            "msg": pd.Series(dtype=object)
        })

    #  Step 3. 批量撤回 
    url = f"{API_BASE}/creekflow/task"
    counter2 = {"done": 0, "total": len(valid)}
    withdraw_results = []

    def withdraw_one(row):
        payload = {
            "data": {
                "action": "withdraw",
                "entityApiKey": "customEntity14__c",
                "dataId": row["dataId"],
                "procInstId": row["procInstId"],
            }
        }
        try:
            resp = sess_post.post(url, json=payload, timeout=60)
            j = resp.json()
            code, msg = j.get("code", resp.status_code), j.get("msg", "OK")
        except Exception as e:
            code, msg = None, str(e)
        with lock:
            counter2["done"] += 1
            print(f" {counter2['done']}/{counter2['total']} | [{code}] {msg} | id={row['dataId']}")
            withdraw_results.append({
                "dataId": row["dataId"],
                "procInstId": row["procInstId"],
                "code": code,
                "msg": msg
            })

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        pool.map(withdraw_one, (r for _, r in valid.iterrows()))

    return pd.DataFrame(withdraw_results)



def stringify_lists(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    return df
        





def get_outlook_excel(since_time: dt, keyword: str, sheet_name: str) -> Optional[pd.DataFrame]:
    """
    從 Outlook 中取得指定主題關鍵字與時間條件的郵件，
    儲存其中的 Excel 附件，並讀取指定 Sheet,回傳 DataFrame。

    參數:
        since_time: datetime-含時區,郵件起始時間
        keyword: str,主旨關鍵字
        sheet_name: str,欲讀取的 Excel Sheet 名稱

    回傳:
        pd.DataFrame 或 None
    """
    save_path = r"C:\Temp\Outlook_Attachments"
    os.makedirs(save_path, exist_ok=True)

    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder(6)
    messages = inbox.Items
    messages.Sort("[ReceivedTime]", True)

    for message in messages:
        try:
            received_time = message.ReceivedTime
            if received_time >= since_time and keyword.lower() in message.Subject.lower():
                sender_name = message.SenderName
                subject = message.Subject
                sent_time = received_time.strftime('%Y-%m-%d %H:%M:%S')
                attachments = message.Attachments

                print(f"主題: {subject}")
                print(f"寄件人: {sender_name}")
                print(f"發送時間: {sent_time}")
                print("附件清單與 Sheet 名稱：")

                for i in range(1, attachments.Count + 1):
                    attachment = attachments.Item(i)
                    filename = attachment.FileName

                    if filename.endswith((".xlsx", ".xls")):
                        full_path = os.path.join(save_path, filename)
                        attachment.SaveAsFile(full_path)

                        try:
                            xls = pd.ExcelFile(full_path)
                            sheet_names = xls.sheet_names
                            print(f"- 附件檔案：{full_path}")
                            print(f"  Sheet 名稱：{sheet_names}")

                            df = pd.read_excel(full_path, sheet_name=sheet_name)
                            print(df.head())
                            return df  # 回傳 DataFrame

                        except Exception as e:
                            print(f"  無法讀取 Excel: {e}")
                return None  # 找到郵件但沒有 Excel 附件
        except Exception as e:
            print(f"錯誤處理郵件：{e}")
            continue

    print("找不到符合條件的郵件或 Excel 附件")
    return None









def submit_to_crm_tw(sql: str,
                  *,
                  max_workers: int       = 16,
                  attempts_per_id: int   = 4,
                  base_backoff: float    = 0.5,
                  loop_max: int          = 10,
                  show_progress: bool    = True,
                  sleep_between_loop: int = 10):
    """
    sql               : 查詢「待提交」清單的 XOQL / SQL
    max_workers       : 同時送審的執行緒數
    attempts_per_id   : 每筆最多重試幾次
    base_backoff      : 指數退避起始秒數
    loop_max          : 查 → 送 → 再查 的最大輪數
    show_progress     : 是否列印進度
    sleep_between_loop: 本輪仍有失敗時，暫停 N 秒再查
    """
    STATUS_URL = ("https://api-p10.xiaoshouyi.com/rest/data/v2.0"
                  "/creekflow/task/actions/preProcessor")
    TASK_URL   = ("https://api-p10.xiaoshouyi.com/rest/data/v2.0"
                  "/creekflow/task")

    def build_session() -> requests.Session:
        sess = requests.Session()
        sess.headers.update({
            "Authorization": f"Bearer {get_access_token()}",
            "Content-Type": "application/json"
        })
        retry = Retry(
            total=5, backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",)
        )
        sess.mount("https://", HTTPAdapter(max_retries=retry))

        # 綁定刷新方法
        sess.refresh_token = lambda: sess.headers.update(
            {"Authorization": f"Bearer {get_access_token()}"})
        return sess

    def _post(sess: requests.Session, url: str, data: dict):
        for _ in (0, 1):               # 最多兩次：第二次換 token
            r = sess.post(url, json=data, timeout=60)
            if r.status_code == 401:
                sess.refresh_token()
                continue
            r.raise_for_status()
            return r
        raise RuntimeError("Unauthorized even after token refresh")

    def submit_one(data_id: str,
                   sess: requests.Session,
                   counter: dict,
                   lock: threading.Lock):

        t0 = time.time()
        last_err = ""
        for attempt in range(1, attempts_per_id + 1):
            try:
                payload_status = {"data": {"action": "submit",
                                           "entityApiKey": "customEntity14__c",
                                           "dataId": data_id}}
                approval = _post(sess, STATUS_URL, payload_status).json().get("data")
                if not approval:
                    raise RuntimeError("approval is None")

                approvers = [u["id"] for u in approval.get("chooseApprover", [])]
                if not approvers:
                    raise RuntimeError("no approvers returned")

                payload_submit = {"data": {
                    "action": "submit",
                    "entityApiKey": "customEntity14__c",
                    "dataId": data_id,
                    "procdefId": approval["procdefId"],
                    "nextTaskDefKey": approval["nextTaskDefKey"],
                    "nextAssignees": approvers,
                    "ccs": approvers}}
                res = _post(sess, TASK_URL, payload_submit).json()
                if str(res.get("code")) != "200":
                    raise RuntimeError(f"CRM code {res.get('code')}")

                elapsed = time.time() - t0
                _progress(lock, counter, True, elapsed, data_id, "")
                return True
            except Exception as e:
                last_err = str(e)
                if attempt < attempts_per_id:
                    time.sleep(base_backoff * (2 ** (attempt - 1))
                               * random.uniform(0.8, 1.2))
                else:
                    elapsed = time.time() - t0
                    _progress(lock, counter, False, elapsed, data_id, last_err)
        return False

    def _progress(lock: threading.Lock, counter: dict,
                  success: bool, elapsed: float,
                  data_id: str, err: str):
        if not show_progress:
            return
        with lock:
            counter["done"] += 1
            total = counter["total"]
            status = "OK" if success else "FAIL"
            msg = (f"{counter['done']}/{total} | {status} | "
                   f"{elapsed:.2f}s | id={data_id}")
            if err and not success:
                msg += f" | {err}"
            print(msg, flush=True)

    all_failed = []
    for loop_idx in range(1, loop_max + 1):
        df = get_data_from_CRM(sql)
        if df.empty:
            print("No pending records, exit.")
            break

        if "approvalStatus" in df.columns:
            df = df[df["approvalStatus"].astype(str).str.contains(r"待提交|撤回")]
        ids = df["id"].tolist()
        if not ids:
            print("No matched approvalStatus, exit.")
            break

        print(f"\nRound {loop_idx}: {len(ids)} records")

        sess   = build_session()
        lock   = threading.Lock()
        counter = {"done": 0, "total": len(ids)}

        failed = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            fut = {pool.submit(submit_one, did, sess, counter, lock): did
                   for did in ids}
            for f in as_completed(fut):
                if not f.result():
                    failed.append(fut[f])

        if failed:
            print(f"Round {loop_idx} finished. Failed: {len(failed)}")
            all_failed.extend(failed)
            time.sleep(sleep_between_loop)
        else:
            continue        # 再查一次

    if all_failed:
        Path("failed_ids.txt").write_text("\n".join(map(str, all_failed)),
                                          encoding="utf-8")
        print(f"\nTotal failed: {len(all_failed)} (see failed_ids.txt)")
    else:
        print("\nAll records processed successfully.")













def submit_df_to_crm_tw(df: pd.DataFrame,
                        *,
                        max_workers: int       = 16,
                        attempts_per_id: int   = 4,
                        base_backoff: float    = 0.5,
                        loop_max: int          = 10,
                        show_progress: bool    = True,
                        sleep_between_loop: int = 10):
    STATUS_URL = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"
    TASK_URL   = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"

    def build_session() -> requests.Session:
        sess = requests.Session()
        sess.headers.update({
            "Authorization": f"Bearer {get_access_token()}",
            "Content-Type": "application/json"
        })
        retry = Retry(
            total=5, backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("POST",)
        )
        sess.mount("https://", HTTPAdapter(max_retries=retry))
        sess.refresh_token = lambda: sess.headers.update(
            {"Authorization": f"Bearer {get_access_token()}"})
        return sess

    def _post(sess: requests.Session, url: str, data: dict):
        for _ in (0, 1):
            r = sess.post(url, json=data, timeout=60)
            if r.status_code == 401:
                sess.refresh_token()
                continue
            r.raise_for_status()
            return r
        raise RuntimeError("Unauthorized even after token refresh")

    def submit_one(data_id: str,
                   sess: requests.Session,
                   counter: dict,
                   lock: threading.Lock):
        t0 = time.time()
        last_err = ""
        for attempt in range(1, attempts_per_id + 1):
            try:
                payload_status = {"data": {"action": "submit",
                                           "entityApiKey": "customEntity14__c",
                                           "dataId": data_id}}
                approval = _post(sess, STATUS_URL, payload_status).json().get("data")
                if not approval:
                    raise RuntimeError("approval is None")

                approvers = [u["id"] for u in approval.get("chooseApprover", [])]
                if not approvers:
                    raise RuntimeError("no approvers returned")

                payload_submit = {"data": {
                    "action": "submit",
                    "entityApiKey": "customEntity14__c",
                    "dataId": data_id,
                    "procdefId": approval["procdefId"],
                    "nextTaskDefKey": approval["nextTaskDefKey"],
                    "nextAssignees": approvers,
                    "ccs": approvers}}
                res = _post(sess, TASK_URL, payload_submit).json()
                if str(res.get("code")) != "200":
                    raise RuntimeError(f"CRM code {res.get('code')}")

                elapsed = time.time() - t0
                _progress(lock, counter, True, elapsed, data_id, "")
                return True
            except Exception as e:
                last_err = str(e)
                if attempt < attempts_per_id:
                    time.sleep(base_backoff * (2 ** (attempt - 1))
                               * random.uniform(0.8, 1.2))
                else:
                    elapsed = time.time() - t0
                    _progress(lock, counter, False, elapsed, data_id, last_err)
        return False

    def _progress(lock: threading.Lock, counter: dict,
                  success: bool, elapsed: float,
                  data_id: str, err: str):
        if not show_progress:
            return
        with lock:
            counter["done"] += 1
            total = counter["total"]
            status = "OK" if success else "FAIL"
            msg = (f"{counter['done']}/{total} | {status} | "
                   f"{elapsed:.2f}s | id={data_id}")
            if err and not success:
                msg += f" | {err}"
            print(msg, flush=True)

    all_failed = []
    submitted_ids = set()

    for loop_idx in range(1, loop_max + 1):
        if df.empty:
            print("No data, exit.")
            break

        df_pending = df[df["approvalStatus"].astype(str).str.contains(r"待提交|撤回")]
        df_pending = df_pending[~df_pending["id"].isin(submitted_ids)]
        ids = df_pending["id"].tolist()

        if not ids:
            print("No more new records to submit, exit.")
            break

        print(f"\nRound {loop_idx}: {len(df_pending)} records to process "
              f"(skipped: {len(submitted_ids)} already successful)")

        sess    = build_session()
        lock    = threading.Lock()
        counter = {"done": 0, "total": len(ids)}
        failed  = []

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            fut = {pool.submit(submit_one, did, sess, counter, lock): did for did in ids}
            for f in as_completed(fut):
                data_id = fut[f]
                if f.result():
                    submitted_ids.add(data_id)
                else:
                    failed.append(data_id)

        if failed:
            print(f"Round {loop_idx} finished. Failed: {len(failed)}")
            all_failed.extend(failed)
            time.sleep(sleep_between_loop)
        else:
            continue

    if all_failed:
        Path("failed_ids.txt").write_text("\n".join(map(str, all_failed)), encoding="utf-8")
        print(f"\nTotal failed: {len(all_failed)} (see failed_ids.txt)")
    else:
        print("\nAll records processed successfully.")










def delete_from_CRM(Tasks_df2, batch_size=5000, object_name="customEntity14__c", location="TW"):

    bulk_id = ask_bulk_id(operation="delete", object_name=object_name, location=location)
    Tasks_df3 = Tasks_df2[['dataId']].rename(columns={'dataId': 'id'})
    num_batches = (len(Tasks_df3) + batch_size - 1) // batch_size
    all_responses = []
    ac_token = get_access_token() if location == "TW" else get_access_token_ml()
    url_batch = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/batch" if location == "TW" \
        else "https://api-scrm.xiaoshouyi.com/rest/bulk/v2/batch"

    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"}

    for current_batch_index in range(num_batches):
        start_index = current_batch_index * batch_size
        end_index = (current_batch_index + 1) * batch_size
        current_batch_df = Tasks_df3.iloc[start_index:end_index]
        json_data = current_batch_df.to_dict(orient='records')
        data = { "data": {"jobId": bulk_id,"datas": json_data}}

        response = requests.post(url_batch, headers=headers, json=data)
        response.raise_for_status()
        all_responses.append(response.json())
        print(f"[Batch {current_batch_index + 1}/{num_batches}] Deleted {len(json_data)} records from {object_name}")
        result_df = pd.DataFrame(all_responses)
    return result_df







def withdraw_tasks(Tasks_df2, delay=0.08):
    ac_token = get_access_token()
    print(f"[Access Token] Successfully fetched: {ac_token[:6]}****")

    url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"}

    results = []
    def withdraw_task(row):
        data_id = row['dataId']
        task_id = row['procInstId']

        data = {
            "data": {
                "action": "withdraw",
                "entityApiKey": "customEntity14__c",
                "dataId": data_id,
                "procInstId": task_id
            }
        }

        try:
            response = requests.post(url_2, headers=headers, json=data)
            response.raise_for_status()
            result = response.json()
            print(f"[Success] Withdraw for dataId: {data_id}")
        except requests.RequestException as e:
            result = {"dataId": data_id, "error": str(e)}
            print(f"[Error] Withdraw for dataId: {data_id} failed with error: {e}")
        
        results.append(result)

    threads = []
    for index, row in Tasks_df2.iterrows():
        thread = threading.Thread(target=withdraw_task, args=(row,))
        threads.append(thread)
        thread.start()
        time.sleep(delay) 
    for thread in threads:
        thread.join()
    return pd.DataFrame(results)






def best_contact(df: pd.DataFrame) -> pd.DataFrame:
    """
    每家公司僅保留一筆最佳聯絡人。
    優先順序依據：關係狀態（在職主要 > 在職配合 > 離職 > 其他）、
                職務類別排序、最近聯繫時間
    """

    # 關係狀態分類邏輯
    def parse_employed(e_str: str) -> str:
        if "主要" in e_str:
            return "在職主要"
        elif "配合" in e_str:
            return "在職配合"
        elif "離職" in e_str:
            return "離職"
        else:
            return "其他"

    def parse_job_role(job_str: str) -> int:
        j = str(job_str)

        if "非木工" in j:
            return 99 
        elif "老闆娘" in j:
            return 99
        elif "工頭" in j:
            return 1
        elif "KEY MAN" in j or "Keyman" in j:
            return 2
        elif "採購" in j:
            return 3
        elif "設計總監" in j or "設計主管" in j:
            return 4
        elif "設計師" in j:
            return 5
        elif "設計助理" in j or "繪圖師" in j:
            return 6
        elif "資材" in j:
            return 7
        elif "建築師" in j:
            return 8
        elif "木工" in j and ("發包" not in j and "師傅" not in j):
            return 9
        elif "發包" in j and ("木工" in j or "師傅" in j):
            return 10
        elif "統包" in j and "老闆" in j:
            return 11
        elif "老闆" in j:
            return 12
        else:
            return 99

    employed_priority = {"在職主要": 1, "在職配合": 2, "離職": 3, "其他": 4}

    df = df.copy()
    df["關係狀態分類"] = df["關係狀態"].astype(str).apply(parse_employed)
    df["employed_code"] = df["關係狀態分類"].map(employed_priority).fillna(99)
    df["職務排序"] = df["職務類別"].astype(str).apply(parse_job_role)
    df["最近聯繫時間_dt"] = pd.to_datetime(df["日期"], errors="coerce")

    before = len(df)
    df = (
        df.sort_values(
            by=["公司代號", "employed_code", "職務排序", "最近聯繫時間_dt"],
            ascending=[True, True, True, False]
        )
        .drop_duplicates(subset=["公司代號"], keep="first")
        .reset_index(drop=True)
    )
    after = len(df)
    print(f"14. 每家公司保留一筆最佳聯絡人: 移除 {before - after} 筆, 剩餘 {after} 筆")
    return df






def filter_by_target(
        df: pd.DataFrame,
        column: str,
        target_list: List[str],
        *,
        fuzzy: bool = False,   
        verbose: bool = False  
    ) -> pd.DataFrame:


    targets = [t.strip() for t in target_list if t is not None]
    targets_no_empty = [t for t in targets if t]       
    if verbose:
        print(f"[DEBUG] targets = {targets}")
    def to_list(val):
        if pd.isna(val) or val == '':
            return []
        if isinstance(val, list):
            return val
        try:
            return json.loads(val)           
        except Exception:
            try:
                return ast.literal_eval(val)  
            except Exception:
                return []

    parsed_col = df[column].apply(to_list)
    def has_hit(lst):
        if not lst:
            return False
        if fuzzy:
            return any(any(t in item for t in targets_no_empty) for item in lst)
        return any(item in targets_no_empty for item in lst)
    hit = parsed_col.apply(has_hit)
    if '' in targets:
        hit = hit | parsed_col.apply(lambda lst: len(lst) == 0) | df[column].isna()
    return df[hit].copy()





def clean_invalid_entries_project(K_invite: pd.DataFrame, source_type: str = None) -> tuple:
    excluded_rows = [] 

    def log_diff(before, after, step_name):
        print(f"{step_name}: 移除 {before - after} 筆, 剩餘 {after} 筆")

    def record_exclusion(before_df, after_df, step_name):
        removed_df = before_df[~before_df.index.isin(after_df.index)].copy()
        removed_df['剔除階段'] = step_name
        excluded_rows.append(removed_df)

    print(f"原始資料筆數: {len(K_invite)}")

    # 1. 排除 SAP 管制戶
    sap_credit = get_data_from_MSSQL('''
        SELECT KUNNR AS SAP公司代號 
        FROM SAPdb.dbo.ZSD31B 
        WHERE VTEX8 = '呆帳管制'
    ''')

    if source_type == 'SAP':
        before_df = K_invite.copy()
        K_invite = K_invite[~K_invite['SAP公司代號'].isin(sap_credit['SAP公司代號'])]
        record_exclusion(before_df, K_invite, "1. 排除 SAP 管制戶（使用 SAP公司代號）")
        log_diff(len(before_df), len(K_invite), "1. 排除 SAP 管制戶（使用 SAP公司代號）")
    else:
        company_map = get_data_from_MSSQL('''
            SELECT company_id 公司代號, sap_company_id SAP公司代號, company_id_parent 關聯公司
            FROM [raw_data].[dbo].[crm_related_company]
        ''')
        merged = pd.merge(sap_credit, company_map, on='SAP公司代號', how='left')
        merged['公司代號'] = merged['關聯公司'].fillna(merged['SAP公司代號'])
        sap_credit = merged[['公司代號']].dropna().drop_duplicates().reset_index(drop=True)

        before_df = K_invite.copy()
        K_invite = K_invite[~K_invite['公司代號'].isin(sap_credit['公司代號'])]
        record_exclusion(before_df, K_invite, "1. 排除 SAP 管制戶（使用公司代號）")
        log_diff(len(before_df), len(K_invite), "1. 排除 SAP 管制戶（使用公司代號）")

    # 2. 公司名稱關鍵字 + 特殊條件（僅當兩者皆為空才剔除）
    keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"

    # 2-1. 含關鍵字的公司名稱
    mask_keyword = (
        K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
        K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False)
    )

    # 3-2. 公司電話與手機皆為空（兩個都為空才排除）
    mask_both_empty = (
        (K_invite['公司電話'].fillna('').str.strip() == '') &
        (K_invite['手機號碼'].fillna('').str.strip() == ''))

    # 組合邏輯：排除 含關鍵字 或 兩者皆空 的資料
    before_df = K_invite.copy()
    K_invite = K_invite[~(mask_keyword | mask_both_empty)]
    record_exclusion(before_df, K_invite, "2. 公司名稱關鍵字 + 聯絡方式皆空")
    log_diff(len(before_df), len(K_invite), "2. 公司名稱關鍵字 + 聯絡方式皆空")


    # 3. 標籤倒閉
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['倒閉'].astype(str).str.contains("是")]
    record_exclusion(before_df, K_invite, "3. 標籤倒閉")
    log_diff(len(before_df), len(K_invite), "3. 標籤倒閉")

    # 4. 特定區域群組
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['資料區域群組名稱'].astype(str).str.contains("INV|TW-888|TW-LB|TW-CPT|TW-PD")]
    record_exclusion(before_df, K_invite, "4. 特定區域群組")
    log_diff(len(before_df), len(K_invite), "4. 特定區域群組")

    # 5. 排除地區
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['公司地址'].astype(str).str.contains("花蓮|台東|金門|澎湖|馬祖")]
    record_exclusion(before_df, K_invite, "5. 排除特定地區")
    log_diff(len(before_df), len(K_invite), "5. 排除特定地區")

    # 6. 公司勿擾選項
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['公司勿擾選項'].astype(str).str.contains("拜訪")]
    record_exclusion(before_df, K_invite, "6. 公司勿擾選項")
    log_diff(len(before_df), len(K_invite), "6. 公司勿擾選項")

    # 7. 關係狀態
    before_df = K_invite.copy()
    K_invite = K_invite[K_invite['關係狀態'].astype(str).str.contains("主要|配合")]
    record_exclusion(before_df, K_invite, "7. 關係狀態為主要或配合")
    log_diff(len(before_df), len(K_invite), "7. 關係狀態為主要或配合")

    # 8. 聯絡人資料無效
    before_df = K_invite.copy()
    K_invite = K_invite[K_invite['連絡人資料無效'].astype(str).str.contains("否")]
    record_exclusion(before_df, K_invite, "8. 聯絡人資料無效")
    log_diff(len(before_df), len(K_invite), "8. 聯絡人資料無效")

    # 9. 電話狀態
    before_df = K_invite.copy()
    mask_not_empty = (K_invite['空號'].fillna(0).astype(int) != 1)
    mask_not_stopped = (K_invite['停機'].fillna(0).astype(int) != 1)
    mask_correct_owner = (K_invite['號碼錯誤非本人'] != "['是']") & (K_invite['號碼錯誤非本人'] != "是")
    K_invite = K_invite[mask_not_empty & mask_not_stopped & mask_correct_owner]
    record_exclusion(before_df, K_invite, "9. 電話狀態正常")
    log_diff(len(before_df), len(K_invite), "9. 電話狀態正常")

    # 10. 聯絡人姓名排除
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世")]
    record_exclusion(before_df, K_invite, "10. 聯絡人姓名排除")
    log_diff(len(before_df), len(K_invite), "10. 聯絡人姓名排除")

    # 11. 聯絡方式皆空
    before_df = K_invite.copy()
    K_invite = K_invite[(K_invite['手機號碼'] != '') | (K_invite['公司電話'] != '')]
    record_exclusion(before_df, K_invite, "11. 聯絡方式不可皆空")
    log_diff(len(before_df), len(K_invite), "11. 聯絡方式不可皆空")

    # 12. 近三月拒訪
    three_month_ago = (dt.today() - relativedelta(months=3) + relativedelta(days=1)).date()
    timestamp = pd.to_datetime(three_month_ago).timestamp() * 1000
    refused = get_data_from_CRM(f'''
        select
        accountCode__c 公司代號,customItem40__c 最近聯繫時間,
        customItem128__c 觸客類型,customItem176__c 無效電拜訪類型
        from customEntity15__c
        where dimDepart.departName like '%TW%'
        and customItem40__c >= {timestamp}
    ''')
    refused = refused[
        (refused['觸客類型'].astype(str).str.contains("電訪-無效")) &
        (refused['無效電拜訪類型'].astype(str).str.contains("拒拜訪"))
    ]
    before_df = K_invite.copy()
    K_invite = K_invite[~K_invite['公司代號'].isin(refused['公司代號'])]
    record_exclusion(before_df, K_invite, "12. 近三月拒訪紀錄")
    log_diff(len(before_df), len(K_invite), "12. 近三月拒訪紀錄")

    # 合併所有被剔除資料
    if excluded_rows:
        excluded_df = pd.concat(excluded_rows, ignore_index=True)
    else:
        excluded_df = pd.DataFrame(columns=K_invite.columns.tolist() + ['剔除階段'])

    return K_invite, excluded_df









# 清理函數定義
def clean_invalid_entries_census(K_invite: pd.DataFrame) -> pd.DataFrame:
    def log_diff(before, after, step_name):
        print(f"{step_name}: 移除 {before - after} 筆, 剩餘 {after} 筆")

    print(f"原始資料筆數: {len(K_invite)}")
    removed_contact_tag_ids = []
    pattern = '|'.join(["客戶不願意提供","2025_已完成普查","2025_K大已完成普查","2025_普查確認速絡人無效"])


    # 1. 排除 SAP 管制戶
    sap_credit = get_data_from_MSSQL('''
            SELECT KUNNR AS SAP公司代號 
            FROM SAPdb.dbo.ZSD31B 
            WHERE VTEX8 = '呆帳管制'
    ''')
    before = len(K_invite)
    K_invite = K_invite[~K_invite['SAP公司代號'].isin(sap_credit['SAP公司代號'])]
    log_diff(before, len(K_invite), "0. 排除 SAP 管制戶")

    # 1. 公司名稱關鍵字 + 特殊條件
    keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"
    mask_keyword = (
                    K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
                    K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False))
    mask_both_empty = (
        (K_invite['公司電話'].fillna('').str.strip() == '') &
        (K_invite['手機號碼'].fillna('').str.strip() == ''))
    before = len(K_invite)
    K_invite = K_invite[~(mask_keyword | mask_both_empty)]
    log_diff(before, len(K_invite), "1. 公司名稱關鍵字 + 特殊條件")

    # 2. 標籤倒閉
    before = len(K_invite)
    K_invite = K_invite[~K_invite['倒閉'].astype(str).str.contains("是", na=False)]
    log_diff(before, len(K_invite), "2. 標籤倒閉")

    # 3. 特定區域群組排除
    before = len(K_invite)
    K_invite = K_invite[~K_invite['資料區域群組名稱'].astype(str).str.contains("INV|TW-888|TW-LB|TW-CPT|TW-PD")]
    log_diff(before, len(K_invite), "3. 特定區域群組排除")

    # 4. 職務類別過濾
    before = len(K_invite)
    K_invite = K_invite[K_invite['職務類別'].astype(str).str.contains("001|002|003|004|005|006|007|010|011|015", na=False)]
    log_diff(before, len(K_invite), "4. 職務類別過濾")

    # 5. 關係狀態為主要
    before = len(K_invite)
    K_invite = K_invite[K_invite['關係狀態'].astype(str).str.contains("主要", na=False)]
    log_diff(before, len(K_invite), "5. 關係狀態為主要")

    # 6. 聯絡人資料無效（值為"否"才保留）
    before = len(K_invite)
    K_invite = K_invite[K_invite['連絡人資料無效'].astype(str).str.contains("否", na=False)]
    log_diff(before, len(K_invite), "6. 聯絡人資料無效")

    # 7. 電話狀態過濾
    before = len(K_invite)
    mask_not_empty = (K_invite['空號'].fillna(0).astype(int) != 1)
    mask_not_stopped = (K_invite['停機'].fillna(0).astype(int) != 1)
    mask_correct_owner = (K_invite['號碼錯誤非本人'] != "['是']") & (K_invite['號碼錯誤非本人'] != "是")
    K_invite = K_invite[mask_not_empty & mask_not_stopped & mask_correct_owner]
    log_diff(before, len(K_invite), "7. 電話狀態正常")

    # 8. 聯絡人姓名剔除（死亡/退休）
    before = len(K_invite)
    K_invite = K_invite[~K_invite['連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世", na=False)]
    log_diff(before, len(K_invite), "8. 聯絡人姓名剔除")

    # 9. 手機不可空
    before = len(K_invite)
    K_invite = K_invite[K_invite['手機號碼'].fillna('') != '']
    log_diff(before, len(K_invite), "9. 手機不可空")

    # # 10. 剔除 公司聯絡人普查標籤
    # pattern = "客戶不願意提供|2025_已完成普查|2025_K大已完成普查|2025_普查確認速絡人無效"
    # before = len(K_invite)
    # mask_company_tag = K_invite['公司聯絡人普查標簽'].astype(str).str.contains(pattern, na=False)
    # removed_company_tag_ids = K_invite.loc[mask_company_tag, '公司代號'].dropna().unique().tolist()
    # K_invite = K_invite[~mask_company_tag]
    # log_diff(before, len(K_invite), "10. 剔除 公司聯絡人普查標籤")

    # 11. 剔除 聯絡人普查標籤
    before = len(K_invite)
    mask_contact_tag = K_invite['聯絡人普查標簽'].astype(str).str.contains(pattern, na=False)
    removed_contact_tag_ids = K_invite.loc[mask_contact_tag, '連絡人代號'].dropna().unique().tolist()
    K_invite = K_invite[~mask_contact_tag]
    log_diff(before, len(K_invite), "11. 剔除 聯絡人普查標籤")

    return K_invite, removed_contact_tag_ids





def add_relate_company(df: pd.DataFrame, type: str = None) -> pd.DataFrame:
    if type == 'SAP':
        company_map = get_data_from_MSSQL('''
            SELECT   sap_company_id AS SAP公司代號, sap_company_id_parent AS SAP關聯公司代號 FROM [raw_data].[dbo].[crm_related_company]
        ''')
        
        df = pd.merge(df, company_map, on='SAP公司代號', how='left')
        df['SAP公司代號'] = df['SAP關聯公司代號'].fillna(df['SAP公司代號'])
        

    else:
        company_map = get_data_from_MSSQL('''
            SELECT   company_id AS 公司代號, company_id_parent AS 關聯公司 FROM [raw_data].[dbo].[crm_related_company]
        ''')

        df = pd.merge(df, company_map, on='公司代號', how='left')
        df['公司代號'] = df['關聯公司'].fillna(df['公司代號'])

    return df



def merge_company_to_parent(df: pd.DataFrame) -> pd.DataFrame:
    """
    根據公司關聯資料，將公司代號替換為關聯公司代號，並去除重複，
    並列出被去除的公司代號及其關聯公司。
    """
    before = len(df)
    df['原始公司代號'] = df['公司代號']
    company_map = get_data_from_MSSQL('''
                        SELECT  company_id 公司代號
                            ,company_id_parent 關聯公司
                        FROM [raw_data].[dbo].[crm_related_company]
    ''')
    
    df = pd.merge(df, company_map, on='公司代號', how='left')
    df['公司代號'] = df['關聯公司'].fillna(df['公司代號'])
    duplicated_companies = df[df.duplicated(subset='公司代號', keep='first')]
    removed_df = duplicated_companies[['原始公司代號', '關聯公司']].dropna().drop_duplicates()

    df = df.drop_duplicates(subset=['公司代號'], keep='first')
    after = len(df)

    if not removed_df.empty:
        print("被去除的公司及其關聯公司：")
        print(removed_df)

    df = df.drop(columns=['原始公司代號', '關聯公司'], errors='ignore')
    return df




def get_MRK_data(years_ago_ts: float, merge_type=None) -> pd.DataFrame:
    def log(msg):
        print(f"[get_MRK_data] {msg}")

    year_ago_str = pd.to_datetime(years_ago_ts, unit='ms').strftime('%Y-%m-%d %H:%M:%S')

    df = get_data_from_MSSQL(f"""
        SELECT company_id AS 公司代號, visit_date AS 日期
        FROM clean_data.dbo.crm_K_3M
        WHERE list_type != '展示館邀約-業主'
          AND visit_date >= '{year_ago_str}'
    """)
    
    log(f"原始抓取筆數：{len(df)}")
    
    df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
    df['類型'] = 'K大'

    if merge_type != 'alone':
        log("合併關聯公司中...")
        df = merge_company_to_parent(df)

    before = len(df)
    df = df.sort_values(by=['公司代號', '日期'], ascending=[True, False])
    df = df.drop_duplicates(subset='公司代號', keep='first').reset_index(drop=True)
    log(f"每家公司保留最近一筆：{before} -> {len(df)}")

    return df








def last_connected(years_ago_ts, source_type=None, merge_type=None, first='拜訪'):
    def log(msg):
        print(f"[last_connected] {msg}")

    year_ago_str = pd.to_datetime(years_ago_ts, unit='ms').strftime('%Y-%m-%d %H:%M:%S')
    if source_type in [None, '拜訪']:
        visited = get_data_from_MSSQL(f"""
            SELECT company_id AS 公司代號, visit_date AS 日期
            FROM clean_data.dbo.crm_track_1year
            WHERE visit_date >= '{year_ago_str}'
            and list_type = '拜訪'
        """)
        visited['日期'] = pd.to_datetime(visited['日期'], errors='coerce')
        visited['類型'] = '拜訪'
        log(f"拜訪資料共 {len(visited)} 筆")
    else:
        visited = pd.DataFrame()

    if source_type in [None, 'K大']:
        K_data = get_data_from_MSSQL(f"""
            SELECT company_id AS 公司代號, visit_date AS 日期
            FROM clean_data.dbo.crm_K_3M
            WHERE list_type != '展示館邀約-業主' and present_time >= 8
              AND visit_date >= '{year_ago_str}'
        """)
        K_data['日期'] = pd.to_datetime(K_data['日期'], errors='coerce')
        K_data['類型'] = 'K大'
        log(f"K大資料共 {len(K_data)} 筆")
    else:
        K_data = pd.DataFrame()

    all_data = pd.concat([visited, K_data], ignore_index=True)
    if all_data.empty:
        log("無任何資料，回傳空 DataFrame")
        return all_data

    log(f"合併後總筆數：{len(all_data)}")

    if first == '拜訪':
        all_data['類型'] = pd.Categorical(all_data['類型'], categories=['拜訪', 'K大'], ordered=True)
        all_data = all_data.sort_values(by=['公司代號', '類型', '日期'], ascending=[True, True, False])
    elif first == 'K大':
        all_data['類型'] = pd.Categorical(all_data['類型'], categories=['K大', '拜訪'], ordered=True)
        all_data = all_data.sort_values(by=['公司代號', '類型', '日期'], ascending=[True, True, False])
    elif first == 'all':
        all_data = all_data.sort_values(by=['公司代號', '日期'], ascending=[True, False])
    else:
        raise ValueError("first 參數錯誤，必須是 '拜訪'、'K大' 或 'all'")

    if merge_type != 'alone':
        log("合併關聯公司中...")
        all_data = merge_company_to_parent(all_data)

    before = len(all_data)
    final_df = all_data.drop_duplicates(subset='公司代號', keep='first').reset_index(drop=True)
    log(f"每家公司保留最近一筆：{before} -> {len(final_df)} 筆")

    return final_df







def clean_invalid_entries_text(K_invite: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    K_invite = K_invite.copy()
    K_invite['排除原因'] = ''

    def mark_exclusion(mask, reason):
        K_invite.loc[mask, '排除原因'] += reason + '; '

    print(f"原始資料筆數: {len(K_invite)}")

    # 1. 排除 SAP 管制戶
    sap_credit = get_data_from_MSSQL('''
        SELECT KUNNR AS SAP公司代號 
        FROM SAPdb.dbo.ZSD31B 
        WHERE VTEX8 = '呆帳管制'
    ''')
    mask = K_invite['SAP公司代號'].isin(sap_credit['SAP公司代號'])
    mark_exclusion(mask, 'SAP管制戶')

    # 2. 職務類別過濾
    mask = ~K_invite['職務類別'].astype(str).str.contains("001|002|003|004|005|006|007|010|011|015|016|017", na=False)
    mark_exclusion(mask, '職務類別不符')

    # 3. 公司名稱關鍵字 + 電話空號
    keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"
    mask_keyword = (
        K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
        K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False)
    )
    mask_both_empty = (
        (K_invite['公司電話'].fillna('').str.strip() == '') &
        (K_invite['手機號碼'].fillna('').str.strip() == '')
    )
    mark_exclusion(mask_keyword, '公司關鍵字')
    mark_exclusion(mask_both_empty, '電話皆空')

    # 4. 標籤倒閉
    mask = K_invite['倒閉'].astype(str).str.contains("是", na=False)
    mark_exclusion(mask, '標記倒閉')

    # 5. 特定區域群組排除
    mask = K_invite['資料區域群組名稱'].astype(str).str.contains("INV|TW-888|TW-Z|TW-LB|TW-CPT|TW-PD", na=False)
    mark_exclusion(mask, '特定區域群組')

    # 6. 聯絡人勿擾選項
    mask = K_invite['聯絡人勿擾選項'].astype(str).str.contains("勿", na=False)
    mark_exclusion(mask, '聯絡人勿擾')

    # 6b. 公司勿擾選項
    mask = K_invite['公司勿擾選項'].astype(str).str.contains("勿", na=False)
    mark_exclusion(mask, '公司勿擾')

    # 7. 關係狀態：在職
    mask = ~K_invite['關係狀態'].astype(str).str.contains("在職", na=False)
    mark_exclusion(mask, '非在職')

    # 8. 聯絡人資料無效（值為"否"才保留）
    mask = ~K_invite['連絡人資料無效'].astype(str).str.contains("否", na=False)
    mark_exclusion(mask, '資料無效')

    # 9. 電話狀態過濾
    mask_not_empty = K_invite['空號'].fillna(0).astype(int) == 1
    mask_stopped = K_invite['停機'].fillna(0).astype(int) == 1
    mask_wrong_owner = (K_invite['號碼錯誤非本人'] == "['是']") | (K_invite['號碼錯誤非本人'] == "是")
    mark_exclusion(mask_not_empty, '空號')
    mark_exclusion(mask_stopped, '停機')
    mark_exclusion(mask_wrong_owner, '號碼錯誤')

    # 10. 聯絡人姓名剔除（死亡/退休）
    mask = K_invite['連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世", na=False)
    mark_exclusion(mask, '姓名顯示死亡/退休')

    # 11. 手機不可空
    mask = K_invite['手機號碼'].fillna('') == ''
    mark_exclusion(mask, '手機空')

    # 最後分離結果
    cleaned = K_invite[K_invite['排除原因'] == ''].copy()
    removed = K_invite[K_invite['排除原因'] != ''].copy()

    print(f"最終剩餘筆數: {len(cleaned)}")
    print(f"被排除筆數: {len(removed)}")

    return cleaned, removed






def clean_invalid_entries_text_規劃組專案(K_invite: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    K_invite = K_invite.copy()
    K_invite['排除原因'] = ''

    def mark_exclusion(mask, reason):
        K_invite.loc[mask, '排除原因'] += reason + '; '

    print(f"原始資料筆數: {len(K_invite)}")

    # 1. 排除 SAP 管制戶
    sap_credit = get_data_from_MSSQL('''
        SELECT KUNNR AS SAP公司代號 
        FROM SAPdb.dbo.ZSD31B 
        WHERE VTEX8 = '呆帳管制'
    ''')
    mask = K_invite['SAP公司代號'].isin(sap_credit['SAP公司代號'])
    mark_exclusion(mask, 'SAP管制戶')

    # 2. 職務類別過濾
    mask = ~K_invite['職務類別'].astype(str).str.contains("001|002|003|004|005|007|008|012|015|016", na=False)
    mark_exclusion(mask, '職務類別不符')

    # 3. 公司名稱關鍵字 + 電話空號
    keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"
    mask_keyword = (
        K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
        K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False)
    )
    mask_both_empty = (
        (K_invite['公司電話'].fillna('').str.strip() == '') &
        (K_invite['手機號碼'].fillna('').str.strip() == '')
    )
    mark_exclusion(mask_keyword, '公司關鍵字')
    mark_exclusion(mask_both_empty, '電話皆空')

    # 4. 標籤倒閉
    mask = K_invite['倒閉'].astype(str).str.contains("是", na=False)
    mark_exclusion(mask, '標記倒閉')

    # 5. 特定區域群組排除
    mask = K_invite['資料區域群組名稱'].astype(str).str.contains("INV|TW-888|TW-Z|TW-LB|TW-CPT|TW-PD", na=False)
    mark_exclusion(mask, '特定區域群組')

    # 6. 聯絡人勿擾選項
    mask = K_invite['聯絡人勿擾選項'].astype(str).str.contains("勿", na=False)
    mark_exclusion(mask, '聯絡人勿擾')

    # 6b. 公司勿擾選項
    mask = K_invite['公司勿擾選項'].astype(str).str.contains("勿", na=False)
    mark_exclusion(mask, '公司勿擾')

    # 7. 關係狀態：在職
    mask = ~K_invite['關係狀態'].astype(str).str.contains("在職", na=False)
    mark_exclusion(mask, '非在職')

    # 8. 聯絡人資料無效（值為"否"才保留）
    mask = ~K_invite['連絡人資料無效'].astype(str).str.contains("否", na=False)
    mark_exclusion(mask, '資料無效')

    # 9. 電話狀態過濾
    mask_not_empty = K_invite['空號'].fillna(0).astype(int) == 1
    mask_stopped = K_invite['停機'].fillna(0).astype(int) == 1
    mask_wrong_owner = (K_invite['號碼錯誤非本人'] == "['是']") | (K_invite['號碼錯誤非本人'] == "是")
    mark_exclusion(mask_not_empty, '空號')
    mark_exclusion(mask_stopped, '停機')
    mark_exclusion(mask_wrong_owner, '號碼錯誤')

    # 10. 聯絡人姓名剔除（死亡/退休）
    mask = K_invite['連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世", na=False)
    mark_exclusion(mask, '姓名顯示死亡/退休')

    # 11. 手機不可空
    mask = K_invite['手機號碼'].fillna('') == ''
    mark_exclusion(mask, '手機空')

    # 最後分離結果
    cleaned = K_invite[K_invite['排除原因'] == ''].copy()
    removed = K_invite[K_invite['排除原因'] != ''].copy()

    print(f"最終剩餘筆數: {len(cleaned)}")
    print(f"被排除筆數: {len(removed)}")

    return cleaned, removed









def get_sap_with_relate_company( start_date_str: str, related_company: bool = True,location: str = "TW"):


    # 1 日期區間
    start_date = dt.strptime(start_date_str, "%Y/%m/%d")
    date_base = dt.now()
    cutover = dt(2025, 1, 1)

    raw_start = start_date
    raw_end = min(date_base, cutover)
    proc_start = max(start_date, cutover)
    proc_end = date_base

    raw_start_s = raw_start.strftime("%Y-%m-%d")
    raw_end_s = raw_end.strftime("%Y-%m-%d")
    proc_start_s = proc_start.strftime("%Y-%m-%d")
    proc_end_s = proc_end.strftime("%Y-%m-%d")

    # 2 location 前綴
    location = location.strip().upper()
    sap_like = f"{location}%"

    # 3 SAP 銷售資料
    sql_union = f"""
    WITH combined AS (
        SELECT 
            CAST(kunag AS NVARCHAR(50)) AS SAP公司代號,
            netwr_l AS 未稅本位幣,
            CAST(wadat AS DATE) AS 預計發貨日期
        FROM [raw_data].[dbo].[final_sales_history]
        WHERE wadat >= '{raw_start_s}'
          AND wadat < '{raw_end_s}'
          AND is_sales = 'V'
          AND LEN(kunag) > 0

        UNION ALL

        SELECT
            CAST(buyer AS NVARCHAR(50)) AS SAP公司代號,
            taxfree_basecurr AS 未稅本位幣,
            CAST(planned_shipping_date AS DATE) AS 預計發貨日期
        FROM [clean_data].[dbo].[sap_sales_data_processed]
        WHERE planned_shipping_date >= '{proc_start_s}'
          AND planned_shipping_date < '{proc_end_s}'
          AND is_count = 1
    )
    SELECT SAP公司代號, 未稅本位幣, 預計發貨日期
    FROM combined
    WHERE SAP公司代號 LIKE '{sap_like}'
      AND 預計發貨日期 >= '{start_date.strftime("%Y-%m-%d")}'
    """

    sap_sales_data = get_data_from_MSSQL(sql_union)

    # 4 不合併關聯公司
    if not related_company:
        return sap_sales_data[["SAP公司代號", "未稅本位幣", "預計發貨日期"]]

    # 5 關聯公司表
    company_map = get_data_from_MSSQL("""
        SELECT
            company_id AS 公司代號,
            sap_company_id AS SAP公司代號,
            company_id_parent AS 關聯公司
        FROM [raw_data].[dbo].[crm_related_company]
    """)

    # 6 合併與替換
    merged = pd.merge(
        sap_sales_data,
        company_map,
        on="SAP公司代號",
        how="left"
    )

    merged["公司代號"] = merged["關聯公司"].fillna(merged["公司代號"])

    # 7 最終結果
    result = (
        merged[["公司代號", "未稅本位幣", "預計發貨日期"]]
        .dropna(subset=["公司代號"])
    )

    return result





def get_sub_companies_by_related_parent(df: pd.DataFrame) -> list:
    company_map = get_data_from_MSSQL('''
        SELECT company_id AS 公司代號, company_id_parent AS 關聯公司
        FROM [raw_data].[dbo].[crm_related_company]
    ''')

    # 找出輸入公司對應的關聯公司（母公司）
    merged = pd.merge(df[['公司代號']], company_map, on='公司代號', how='left')
    related_parents = merged['關聯公司'].fillna(merged['公司代號']).unique().tolist()

    # 根據關聯公司，再找出底下所有子公司（company_id）
    sub_companies = company_map[company_map['關聯公司'].isin(related_parents)]
    sub_company_ids = sub_companies['公司代號'].dropna().astype(str).unique().tolist()

    return sub_company_ids





def get_latest_excel(folder_path, keyword):
    file_pattern = os.path.join(folder_path, f"*{keyword}*.xlsx")
    files = glob.glob(file_pattern)
    files = [f for f in files if not os.path.basename(f).startswith("~$")]
    if files:
        latest_file = max(files, key=os.path.getctime)
        print(f"找到的最新文件: {os.path.basename(latest_file)}")  
        return latest_file
    else:
        print("沒有找到匹配的文件！")
        return None



filter_applicability = {
    1: {"company", "contact", "stored_value"}, #SAP 管制戶
    2: {"company", "contact", "stored_value"}, #公司名稱包含關鍵字 + 特殊情況
    3: {"company", "contact", "stored_value"}, #標籤為倒閉的公司
    4: {"company", "contact", "stored_value"}, #特定區域群組
    5: {"company", "contact", "stored_value"}, #公司型態為 SD
    6: {"company", "contact"},                 #公司勿擾設定為「勿拜訪」
    7: {"company", "contact", "stored_value"}, #聯絡人關係狀態不是「主要」或「配合」
    8: {"company", "contact", "stored_value"}, #聯絡人資料標註為無效
    9: {"company", "contact", "stored_value"}, #電話狀態異常
    10: {"company", "contact", "stored_value"}, #聯絡人姓名含「過世、退休…」等
    11: {"company", "contact", "stored_value"}, #公司電話與手機皆為空白
    12: {"company", "contact"},                 #近三個月曾拒絕拜訪紀錄者
    13: {"stored_value"},                       #勿擾名單 Excel 中的公司代號
}



def clean_invalid_entries_visit(K_invite: pd.DataFrame, data_type: str = 'company') -> pd.DataFrame:
    def log_diff(before, after, step_name):
        print(f"{step_name}: 移除 {before - after} 筆, 剩餘 {after} 筆")

    def is_enabled(step: int) -> bool:
        return data_type in filter_applicability.get(step, set())

    def need_company_relation(data_type: str) -> bool:
        return data_type in (None, "company", "stored_value")

    print(f"原始資料筆數: {len(K_invite)}")

    # 1. SAP 管制戶
    if is_enabled(1):
        sap_credit = get_data_from_MSSQL('''
            SELECT KUNNR AS SAP公司代號 
            FROM SAPdb.dbo.ZSD31B 
            WHERE VTEX8 = '呆帳管制'
        ''')
        if need_company_relation(data_type):
            company_map = get_data_from_MSSQL('''
                        SELECT  company_id 公司代號
                            ,sap_company_id SAP公司代號
                            ,company_id_parent 關聯公司
                        FROM [raw_data].[dbo].[crm_related_company]
            ''')
            merged = pd.merge(sap_credit, company_map, on='SAP公司代號', how='left')
            merged['公司代號'] = merged['關聯公司'].fillna(merged['SAP公司代號'])
            sap_credit_ids = merged['公司代號'].dropna().unique().tolist()
            before = len(K_invite)
            K_invite = K_invite[~K_invite['公司代號'].isin(sap_credit_ids)]
            log_diff(before, len(K_invite), "1. 排除 SAP 管制戶（使用關聯公司代號）")
        else:
            before = len(K_invite)
            K_invite = K_invite[~K_invite['SAP公司代號'].isin(sap_credit['SAP公司代號'])]
            log_diff(before, len(K_invite), "1. 排除 SAP 管制戶（使用本公司SAP代號）")

    # 2. 公司名稱關鍵字 + 無聯絡方式過濾
    if is_enabled(2):
        keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"
        
        # A. 公司名稱含關鍵字
        mask_keyword = (
            K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
            K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False)
        )

        # B. 公司電話與手機號碼都為空
        mask_both_empty = (
            (K_invite['公司電話'].fillna('') == '') &
            (K_invite['手機號碼'].fillna('') == '')
        )

        # 合併條件，標記需排除的資料
        mask_to_exclude = mask_keyword | mask_both_empty

        before = len(K_invite)
        K_invite = K_invite[~mask_to_exclude]
        log_diff(before, len(K_invite), "2. 公司名稱關鍵字 + 無聯絡方式")


    # 3. 標籤倒閉
    if is_enabled(3):
        before = len(K_invite)
        K_invite = K_invite[~K_invite['倒閉'].astype(str).str.contains("是")]
        log_diff(before, len(K_invite), "3. 標籤倒閉")

    # 4. 特定區域群組
    if is_enabled(4):
        before = len(K_invite)
        K_invite = K_invite[~K_invite['資料區域群組名稱'].astype(str).str.contains("INV|TW-888|TW-Z|TW-LB|TW-CPT|TW-PD")]
        log_diff(before, len(K_invite), "4. 特定區域群組")

    # 5. 公司型態為 SD
    if is_enabled(5):
        before = len(K_invite)
        K_invite = K_invite[~K_invite['公司型態'].astype(str).str.contains("SD")]
        log_diff(before, len(K_invite), "5. 公司型態為 SD")

    # 6. 公司勿擾選項
    if is_enabled(6):
        before = len(K_invite)
        K_invite = K_invite[~K_invite['公司勿擾選項'].astype(str).str.contains("拜訪")]
        log_diff(before, len(K_invite), "6. 公司勿擾選項")

    # 7. 關係狀態
    if is_enabled(7):
        before = len(K_invite)
        K_invite = K_invite[K_invite['關係狀態'].astype(str).str.contains("主要|配合")]
        log_diff(before, len(K_invite), "7. 關係狀態為主要或配合")

    # 8. 聯絡人資料無效
    if is_enabled(8):
        before = len(K_invite)
        K_invite = K_invite[K_invite['連絡人資料無效'].astype(str).str.contains("否")]
        log_diff(before, len(K_invite), "8. 聯絡人資料無效")

    # 9. 電話狀態
    if is_enabled(9):
        before = len(K_invite)
        mask_not_empty = (K_invite['空號'].fillna(0).astype(int) != 1)
        mask_not_stopped = (K_invite['停機'].fillna(0).astype(int) != 1)
        mask_correct_owner = (K_invite['號碼錯誤非本人'] != "['是']") & (K_invite['號碼錯誤非本人'] != "是")
        K_invite = K_invite[mask_not_empty & mask_not_stopped & mask_correct_owner]
        log_diff(before, len(K_invite), "9. 電話狀態正常")

    # 10. 聯絡人姓名排除
    if is_enabled(10):
        before = len(K_invite)
        K_invite = K_invite[~K_invite['連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世")]
        log_diff(before, len(K_invite), "10. 聯絡人姓名排除")

    # 11. 聯絡方式不可皆空
    if is_enabled(11):
        before = len(K_invite)
        K_invite = K_invite[(K_invite['手機號碼'] != '') | (K_invite['公司電話'] != '')]
        log_diff(before, len(K_invite), "11. 聯絡方式不可皆空")

    # 12. 拒訪紀錄
    if is_enabled(12):
        three_month_ago = (dt.today() - relativedelta(months=3) + relativedelta(days=1)).date()
        timestamp = pd.to_datetime(three_month_ago).timestamp() * 1000
        refused = get_data_from_CRM(f'''
            select
            accountCode__c 公司代號,customItem40__c 最近聯繫時間,
            customItem128__c 觸客類型,customItem176__c 無效電拜訪類型
            from customEntity15__c
            where  dimDepart.departName  like '%TW%'
            and customItem40__c >= {timestamp}
        ''')
        refused = refused[
            (refused['觸客類型'].astype(str).str.contains("電訪-無效")) &
            (refused['無效電拜訪類型'].astype(str).str.contains("拒拜訪"))
        ]
        before = len(K_invite)
        K_invite = K_invite[~K_invite['公司代號'].isin(refused['公司代號'])]
        log_diff(before, len(K_invite), "12. 拒訪紀錄")

    # 13. 勿擾名單 Excel 處理
    if is_enabled(13):
        try:
            folder_path = r"Z:\06_業管部\04_管理課\張恆碩\儲值金LINE訊息邀約"
            keyword = "勿擾名單"
            latest_file = get_latest_excel(folder_path, keyword)

            if latest_file:
                disturb_df = pd.read_excel(latest_file, sheet_name="勿擾名單")

                #  直接清洗公司代號欄位（移除空白、換行、全形空格等）
                disturb_df['公司代號'] = (
                    disturb_df['公司代號']
                    .astype(str)
                    .str.strip()
                    .str.replace(r'[\s\u3000\xa0\r\n]+', '', regex=True)
                )

                # 若 K_invite['公司代號'] 也可能含髒字元，一併清洗（可省略，保險起見建議加）
                K_invite['公司代號'] = (
                    K_invite['公司代號']
                    .astype(str)
                    .str.strip()
                    .str.replace(r'[\s\u3000\xa0\r\n]+', '', regex=True)
                )

                # 找出關聯公司底下所有子公司
                sub_company_ids = get_sub_companies_by_related_parent(disturb_df)

                before = len(K_invite)
                K_invite = K_invite[~K_invite['公司代號'].isin(sub_company_ids)]
                log_diff(before, len(K_invite), "13. 剔除勿擾名單的子公司（含關聯）")
        except Exception as e:
            print(f"無法處理勿擾名單 Excel：{str(e)}")

    return K_invite




def get_sap_with_relate_company_os( start_date_str: str, location: str = "TW",related_company: bool = True):

    #  1. 日期 
    start_date = dt.strptime(start_date_str, "%Y/%m/%d")
    start_date_s = start_date.strftime("%Y-%m-%d")
    location = location.strip().upper()
    sap_like = f"{location}%"
    #  2. SAP 銷售資料 
    sap = get_data_from_MSSQL(f"""
        SELECT
            buyer AS SAP公司代號,
            taxfree_basecurr AS 未稅本位幣,
            CAST(planned_shipping_date AS DATE) AS 預計發貨日期,
            cal_curr AS 交易貨幣
        FROM sap_sales_data
        WHERE buyer LIKE '{sap_like}'
          AND planned_shipping_date >= '{start_date}'
    """)

    sap["預計發貨日期"] = pd.to_datetime(sap["預計發貨日期"], errors="coerce")
    sap["交易年份"] = sap["預計發貨日期"].dt.year

    #  3. 匯率表 
    rate_path = r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\★每月業務電拜訪★\共用CRM資訊\Temp\電拜訪清單資料\匯率_2026.xlsx"

    rate = (
        pd.read_excel(rate_path)
        .rename(columns={
            "yyyy": "年份",
            "貨幣": "交易貨幣",
            "固定匯率": "對台幣匯率"
        }))

    #  4. 匯率合併（只處理非 TWD） 
    sap_twd = sap[sap["交易貨幣"] == "TWD"].copy()
    sap_twd["對台幣匯率"] = 1.0

    sap_non_twd = sap[sap["交易貨幣"] != "TWD"].merge(
        rate,
        how="left",
        left_on=["交易年份", "交易貨幣"],
        right_on=["年份", "交易貨幣"] )

    sap = pd.concat([sap_twd, sap_non_twd], ignore_index=True)

    #  5. 台幣金額 
    sap["未稅金額_台幣"] = sap["未稅本位幣"] * sap["對台幣匯率"]

    #  6. 關聯公司 
    if related_company:
        company_map = get_data_from_MSSQL("""
            SELECT
                company_id AS 公司代號,
                sap_company_id AS SAP公司代號,
                company_id_parent AS 關聯公司
            FROM [raw_data].[dbo].[crm_related_company]
        """)

        sap = sap.merge(company_map, on="SAP公司代號", how="left")
        sap["公司代號"] = sap["關聯公司"].fillna(sap["公司代號"])
    else:
        sap["公司代號"] = sap["SAP公司代號"]

    #  7. 最終輸出 
    return (sap[[ "公司代號", "交易貨幣", "未稅本位幣", "未稅金額_台幣","預計發貨日期" ]].dropna(subset=["公司代號"]))





def submit_to_CRM(Tasks_df):
    ac_token = get_access_token()

    status_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task/actions/preProcessor"
    task_url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/task"
    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"
    }

    def preProcessor(process_ID):
        status_body = {
            "data": {
                "action": "submit",
                "entityApiKey": "customEntity14__c",
                "dataId": process_ID
            }
        }
        response = requests.post(status_url, headers=headers, json=status_body)
        response.raise_for_status()
        return response.json()['data']

    last_id = Tasks_df.iloc[-1]['id']
    approval_status = preProcessor(last_id)
    def submit_task(row):
        data_id = row['id']
        task_id = row['customItem10__c']
        data = {
            "data": {
                "action": "submit",
                "entityApiKey": "customEntity14__c",
                "dataId": data_id,
                "procdefId": approval_status['procdefId'],
                "nextTaskDefKey": approval_status['nextTaskDefKey'],
                "nextAssignees": [task_id],
                "ccs": [task_id]
            }
        }
        try:
            response = requests.post(task_url, headers=headers, json=data)
            result = response.json()
            print(f"Response for dataId {data_id}: {result}")
        except Exception as e:
            print(f"Error submitting dataId {data_id}: {str(e)}")

    threads = []
    for _, row in Tasks_df[['id', 'customItem10__c']].iterrows():
        thread = threading.Thread(target=submit_task, args=(row,))
        threads.append(thread)
        thread.start()
        time.sleep(0.08)  # 控制送出速率，避免過快被拒絕

    for thread in threads:
        thread.join()

    print("所有任務提交完成")





if __name__ == "__main__":
    print("跑~")