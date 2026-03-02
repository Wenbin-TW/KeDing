
import pandas as pd
import pyodbc
import json
import requests
from datetime import datetime, timedelta
import pymysql
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil
import os
from pathlib import Path
import sys
import numpy as np
import time
import re
import math
from pandas import Timestamp
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import pytz

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd


def build_time_ranges(now: datetime):
    today = now.date()
    ranges = []

    ranges.append({
        "rule": "DAY_3",
        "start": today - timedelta(days=3),
        "end": today
    })

    if today.weekday() == 0:
        ranges.append({
            "rule": "WEEK_7",
            "start": today - timedelta(days=7),
            "end": today
        })

    if today.day == 5:
        first_day_this_month = today.replace(day=1)
        ranges.append({
            "rule": "MONTH_LAST",
            "start": first_day_this_month - relativedelta(months=1),
            "end": first_day_this_month
        })

    return ranges



def insert_to_CRM(bulk_id, test_MRK, location="TW"):

    batch_size = 5000
    num_batches = (len(test_MRK) + batch_size - 1) // batch_size
    all_responses = []

    if location in ["", "TW"]:
        ac_token = kd.get_access_token()
        url = "https://api-p10.xiaoshouyi.com/rest/bulk/v2/batch"
    elif location == "ML":
        ac_token = kd.get_access_token_ml()
        url = "https://api-scrm.xiaoshouyi.com/rest/bulk/v2/batch"
    else:
        raise ValueError("location 必須為 'TW' 或 'ML'")

    headers = {
        "Authorization": f"Bearer {ac_token}",
        "Content-Type": "application/json"
    }

    for current_batch_index in range(num_batches):
        start_index = current_batch_index * batch_size
        end_index = (current_batch_index + 1) * batch_size
        current_batch_df = test_MRK.iloc[start_index:end_index]

        json_data = current_batch_df.to_dict(orient="records")

        for row in json_data:

            if row.get("customItem48__c") in [None, "None", "nan", "<NA>", ""]:
                row.pop("customItem48__c", None)
            else:
                try:
                    row["customItem48__c"] = int(row["customItem48__c"])
                except Exception:
                    row.pop("customItem48__c", None)

            if "entityType" in row:
                try:
                    row["entityType"] = int(row["entityType"])
                except Exception:
                    raise ValueError(f"entityType 無法轉成 int: {row.get('entityType')}")

            for date_key in ["customItem4__c", "customItem51__c", "customItem95__c"]:
                if date_key in row:
                    v = row[date_key]
                    if isinstance(v, Timestamp):
                        row[date_key] = v.strftime("%Y-%m-%d")
                    elif v in ["None", "<NA>", "nan"]:
                        row[date_key] = None

            for k, v in list(row.items()):
                if v in ["None", "<NA>", "nan"]:
                    row[k] = None

        data = {
            "data": {
                "jobId": bulk_id,
                "datas": json_data
            }
        }

        response = requests.post(url, headers=headers, json=data)

        try:
            res_json = response.json()
            if response.status_code != 200:
                print(f" 批次 {current_batch_index + 1} 上傳失敗")
                print("Status:", response.status_code)
                print("Message:", json.dumps(res_json, indent=2, ensure_ascii=False))
            else:
                print(f" 批次 {current_batch_index + 1} 上傳成功，筆數：{len(json_data)}")
        except Exception as e:
            print(f" 批次 {current_batch_index + 1} 回傳格式錯誤")
            print("原始內容：", response.text)
            print("錯誤訊息：", str(e))

        all_responses.append(response)

    return all_responses



NOW = datetime.now()
time_ranges = build_time_ranges(NOW)

base_info = kd.get_data_from_MSSQL(""" 
SELECT  [id] customItem48__c ,[data_region_name] 地區  ,[sap_company_id] name FROM [raw_data].[dbo].[crm_account_os] union all
SELECT  [id] customItem48__c ,[data_region_name] 地區 ,[sap_company_id] name FROM [raw_data].[dbo].[crm_account_tw] union all
SELECT  [id] customItem48__c ,[data_region_name] 地區  ,[sap_company_id] name FROM [raw_data].[dbo].[crm_account_cn]
 """)


user_df = kd.get_data_from_CRM(""" SELECT id ownerId, name 地區 FROM user """)
user_df["地區"] = user_df["地區"].str.replace("印度分公司-加盟", "印度分公司", regex=False)
base_info = pd.merge(base_info, user_df, on = '地區', how = 'left')

base_info[base_info['ownerId'].isna()]



for idx, r in enumerate(time_ranges):
    START_DATE = datetime.combine(r["start"], datetime.min.time())
    END_DATE   = datetime.combine(r["end"],   datetime.min.time())

    START_STR = START_DATE.strftime('%Y-%m-%d')
    END_STR   = END_DATE.strftime('%Y-%m-%d')

    START_TS = int(START_DATE.timestamp() * 1000)
    END_TS   = int(END_DATE.timestamp() * 1000)

    print(f"[RUN] {r['rule']} | {START_STR} ~ {END_STR}")

    CRM_sales = kd.get_data_from_CRM(f"""
    SELECT id dataId,
    customItem4__c 預計發貨日期,
    customItem8__c 買方說明,
    customItem18__c 未税本位币,
    customItem46__c CRM公司代號,
    customItem58__c 物料群組,
    name buyer,customItem46__c 公司代號,
    customItem5__c 銷售檔案號碼,
    customItem14__c 銷項交貨單號,
    createdAt
    FROM customEntity34__c
    where customItem4__c >= {START_TS}  AND customItem4__c < {END_TS}
    """)

    if not CRM_sales.empty:
        kd.delete_from_CRM(CRM_sales, object_name="customEntity34__c")
        print(f"刪除 CRM：{len(CRM_sales)} 筆")

    sap_sales = kd.get_data_from_MSSQL(f"""
    SELECT 
        KUNAG               AS name,                 -- 買方
        VGBEL               AS customItem5__c,        -- 銷售檔案號碼
        WADAT_DATETIME      AS customItem4__c,        -- 預計發貨日期

        NAME1_R             AS customItem8__c,        -- 買方說明
        ZZCRM_CUSTOMER      AS customItem46__c,       -- CRM公司代號

        ADTXT               AS customItem12__c,       -- 送貨地址
        STR_SUPPL2          AS customItem13__c,       -- 送貨地址3
        VBELN               AS customItem14__c,       -- 銷項交貨單號
        LFIMG               AS customItem50__c,       -- 交貨數量
        LFDAT               AS customItem95__c,       -- 交貨日期
        WADAT_IST           AS customItem51__c,       -- 過賬日期
        VTEXT4              AS customItem40__c,       -- 出貨點說明

        WAERK               AS customItem20__c,       -- 幣別
        PRICE_L             AS customItem17__c,       -- 單價本位幣
        NETWR_L             AS customItem18__c,       -- 未稅本位幣
        MWSBP_L             AS customItem19__c,       -- 稅額本位幣
        AMOUNT_L            AS customItem21__c,       -- 金額本位幣
        KBETR               AS customItem30__c,       -- 折扣

        XBLNR               AS customItem36__c,       -- 發票號碼
        NAME1_G             AS customItem42__c,       -- 發票抬頭
        VBELN_F             AS customItem49__c,       -- 請款單號

        MATNR               AS customItem38__c,       -- 物料
        MAKTX               AS customItem39__c,       -- 物料說明
        MATKL               AS customItem58__c,       -- 物料群組
        TDLIN4              AS customItem31__c,       -- 貨品需求

        ZZCRM_KEYMAN        AS customItem25__c,       -- CRM-Key man
        ZZCRM_KEYMAN_NAME   AS customItem24__c,       -- CRM-Key man 姓名
        ZZCRM_FOREMAN       AS customItem23__c,       -- CRM-專案經理/工
        ZZCRM_FOREMAN_NAME  AS customItem22__c,       -- CRM-專案經理/工頭姓名
        ZZCRM_DESIGNER      AS customItem27__c,       -- CRM-設計師
        ZZCRM_DESIGNER_NAME AS customItem26__c,       -- CRM-設計師姓名
        ZZCRM_ORDERER       AS customItem29__c,       -- CRM-訂貨人
        ZZCRM_ORDERER_NAME  AS customItem28__c,       -- CRM-訂貨人姓名

        PERNR_ZM            AS customItem67__c,       -- 業務主任代號
        PERNR_ZM_NAME       AS customItem66__c,       -- 業務主任

        VKBUR               AS customItem52__c,       -- 部門
        BEZEI               AS customItem68__c,       -- 部門說明
        KUNNR               AS customItem54__c,       -- 交貨方
        KONDA               AS customItem55__c,       -- 行業別

        CHARG               AS customItem9__c,        -- 批次
        NACHN               AS customItem43__c,       -- 姓
        VORNA               AS customItem44__c,       -- 名
        TDLIN1              AS customItem32__c,       -- TO 司機
        TDLIN2              AS customItem33__c,       -- 修改記錄
        TDLIN3              AS customItem56__c        -- TO 倉管
        -- ,VTEX08             as 地區

    FROM [raw_data].[dbo].[sap_sales_data_rfc]
    WHERE WADAT_DATETIME >= '{START_STR}'
      AND WADAT_DATETIME < '{END_STR}'
      AND KUNAG NOT LIKE '%CN%'
      AND KUNAG != 'TW00000'
      and NACHN is not null
    """)

    if sap_sales.empty:
        print("SAP 無資料，跳過")
        continue

    # sap_sales['地區'] = (sap_sales['地區'].astype(str).str.replace('台灣', 'TW', regex=False).str.replace('大陸', 'CN', regex=False)
    # .str.replace('區', '', regex=False).str.replace('区', '', regex=False))

    sap_sales_total = pd.merge(sap_sales, base_info, on="name", how="left").drop(columns=["地區"])
    sap_sales_total = sap_sales_total[sap_sales_total['ownerId'].notna()]

    sap_sales_total["entityType"] = 2966154709408113

    sap_sales_total["customItem48__c"] = (
        sap_sales_total["customItem48__c"]
        .where(pd.notna(sap_sales_total["customItem48__c"]), None)
        .astype("Int64")
    )

    bulk_id = kd.ask_bulk_id(object_name="customEntity34__c")
    insert_to_CRM(bulk_id, sap_sales_total)

    print(f"[DONE] {r['rule']} 完成\n")


    if idx < len(time_ranges) - 1:
        print("sleep 300 seconds before next range")
        time.sleep(300)


