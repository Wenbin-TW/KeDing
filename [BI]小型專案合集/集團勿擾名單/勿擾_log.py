
import sys
import time
import json
import ast
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd



year_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_three = pd.to_datetime((datetime.today() - relativedelta(years=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_one = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
three_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-3)).timestamp() * 1000)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
ENV_CONFIGS = [
    {
        "name": "TWOS",
        "base_url": "https://api-p10.xiaoshouyi.com/rest/data/v2.0/logs/xobjectFieldHistory/actions/query",
        "token": kd.get_access_token(),
        "targets": [
            {"objectApiKey": "account", "itemApiKey": "customItem291__c"},
            {"objectApiKey": "contact", "itemApiKey": "customItem219__c"}
        ]
    },
    {
        "name": "CN",
        "base_url": "https://api-scrm.xiaoshouyi.com/rest/data/v2.0/logs/xobjectFieldHistory/actions/query",
        "token": kd.get_access_token_ml(),
        "targets": [
            {"objectApiKey": "account", "itemApiKey": "customItem346__c"},
            {"objectApiKey": "contact", "itemApiKey": "customItem213__c"}
        ]
    }
]
def to_ms_start(date_str):
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d")) * 1000)

def to_ms_end(date_str):
    return int(time.mktime(time.strptime(date_str + " 23:59:59", "%Y-%m-%d %H:%M:%S")) * 1000)
def query_field_logs(
    base_url,
    access_token,
    objectApiKey,
    itemApiKey,
    startDate,
    endDate,
    sleep_sec=0.3,
    max_retry=3
):
    if not access_token:
        raise ValueError("access_token is empty")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json;charset=UTF-8"
    }

    startTime = to_ms_start(startDate)
    endTime = to_ms_end(endDate)

    page = 1
    all_rows = []
    error_logs = []

    while True:
        payload = {
            "pageNum": page,
            "objectApiKey": objectApiKey,
            "itemApiKey": itemApiKey,
            "startTime": startTime,
            "endTime": endTime
        }

        for attempt in range(1, max_retry + 1):
            try:
                resp = requests.post(base_url, json=payload, headers=headers, timeout=15)
                resp.raise_for_status()
                data = resp.json()
                break
            except Exception as e:
                error_logs.append({
                    "page": page,
                    "attempt": attempt,
                    "payload": payload,
                    "error": str(e)
                })
                if attempt == max_retry:
                    return pd.DataFrame(all_rows), pd.DataFrame(error_logs)
                time.sleep(1)

        if data.get("code") != "0":
            error_logs.append({
                "page": page,
                "payload": payload,
                "response": data
            })
            break

        result = data["result"]
        records = result.get("records", [])
        pages = result.get("pages", 1)

        all_rows.extend(records)

        if page >= pages:
            break

        page += 1
        time.sleep(sleep_sec)

    return pd.DataFrame(all_rows), pd.DataFrame(error_logs)
end_dt = datetime.today() - timedelta(days=1)
start_dt = end_dt - timedelta(days=6)

all_dfs = []
all_errs = []

current_dt = start_dt
while current_dt <= end_dt:
    day_str = current_dt.strftime("%Y-%m-%d")
    print(f"抓取日期：{day_str}")

    for cfg in ENV_CONFIGS:
        for t in cfg["targets"]:
            print(f"  {cfg['name']} | {t['objectApiKey']} | {t['itemApiKey']}")

            df_day, df_err = query_field_logs(
                base_url=cfg["base_url"],
                access_token=cfg["token"],
                objectApiKey=t["objectApiKey"],
                itemApiKey=t["itemApiKey"],
                startDate=day_str,
                endDate=day_str
            )

            if not df_day.empty:
                df_day["source_env"] = cfg["name"]
                df_day["objectApiKey"] = t["objectApiKey"]
                df_day["itemApiKey"] = t["itemApiKey"]
                all_dfs.append(df_day)

            if not df_err.empty:
                df_err["source_env"] = cfg["name"]
                df_err["objectApiKey"] = t["objectApiKey"]
                df_err["itemApiKey"] = t["itemApiKey"]
                all_errs.append(df_err)

    current_dt += timedelta(days=1)
df_logs = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
df_errors = pd.concat(all_errs, ignore_index=True) if all_errs else pd.DataFrame()

print(df_logs.head())
print("總筆數：", len(df_logs))
print("錯誤總筆數：", len(df_errors))

df_logs_copy = df_logs.copy()
kd.write_to_sql( df=df_logs_copy, db_name="clean_data", table_name="crm_not_disturb_log", if_exists="update",dedup_keys=["logId"])

not_disturb = kd.get_data_from_MSSQL('SELECT * from clean_data.[dbo].crm_not_disturb_log')
kd.convert_to_date(not_disturb,'createdAt' )
total_company_info = kd.get_data_from_CRM(f'''  
        select id, accountCode__c 公司代號,customItem291__c 公司勿擾選項,customItem226__c 建檔日期
        from account
 ''')
total_company = total_company_info.copy()
kd.convert_to_date(total_company,'建檔日期' )


def normalize_list(v):
    if v is None:
        return []
    if isinstance(v, float) and pd.isna(v):
        return []
    if isinstance(v, (list, tuple, set, np.ndarray)):
        return sorted([str(x).strip() for x in v if str(x).strip()])
    if isinstance(v, str):
        v = v.strip()
        if not v or v == "[]":
            return []
        try:
            parsed = ast.literal_eval(v)
            if isinstance(parsed, (list, tuple, set)):
                return sorted([str(x).strip() for x in parsed if str(x).strip()])
            return [str(parsed).strip()]
        except:
            return [v]
    return [str(v).strip()]



not_disturb["dataId"] = not_disturb["dataId"].astype(str)
not_disturb["created_dt"] = pd.to_datetime(not_disturb["createdAt"], errors="coerce")

logs = not_disturb[(not_disturb["itemApiKey"] == "customItem291__c") &
    (not_disturb["created_dt"].notna())].copy()

logs["old_norm"] = logs["oldValue"].apply(normalize_list)
logs["new_norm"] = logs["newValue"].apply(normalize_list)

logs = logs.sort_values(["dataId", "created_dt"])

total_company["id"] = total_company["id"].astype(str)
total_company["建檔日期"] = pd.to_datetime(total_company["建檔日期"], errors="coerce")
total_company["current_norm"] = total_company["公司勿擾選項"].apply(normalize_list)

company_created = total_company.set_index("id")["建檔日期"].to_dict()
company_code = total_company.set_index("id")["公司代號"].to_dict()
current_state = total_company.set_index("id")["current_norm"].to_dict()

logs_by_company = {
    cid: df for cid, df in logs.groupby("dataId")}

month_starts = pd.date_range(
    start="2024-11-01",
    end="2026-12-01",
    freq="MS")

rows = []

for snapshot_dt in month_starts:
    print(f"Processing snapshot {snapshot_dt.date()}")

    for _, row in total_company.iterrows():
        cid = row["id"]
        create_dt = row["建檔日期"]
        if pd.isna(create_dt) or create_dt > snapshot_dt:
            continue

        comp_logs = logs_by_company.get(cid)

        if comp_logs is None:
            nd_list = current_state.get(cid, [])
        else:
            past_logs = comp_logs[comp_logs["created_dt"] <= snapshot_dt]
            future_logs = comp_logs[comp_logs["created_dt"] > snapshot_dt]

            if not future_logs.empty:
                nd_list = future_logs.iloc[0]["old_norm"]
            elif not past_logs.empty:
                nd_list = past_logs.iloc[-1]["new_norm"]
            else:
                nd_list = current_state.get(cid, [])

        rows.append({
            "company_id": cid,
            "company_code": company_code.get(cid),
            "snapshot_month": snapshot_dt.strftime("%Y-%m-01"),
            "not_disturb_list": nd_list
        })


df_company_not_disturb_monthly = pd.DataFrame(rows)

print("完成，總筆數：", len(df_company_not_disturb_monthly))

df_company_not_disturb_monthly["not_disturb_list"] = (
    df_company_not_disturb_monthly["not_disturb_list"]
    .apply(lambda x: json.dumps(x, ensure_ascii=False)))

kd.write_to_sql(df=df_company_not_disturb_monthly, db_name="clean_data", table_name="crm_not_disturb_log_monthly", if_exists="replace")








