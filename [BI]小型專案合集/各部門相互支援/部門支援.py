

import os
import sys
import urllib
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from dateutil.relativedelta import relativedelta
from math import ceil
import re

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd

load_dotenv()

params = urllib.parse.quote_plus(
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}")

engine = create_engine( f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)


def parse_excel_to_long_format(file_path):
    import pandas as pd

    print(f"正在解析：{file_path}")

    df = pd.read_excel(file_path, header=None)
    orig_row = df.iloc[0].ffill()
    supp_row = df.iloc[1].ffill()
    type_row = df.iloc[2]
    data = df.iloc[3:].reset_index(drop=True)

    records = []
    n_cols = df.shape[1]

    for col in range(1, n_cols):
        orig = str(orig_row.iloc[col]).strip()
        supp = str(supp_row.iloc[col]).strip()
        t = str(type_row.iloc[col]).strip()

        if orig in ["", "原部門"] or supp in ["", "支援部門"] or t not in ["人數", "時數"]:
            continue

        for i in range(len(data)):
            val = data.iloc[i, col]
            if pd.isna(val):
                continue

            date = data.iloc[i, 0]  

            records.append({
                "date": date,
                "origin_dept": orig,
                "support_dept": supp,
                "type": t,
                "value": val
            })

    long_df = pd.DataFrame(records)
    final_df = (
        long_df
        .pivot_table(
            index=["date", "origin_dept", "support_dept"],
            columns="type",
            values="value",
            aggfunc="sum"
        )
        .reset_index()
    )

    final_df.columns.name = None
    final_df = final_df.rename(columns={
        "人數": "staff_count",
        "時數": "work_hours"
    })
    return final_df



def load_recent_excel(folder_path):
    import glob
    import pandas as pd
    files = glob.glob(folder_path + r"\*部門支援統計*.xlsx")
    if not files:
        print("⚠ 沒找到含『部門支援統計』的 Excel")
        return pd.DataFrame()
    df_total = pd.DataFrame()
    for fp in files:
        try:
            df_tmp = parse_excel_to_long_format(fp)
            df_total = pd.concat([df_total, df_tmp], ignore_index=True)
            print(f"讀取成功：{fp}")
        except Exception as e:
            print(f"解析失敗（已跳過）{fp} → {e}")
    df_total["date"] = pd.to_datetime(df_total["date"], errors="coerce").dt.date
    cutoff = (datetime.today() - timedelta(days=7)).date()

    df_recent = df_total[df_total["date"] >= cutoff]
    print(f"➡ 共取得 {len(df_recent)} 筆最近 7 天資料（來源 {len(files)} 個檔案）")
    return df_recent


def delete_recent_7days(db_name, table_name):
    import os, urllib
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine, text

    params = urllib.parse.quote_plus(
        f"DRIVER={os.getenv('DB_DRIVER')};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={db_name};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')}")

    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)
    cutoff = (datetime.today() - timedelta(days=7)).date()
    with engine.begin() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE date >= :cutoff"),
            {"cutoff": cutoff})




if __name__ == "__main__":
    folder_path = r"Z:\18_各部門共享區\01_會計課\03_薪資相關\01_加班單\大埔美加班單\電子點名表"
    df_recent = load_recent_excel(folder_path)
    if len(df_recent) > 0:
        delete_recent_7days("clean_data", "dept_support_log")
        kd.add_relate_companywrite_to_sql(df_recent, "clean_data", "dept_support_log")
    print("完成每日 ETL")
