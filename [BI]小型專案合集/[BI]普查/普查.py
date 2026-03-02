import os
import sys
import re
import urllib
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import pytz
import pandas as pd
import win32com.client
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

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

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

yeaterday = pytz.timezone('Asia/Taipei').localize(datetime.now() - timedelta(days=1))
last_month_str = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')


df_census= kd.get_outlook_excel(yeaterday, keyword="普查數據看板", sheet_name="月度數據")
df_census = df_census.rename(columns={'欄欄位': '說明','值欄位': last_month_str})


df_census = df_census[df_census['地區'] != '合計']
df_census["地區"] = df_census["地區"].fillna(method="ffill")


overseas_regions = ['SG', 'HK', 'PH', 'ID', 'VN', 'TH']
df_census['地區'] = df_census['地區'].ffill().replace({'TW': '台灣', 'CN': '大陸'})
df_domestic = df_census[df_census['地區'].isin(['台灣', '大陸'])]
df_foreign_detail = df_census[df_census['地區'].isin(overseas_regions)]
df_long_foreign = df_foreign_detail.melt(id_vars=['地區', '說明'],
                                         var_name='月份',
                                         value_name='數值')
df_foreign_sum = df_long_foreign.groupby(['月份', '說明'], as_index=False)['數值'].sum()
df_foreign_sum['地區'] = '海外'
df_wide_foreign_sum = df_foreign_sum.pivot(index=['地區', '月份'],
                                           columns='說明',
                                           values='數值').reset_index()
df_long_keep = pd.concat([df_domestic, df_foreign_detail], ignore_index=True)
df_long_keep = df_long_keep.melt(id_vars=['地區', '說明'],
                                 var_name='月份',
                                 value_name='數值')
df_wide_keep = df_long_keep.pivot(index=['地區', '月份'],
                                  columns='說明',
                                  values='數值').reset_index()
df_all = pd.concat([df_wide_keep, df_wide_foreign_sum], ignore_index=True)
df_all.columns.name = None
df_all['公司普查率'] = df_all['公司普查完成家數'] / df_all['公司應普查家數']
df_all['聯絡人普查率'] = df_all['一年內普查完成聯絡人數'] / df_all['應普查聯絡人數']
df_all['公司普查率'] = df_all['公司普查率'].fillna(0)
df_all['聯絡人普查率'] = df_all['聯絡人普查率'].fillna(0)


df_all = df_all.fillna(0)
df_all['query_time'] = datetime.now()


rename_dict = {
    '地區': 'region',
    '月份': 'month',
    '公司應普查家數': 'expected_company_count',
    '公司普查完成家數': 'completed_company_count',
    '公司普查率': 'company_completion_rate',
    '應普查聯絡人數': 'expected_contact_count',
    '一年內普查完成聯絡人數': 'completed_contact_count',
    '聯絡人普查率': 'contact_completion_rate'}
df_all.rename(columns=rename_dict, inplace=True)



kd.write_to_sql(
    df=df_all,db_name='bi_ready',table_name='monthly_census',
    if_exists='update', dedup_keys=['region', 'month'], keep='new')
