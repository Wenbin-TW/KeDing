
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os
import urllib
from dotenv import load_dotenv

load_dotenv()

params = urllib.parse.quote_plus(
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)

engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)
excel_path = r"Z:\18_各部門共享區\03_台灣事業部\6.訊息公佈欄\★各類報表查詢★\★K大約訪統計表★\TO佳佳數據資料夾\9.業務管理(製表人：郭佳佳)每月5號下班前完成1009---別動.修改要找++.xlsm"
sheet_name = "資料"
start_row = 457
end_row = 529
overseas_regions = ['SG', 'HK', 'PH', 'ID', 'VN', 'TH']
df_raw = pd.read_excel(excel_path, sheet_name=sheet_name, header=2)
df_partial = df_raw.iloc[start_row:end_row].copy()
columns = df_partial.columns
col_keep = [columns[2], columns[3]]  # 地區、說明
today = datetime.today()
prev_month = today.replace(day=1) - pd.DateOffset(months=1)
start_month = pd.to_datetime("2024-04")
valid_months = []

for col in columns[4:]:
    try:
        col_date = pd.to_datetime('20' + col, format='%Y-%m')
        if start_month <= col_date <= prev_month:
            col_keep.append(col)
            valid_months.append(col)
    except:
        continue
df_partial = df_partial[col_keep].copy()
df_partial.rename(columns={df_partial.columns[0]: '地區',
                           df_partial.columns[1]: '說明'}, inplace=True)
df_partial = df_partial[df_partial['地區'] != '海外']
df_domestic = df_partial[df_partial['地區'].isin(['台灣', '大陸'])]
df_foreign_detail = df_partial[df_partial['地區'].isin(overseas_regions)]
df_long_foreign = df_foreign_detail.melt(id_vars=['地區', '說明'],
                                         value_vars=valid_months,
                                         var_name='月份',
                                         value_name='數值')
df_long_foreign['月份'] = '20' + df_long_foreign['月份']
df_foreign_sum = df_long_foreign.groupby(['月份', '說明'], as_index=False)['數值'].sum()
df_foreign_sum['地區'] = '海外'
df_wide_foreign_sum = df_foreign_sum.pivot(index=['地區', '月份'],
                                           columns='說明',
                                           values='數值').reset_index()
df_long_keep = pd.concat([df_domestic, df_foreign_detail], ignore_index=True)
df_long_keep = df_long_keep.melt(id_vars=['地區', '說明'],
                                 value_vars=valid_months,
                                 var_name='月份',
                                 value_name='數值')
df_long_keep['月份'] = '20' + df_long_keep['月份']
df_wide_keep = df_long_keep.pivot(index=['地區', '月份'],
                                  columns='說明',
                                  values='數值').reset_index()
df_all = pd.concat([df_wide_keep, df_wide_foreign_sum], ignore_index=True)
df_all.columns.name = None

df_all = df_all.fillna(0)
rename_dict = {
    '公司應普查家數': '公司應普查家數',
    '公司普查完成家數': '公司普查完成家數',
    '公司普查%': '公司普查百分比',
    '聯絡人應普查人數': '聯絡人應普查人數',
    '聯絡人普查完成人數': '聯絡人普查完成人數',
    '聯絡人普查%': '聯絡人普查百分比'
}
df_all.rename(columns=rename_dict, inplace=True)
df_all['公司普查百分比'] = df_all['公司普查完成家數'] / df_all['公司應普查家數']
df_all['聯絡人普查百分比'] = df_all['聯絡人普查完成人數'] / df_all['聯絡人應普查人數']
df_all['公司普查百分比'] = df_all['公司普查百分比'].fillna(0)
df_all['聯絡人普查百分比'] = df_all['聯絡人普查百分比'].fillna(0)
df_all['查詢時間'] = datetime.now()
print(f"料整理完成，共 {len(df_all)} 筆")


rename_dict = {
    '地區': 'region',
    '月份': 'month',
    '公司應普查家數': 'expected_company_count',
    '公司普查百分比': 'company_completion_rate',
    '公司普查完成家數': 'completed_company_count',
    '聯絡人應普查人數': 'expected_contact_count',
    '聯絡人普查百分比': 'contact_completion_rate',
    '聯絡人普查完成人數': 'completed_contact_count',
    '查詢時間': 'query_time'  
}


df_all.rename(columns=rename_dict, inplace=True)


with engine.connect() as connection:
    df_all.to_sql(name='monthly_census', con=connection, if_exists='replace', index=False)
    print(f"共 {len(df_all)} 筆資料已成功寫入 SQL → [monthly_census]")


 




















         