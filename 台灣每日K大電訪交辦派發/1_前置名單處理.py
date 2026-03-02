
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


today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
month_ago_six = pd.to_datetime((datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date()).timestamp()*1000


new_company = kd.get_data_from_CRM(f'''
        select accountCode__c 公司代號, customItem202__c 公司地址,id customItem11__c, dimDepart, dimDepart.departName 資料區域群組名稱,customItem226__c 建檔日期,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號,accountName 公司名稱,customItem322__c 目標客戶類型,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉,
        approvalStatus 審核狀態,customItem277__c 客戶付款類型,customItem311__c 公司公用標籤
        from account
        WHERE dimDepart.departName LIKE '%TW%'
''')
contact_related = kd.get_data_from_CRM(
            f'''
            select name, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 聯絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人,approvalStatus 審批狀態,customItem6__c 客關連建檔日期

            from customEntity22__c 
            where customItem37__c  like '%TW%'
            '''
            )

contact_related['客關連建檔日期'] = pd.to_numeric(contact_related['客關連建檔日期'], errors='coerce')
new_phone_set = set(  contact_related.dropna(subset=['手機號碼']).loc[contact_related['手機號碼'].astype(str).str.match(r'^09\d{8}$')]
    .groupby('手機號碼')['客關連建檔日期'].min().loc[lambda s: s >= month_ago_six].index)

contact_related['半年新建聯絡人'] = contact_related['手機號碼'].isin(new_phone_set).map({True:'是', False:'否'})

K_invite = pd.merge(new_company, contact_related, on = '公司代號', how = 'inner')

summary_df, cleaned_df, removed_df = kd.clean_invalid_entries_MRK(K_invite)
cleaned_df = cleaned_df.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
filename = fr"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\K大名單監控具體數據.xlsx"
cleaned_df[cleaned_df["公司型態"].astype(str).str.contains("[CD]")].to_excel(filename, index=False)


df = pd.concat([cleaned_df, removed_df], ignore_index=True)
for col in df.columns:
    df[col] = df[col].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
df['query_time'] = today
kd.convert_to_date(df,'建檔日期')
kd.convert_to_date(df,'客關連建檔日期')
kd.write_to_sql(df, 'bi_ready','crm_tw_contact_datail',  if_exists="replace")



mask_removed = removed_df[removed_df["聯絡人勿擾選項"].astype(str).str.contains("勿電訪", na=False)]

combined_annually = pd.concat([cleaned_df, mask_removed], ignore_index=True)
combined_annually['唯一識別'] = ( combined_annually['手機號碼'].str.strip().replace('', pd.NA) .combine_first(combined_annually['連絡人代號']))


combined_annually = combined_annually.drop_duplicates(subset=['唯一識別'], keep='first')




params = urllib.parse.quote_plus(
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}"
)
tomorrow = (datetime.today() + timedelta(days=1)).date()
tw_annually_value = len(combined_annually)

update_query = """
UPDATE [bi_ready].[dbo].[crm_valid_customer_daily]
   SET tw_annually = :tw_annually_value
 WHERE [Date] = :date_value;
"""

with engine.begin() as connection:
    connection.execute(
        text(update_query),
        {
            "tw_annually_value": tw_annually_value,
            "date_value": tomorrow
        }
    )


kd.賈維斯1號('前置名單梳理完畢, 並導出數據🎉🎉🎉')