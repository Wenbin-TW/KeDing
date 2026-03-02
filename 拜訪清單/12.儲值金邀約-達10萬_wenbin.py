
import pandas as pd
import pyodbc
import json
import requests
from datetime import datetime, timedelta, date
import pymysql
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil
import os
import common  as kd
from pathlib import Path

# 模擬今天為 2025 年 6 月 20 日,如果超過20號, 需要回溯到20號
# mock_today = datetime(2025, 9, 20)
mock_today = datetime.today()

month_ago_2d = (mock_today - relativedelta(months=2) ).date().strftime("%Y/%m/%d")
month_ago_1d = (mock_today - relativedelta(months=1) ).date().strftime("%Y/%m/%d")
year_ago_1d = (mock_today - relativedelta(years=1) ).date().strftime("%Y/%m/%d")
month_ago_3ts = pd.to_datetime((mock_today - relativedelta(months=3) ).date()).timestamp() * 1000
year_ago_1ts = pd.to_datetime((mock_today - relativedelta(years=1) ).date()).timestamp() * 1000

# 銷貨數據
#total_sales= kd.get_sap_with_relate_company(month_ago_2d)
total_sales = kd.get_data_from_MSSQL(f'''
        SELECT buyer as SAP公司代號, taxfree_basecurr as 未稅本位幣,  planned_shipping_date as 預計發貨日期 
        FROM sap_sales_data 
        WHERE  buyer LIKE 'TW%' 
            AND planned_shipping_date >= '{month_ago_2d}'  AND planned_shipping_date < '{mock_today}'
    ''')

total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])

target_summary = total_sales.groupby('SAP公司代號')['未稅本位幣'].sum().reset_index()
target_summary = target_summary[target_summary['未稅本位幣'] / 2 >= 100000]



# 根據target_summary銷貨數據所在公司,並查找公司的各維度數據
all_company_ids = target_summary['SAP公司代號'].dropna().unique().tolist()
batch_size = 100
account_df = pd.DataFrame()

for i in range(0, len(all_company_ids), batch_size):
    batch_ids = all_company_ids[i:i + batch_size]
    target_customer_str = "(" + ",".join(f"'{c}'" for c in batch_ids) + ")"

    xoql = f'''
        select accountCode__c 公司代號, customItem202__c 公司地址, dimDepart.departName 資料區域群組名稱,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號, accountName 公司名稱, customItem322__c 目標客戶類型,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉
        from account
        where dimDepart.departName like '%TW%' and (customItem199__c like '%C%'  or customItem199__c like '%D%' )
        and SAP_CompanyID__c in {target_customer_str}
    '''

    try:
        df = kd.get_data_from_CRM(xoql)
        account_df = pd.concat([account_df, df], ignore_index=True, sort=False)
    except Exception as e:
        print(f"Error on batch {i // batch_size + 1}: {e}")

K_invite = pd.merge(target_summary, account_df, on = 'SAP公司代號', how = 'inner')


# select related_contact 客關連數據
contact_related = kd.get_data_from_CRM(
            f'''
            select name 客戶關係聯絡人代號, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 聯絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人
            from customEntity22__c 
            where customItem37__c  like '%TW%'
            ''')

K_invite = pd.merge(K_invite, contact_related, on = '公司代號', how = 'inner')


# 剩餘儲值金大於0的都排除
stored_value_file = r"Z:\18_各部門共享區\03_台灣事業部\6.訊息公佈欄\1.每日帳款查詢"
stored_value_keyword = "儲值金餘額"
stored_value_path = kd.get_latest_excel(stored_value_file, stored_value_keyword)
stored_value_df = pd.read_excel(stored_value_path, sheet_name="業務觀看用", header=1)
stored_value_df = stored_value_df[stored_value_df['剩餘儲值金\n(含稅)']>0]
# 找到關聯公司, 排除---更改
stored_value_df = stored_value_df.rename(columns={'客戶編號': 'SAP公司代號'})
# stored_value_df = kd.add_relate_company(stored_value_df,"SAP")
K_invite = K_invite[~K_invite['SAP公司代號'].isin(set(stored_value_df['SAP公司代號']))]


# # 欄位"專案到期日"中的日期，大於當前日期時，排除
# project_file = r"Z:\18_各部門共享區\01_會計課\01_會計部報表\06_專案彙總表(製表人-銷管助理)"
# project_keyword = "專案價彙總表"
# project_path = kd.get_latest_excel(project_file, project_keyword)
# project_df = pd.read_excel(project_path, sheet_name="專案價總表")
# today = datetime.today().date()
# def parse_date_or_future(val):
#     val_str = str(val).strip()
#     if val_str == '無期限':
#         return date(2999, 12, 31)
#     try:
#         return pd.to_datetime(val_str).date()
#     except:
#         return None  
# project_df['專案到期日_parsed'] = project_df['專案到期日'].apply(parse_date_or_future)
# project_df = project_df[project_df['專案到期日_parsed'] > today].copy()
# project_df = pd.merge(project_df, company_map,left_on='客代', right_on='SAP公司代號', how='left')
# K_invite = K_invite[~K_invite['公司代號'].isin(set(stored_value_df['關聯公司']) )]


# 欄位"專案到期日"中的日期，大於當前日期時，排除
# 也就是只保留「專案已到期」的公司
today = datetime.today().date()
start_date = (today.replace(day=1) - relativedelta(months=1)).replace(day=21)
end_date = (today.replace(day=1) + relativedelta(months=1)).replace(day=1)
project_file = r"Z:\18_各部門共享區\01_會計課\01_會計部報表\06_專案彙總表(製表人-銷管助理)"
project_keyword = "專案價彙總表"
project_path = kd.get_latest_excel(project_file, project_keyword)
project_df = pd.read_excel(project_path, sheet_name="專案價總表")
today = datetime.today().date()
def parse_date_or_future(val):
    val_str = str(val).strip()
    if val_str == '無期限':
        return date(2999, 12, 31)
    try:
        return pd.to_datetime(val_str).date()
    except:
        return None  

project_df['專案到期日_parsed'] = project_df['專案到期日'].apply(parse_date_or_future)
future_project = project_df[project_df['專案到期日_parsed'] >= end_date].copy()
project_df = project_df[(project_df['專案到期日_parsed'] >= start_date) &(project_df['專案到期日_parsed'] < end_date)  ].copy()
project_df = project_df.rename(columns={'客代': 'SAP公司代號'})
# project_df = kd.add_relate_company(project_df,"SAP")



latest_project = project_df.sort_values('專案到期日_parsed', ascending=False).drop_duplicates('SAP公司代號')
K_invite = pd.merge(K_invite, latest_project[['SAP公司代號', '專案到期日_parsed']], on='SAP公司代號', how='left')
K_invite = K_invite.rename(columns={'專案到期日_parsed': '專案到期日'})
K_invite = K_invite[~K_invite['SAP公司代號'].isin(future_project['客代'])].copy()


# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts,merge_type='alone')
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')

# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite,data_type="stored_value")
K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]
K_invite['公司代號'].value_counts()
K_invite = K_invite.sort_values(by='日期', ascending=True, na_position='first')

path = fr"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\★每月業務電拜訪★\共用CRM資訊\Temp\電拜訪清單資料\儲值金\儲值金名單\{str((datetime.today()).date().strftime("%Y-%m"))}\\"
os.makedirs(path, exist_ok=True) # ← 若路徑不存在就建立（含所有父層）
K_invite.to_excel(path + "CD類儲值金客戶_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
print('數據導出成功~')
