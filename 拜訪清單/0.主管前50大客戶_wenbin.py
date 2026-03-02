
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

month_ago_2d = (datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_1d = (datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_2ts = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000


top_50 = kd.get_sap_with_relate_company(year_ago_1d)
top_50 = top_50.groupby('公司代號')['未稅本位幣'].sum().reset_index()

# 根據top_50銷貨數據所在公司,並查找公司的各維度數據
# and (customItem199__c like '%C%'  or customItem199__c like '%D%' )
all_company_ids = top_50['公司代號'].dropna().unique().tolist()
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
        and accountCode__c in {target_customer_str}
    '''
    try:
        df = kd.get_data_from_CRM(xoql)
        account_df = pd.concat([account_df, df], ignore_index=True, sort=False)
    except Exception as e:
        print(f"Error on batch {i // batch_size + 1}: {e}")

K_invite = pd.merge(top_50, account_df, on = '公司代號', how = 'inner')
K_invite = K_invite[~K_invite['資料區域群組名稱'].astype(str).str.contains("TW-Z")]


# 增加區域數據
區域業務 = pd.read_excel("Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/91.台灣業務負責區域表/台灣業務負責區域_2026.xlsx", dtype='object',sheet_name='區域表(報表用)',skiprows=1 ).ffill()
區域業務 = 區域業務[['小區主管','小區','大區']]
區域業務 = 區域業務[~區域業務['小區'].astype(str).str.contains('TW-Z', na=False) &區域業務['小區'].notna()]
K_invite = pd.merge(K_invite, 區域業務, left_on = '資料區域群組名稱', right_on = '小區', how = 'left')


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
 

# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts)
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')


# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite)
K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')

target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]
K_invite = K_invite[K_invite['大區'].notna()].copy()
K_invite['未稅本位幣'] = pd.to_numeric(K_invite['未稅本位幣'], errors='coerce').fillna(0).astype(int)
K_invite['Rank'] = K_invite.groupby('大區')['未稅本位幣'].rank(method='first', ascending=False).astype(int)
K_invite = K_invite[K_invite['Rank'] <= 50].copy()
K_invite = K_invite.sort_values(by=['大區', 'Rank'])
K_invite = K_invite.sort_values(by='日期', ascending=True, na_position='first').rename(columns={"小區主管": "執行人"})


# 主旨欄位：銷貨50大排行榜-{RANK}
K_invite['主旨'] = '銷貨50大排行榜-' + K_invite['Rank'].astype(str)



path = f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{str((datetime.today()+relativedelta(months=1)).date().strftime("%Y.%m"))}/"
K_invite.to_excel(path + "主管前50大客戶_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
print('數據導出成功~')
