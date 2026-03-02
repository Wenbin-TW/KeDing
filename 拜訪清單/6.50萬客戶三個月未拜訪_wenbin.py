
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
import common  as kd
from pathlib import Path

month_ago_3d = (datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_2d = (datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_6d = (datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_2d = (datetime.today() - relativedelta(years=2) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_2ts = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000

# 銷貨數據
total_sales = kd.get_sap_with_relate_company(year_ago_1d)
total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])

target_summary = total_sales.groupby('公司代號')['未稅本位幣'].sum().reset_index()
target_summary = target_summary[target_summary['未稅本位幣'] > 200000]



# 根據summary_merged銷貨數據所在公司,並查找公司的各維度數據
all_company_ids = target_summary['公司代號'].dropna().unique().tolist()
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

K_invite = pd.merge(target_summary, account_df, on = '公司代號', how = 'inner')


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


# 排除"近3個月內有拜訪、K大"
pass_visited =  kd.last_connected(month_ago_3ts, first='all')
K_invite = K_invite[~K_invite['公司代號'].isin(set(pass_visited['公司代號']) )]

# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts)
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')


# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite)
K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]
K_invite['公司代號'].value_counts()
K_invite = K_invite.sort_values(by='日期', ascending=True, na_position='first')


# 近一年業績區間切分
invite_50up = K_invite[K_invite['未稅本位幣'] > 500000].copy()
invite_20_50 = K_invite[(K_invite['未稅本位幣'] >= 200000) & (K_invite['未稅本位幣'] <= 500000)].copy()



# path C:\Users\TW0002.TPTWKD\Desktop
path = f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{str((datetime.today()+relativedelta(months=1)).date().strftime("%Y.%m"))}/"
invite_50up.to_excel(path + "近1年50萬以上客戶三個月未拜訪_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
invite_20_50.to_excel(path + "近1年20-50萬客戶三個月未拜訪_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
print('數據導出成功~')
