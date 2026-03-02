
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

year_ago_3d = (datetime.today() - relativedelta(years=3) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_2ts = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(years=3) + relativedelta(days=1)).date()).timestamp()*1000


# select from trackingrecord  -->  客訴數據,最近3年
complaint = kd.get_data_from_CRM(
            f'''
                    select
                    name 客訴代號,
                    customItem40__c 資料日期,
                    customItem4__c 工作類別,
                    accountCode__c 公司代號
                    from customEntity15__c
                    where customItem118__c like '%TW%' and (customItem4__c = 12 or customItem168__c in ('C3','C3-1','C3-2'))
                    and customItem40__c >= {year_ago_3ts}
            ''')

kd.convert_to_date(complaint,"資料日期")
complaint = complaint.sort_values('資料日期', ascending=False).drop_duplicates(subset=['公司代號'], keep='first')

# 關聯公司
complaint = kd.merge_company_to_parent(complaint)

# 根據客訴數據找到所在公司,並查找公司的各維度數據
all_company_ids = complaint['公司代號'].dropna().unique().tolist()
batch_size = 100
account_df = pd.DataFrame()

for i in range(0, len(all_company_ids), batch_size):
    batch_ids = all_company_ids[i:i + batch_size]
    target_customer_str = "(" + ",".join(f"'{c}'" for c in batch_ids) + ")"

    xoql = f'''
        select accountCode__c 公司代號, customItem202__c 公司地址, dimDepart.departName 資料區域群組名稱,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號, accountName 公司名稱,customItem322__c 目標客戶類型,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉
        from account
        where dimDepart.departName like '%TW%' and (customItem199__c like '%C%' or customItem199__c like '%D%')
        and accountCode__c in {target_customer_str}
    '''

    try:
        df = kd.get_data_from_CRM(xoql)
        account_df = pd.concat([account_df, df], ignore_index=True, sort=False)
    except Exception as e:
        print(f"Error on batch {i // batch_size + 1}: {e}")

K_invite = pd.merge(complaint, account_df, on = '公司代號', how = 'inner')


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


#   排除近2個月拜訪, K大過
pass_visited =  kd.last_connected(month_ago_2ts, first='all')
K_invite = K_invite[~K_invite['公司代號'].isin(set(pass_visited['公司代號']) )]


# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts)
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')

# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite)


# from 銷貨
sap = kd.get_sap_with_relate_company(year_ago_3d)
K_invite['資料日期'] = pd.to_datetime(K_invite['資料日期'])
sap['預計發貨日期'] = pd.to_datetime(sap['預計發貨日期'])


# 每家公司最晚投訴日
complaint_latest = K_invite.groupby('公司代號')['資料日期'].max().reset_index()
complaint_latest.columns = ['公司代號', '最近投訴日期']

# inner join 獲取投訴後下單的
sap_merge = pd.merge(sap, complaint_latest, left_on='公司代號', right_on='公司代號', how='inner')

# 篩選出在投訴日期之後還有下單的, 反向篩選那些沒有再回購的公司
came_back = sap_merge[sap_merge['預計發貨日期'] > sap_merge['最近投訴日期']]['公司代號'].unique()
no_return = complaint_latest[~complaint_latest['公司代號'].isin(came_back)]


K_invite = K_invite[K_invite['公司代號'].isin(no_return['公司代號'])]

# # 關聯公司
# K_invite = kd.merge_company_to_parent(K_invite, kd)
# K_invite = K_invite[K_invite['公司代號'].isin(account_df['公司代號'])]

K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
K_invite['公司代號'].value_counts()
K_invite = K_invite.sort_values(by='日期', ascending=True, na_position='first')

folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}")
filename = f"客訴後未交易客戶_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
K_invite.to_excel(folder / filename, index=False)
print('數據導出成功~')
