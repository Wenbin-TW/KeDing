
###############################    contact類型,不找關聯公司,只要最佳聯絡人
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

year_ago_5d = (datetime.today() - relativedelta(years=5) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_3d = (datetime.today() - relativedelta(years=3) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_2ts = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
day_ago_45ts = pd.to_datetime((datetime.today() - relativedelta(days=45) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000


# 銷貨數據
#total_sales = kd.get_sap_with_relate_company(year_ago_5d)

total_sales = kd.get_data_from_MSSQL(f'''
        SELECT buyer as SAP公司代號, taxfree_basecurr as 未稅本位幣,  planned_shipping_date as 預計發貨日期 
        FROM sap_sales_data 
        WHERE  buyer LIKE 'TW%' 
        AND planned_shipping_date >= '{year_ago_5d}'
    ''')

target_summary = total_sales.groupby('SAP公司代號')['未稅本位幣'].sum().reset_index()


# 根據最近5年的銷貨數據,找到所在公司的各維度數據
all_company_ids = target_summary['SAP公司代號'].dropna().unique().tolist()
batch_size = 100
account_df = pd.DataFrame()

for i in range(0, len(all_company_ids), batch_size):
    batch_ids = all_company_ids[i:i + batch_size]
    target_customer_str = "(" + ",".join(f"'{c}'" for c in batch_ids) + ")"

    xoql = f'''
        select accountCode__c 公司代號, customItem202__c 公司地址, dimDepart.departName 資料區域群組名稱,customItem202__c 公司地址,
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

# 只保留開發客戶
target = ['經營客戶']
K_invite = K_invite[~K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]

# 排除近45天拜訪過,K大過
pass_visited =  kd.last_connected(day_ago_45ts, first='all',merge_type="alone")
K_invite = K_invite[~K_invite['公司代號'].isin(set(pass_visited['公司代號']) )]

# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts,merge_type="alone")
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')

# 剔除配合的關係聯絡人
K_invite = K_invite[~K_invite['關係狀態'].astype(str).str.contains("配合")]

target = ['經營客戶']
K_invite = K_invite[~K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]

# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite,data_type="contact")

# 篩選最佳聯絡人
K_invite = kd.best_contact(K_invite)
K_invite = K_invite.sort_values(by='日期', ascending=True, na_position='first')


company_map = kd.get_data_from_MSSQL(f'''SELECT  company_id ,company_id_parent 關聯公司代號
           FROM [raw_data].[dbo].[crm_related_company]''')

K_invite = pd.merge(K_invite, company_map, left_on = '公司代號', right_on  = 'company_id', how='left')

# 建偏好旗標：公司代號 等於 關聯公司代號 → True（優先保留）
K_invite = K_invite.assign(
    _prefer=(K_invite['公司代號'].fillna('').astype(str).str.strip()
             == K_invite['關聯公司代號'].fillna('').astype(str).str.strip())
)

# 按照手機號碼排序（先偏好、再日期）
K_sorted = K_invite.sort_values(by=['手機號碼', '_prefer', '日期'],
                                ascending=[True, False, True],
                                na_position='last')

# 找到每個手機號碼保留的第一筆
keep_index = K_sorted.drop_duplicates(subset=['手機號碼'], keep='first').index

# 建立一個布林欄位：True=被保留, False=被排除
K_sorted['_是否保留'] = K_sorted.index.isin(keep_index)

# 分出被排除的資料
excluded = K_sorted[~K_sorted['_是否保留']]

print(f"總共有 {len(excluded)} 筆被排除")
print(excluded[['手機號碼', '公司代號', '關聯公司代號', '日期']].head(20))

included = K_sorted[K_sorted['_是否保留']]



path = f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{str((datetime.today()+relativedelta(months=1)).date().strftime("%Y.%m"))}/"
included.to_excel(path + "五年內有交易之客戶_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
print('數據導出成功~')