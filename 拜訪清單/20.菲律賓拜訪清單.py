

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
import re

month_ago_7d = (datetime.today() - relativedelta(months=7) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_6d = (datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_3d = (datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_2d = (datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_5d = (datetime.today() - relativedelta(years=5) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_6ts = pd.to_datetime((datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000




##### 信用管制 #####
invalid_company = kd.get_data_from_MSSQL('select *   FROM [clean_data].[dbo].[crm_account_invalid] ')

##### 客戶 #####
select_query = f'''
SELECT accountCode__c 公司代號
, accountName 公司全名
, SAP_CompanyID__c sap公司代號
, dimDepart.departName 資料區域名稱
, customItem202__c 公司地址
, region__c.customItem9__c 是否主營
, customItem322__c 目標客戶類型
, customItem198__c.name 公司型態
, SAP_CompanyID__c sap公司代號
, customItem278__c 倒閉無效
, customItem291__c 勿擾選項
, createdAt 創建日期
FROM account
where dimDepart.departName like 'PH%'
'''
account = kd.get_data_from_CRM(select_query)
data_account = account.copy()

cols = [  '公司全名', '目標客戶類型', '倒閉無效', '勿擾選項', '是否主營']
data_account[cols] = data_account[cols].astype(str) .replace(['None', 'nan'], '')
kd.convert_to_date(data_account,'創建日期')
data_account = data_account.drop_duplicates('公司代號')

company_name = ['倒閉', '歇業', '停業', '轉行', '退休', '過世', '燈箱', '群組', '支援', '留守', '教育訓練', '無效拜訪', '資料不全', '搬遷', '廢止', 
                '解散', '管制', '非營業中', 'c>']

data_account = data_account.loc[(
    (~data_account['公司全名'].str.contains('|'.join(company_name), na=False)) &
    (~data_account['倒閉無效'].str.contains('是')) &
    (~data_account['資料區域名稱'].str.contains('INV|Others')) &
    ~data_account['勿擾選項'].str.contains('勿拜訪', na=False) &
    ~data_account['公司地址'].str.contains(r'\(x\)', na=False) &
     data_account['公司型態'].astype(str).str.contains(r'[CD]', na=False) 
         & (~data_account['是否主營'].str.contains('否'))
    )]

data_account = data_account.loc[~data_account['sap公司代號'].isin(invalid_company['company_id'])]

########## 客戶關係聯絡人 ##########
select_query = f'''
SELECT customItem8__c 公司代號
, customItem24__c 關係狀態
, customItem50__c 空號
, customItem51__c 停機
, customItem42__c 聯絡人資料無效
, customItem89__c 號碼錯誤非本人
, dimDepart.departName 資料區域名稱
, customItem95__c 職務類別
FROM customEntity22__c
where dimDepart.departName like 'PH%'
'''
rel_contact = kd.get_data_from_CRM((select_query))
data_rel_contact = rel_contact.copy()

def clean_str(s):
    return re.sub(r"[\[\]'\"\(\)]", "", s)

cols = ['關係狀態','號碼錯誤非本人','聯絡人資料無效','職務類別']
data_rel_contact[cols] = (  data_rel_contact[cols] .fillna('').astype(str) .applymap(clean_str))
data_rel_contact[['空號','停機']] = (  data_rel_contact[['空號','停機']] .fillna(0) .astype(int))

data_rel_contact = data_rel_contact.loc[
    data_rel_contact['關係狀態'].str.contains('在職') &
    (data_rel_contact['空號'] == 0) &
    (data_rel_contact['停機'] == 0) &
    (~data_rel_contact['聯絡人資料無效'].str.contains('是')) &
    (~data_rel_contact['號碼錯誤非本人'].str.contains('是'))]
data_rel_contact = data_rel_contact.drop_duplicates('公司代號')


########## 近兩個月拜訪 ##########
now = pd.Timestamp.now(tz="UTC")
two_months_ago = now - pd.DateOffset(months=2)

cont_query = f''' SELECT company_id, customers_type, region, visit_date FROM clean_data.dbo.crm_track_1year 
where visit_date >= '{two_months_ago}' 
and (region like 'PH-%')
and (customers_type = 'A1 拜訪' OR customers_type = 'C2 視訊拜訪')'''
track = kd.get_data_from_MSSQL(cont_query)
data_track = track.copy()

########## 近兩個月K大 ##########
now = pd.Timestamp.now(tz="UTC")
two_months_ago = now - pd.DateOffset(months=2)

cont_query = f''' SELECT company_id, region, visit_date, list_type, present_time
FROM clean_data.dbo.crm_K_3M 
WHERE region LIKE 'PH-%'
    AND list_type = 'K大預約表'
    AND visit_date >= '{two_months_ago}' 
    AND present_time >= 8'''
data_k_2M_orgi = kd.get_data_from_MSSQL(cont_query)
data_k_2M = data_k_2M_orgi.copy()

# ########## 近三個月電訪 ##########
# now = pd.Timestamp.now(tz="UTC")
# three_months_ago = now - pd.DateOffset(months=3)

# cont_query = f''' SELECT company_id, customers_type, region, visit_date FROM clean_data.dbo.crm_track_1year 
# where visit_date >= '{three_months_ago}' 
# and (region like 'PH-%')
# and (customers_type = 'B2 電訪-無效')'''
# tel_track = kd.get_data_from_MSSQL(cont_query)
# tel_track = tel_track.copy()





# 近半年未叫貨  剔除 兩個月內 拜訪 K大
total_sales= kd.get_sap_with_relate_company(month_ago_7d,location = 'PH')
total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])
sales_month_7_to_6 = total_sales[(total_sales['預計發貨日期'] >= month_ago_7d) &(total_sales['預計發貨日期'] < month_ago_6d)]
sales_last_6_months = total_sales[total_sales['預計發貨日期'] >= month_ago_6d]
target_summary = sales_month_7_to_6.groupby('公司代號')['未稅本位幣'].sum().reset_index()
sap_ids_last6 = set(sales_last_6_months['公司代號'].unique())
sales_nobuy_after = target_summary[~target_summary['公司代號'].isin(sap_ids_last6)]

K_invite = pd.merge(data_account[~data_account['公司代號'].isin(set(data_track['company_id'].dropna()) |  # 拜訪
                                                             set(data_k_2M['company_id'].dropna()) )],   # K大
                                data_rel_contact[['公司代號','關係狀態']], on = '公司代號', how = 'inner')
K_invite = pd.merge(sales_nobuy_after, K_invite, on = '公司代號', how = 'inner')

K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]

folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}/raw")
folder.mkdir(parents=True, exist_ok=True)
filename = f"PH_近半年未叫貨_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
K_invite.to_excel(folder / filename, index=False)
print(f'{filename}數據導出成功~')





# # 5年有交易  剔除 兩個月內 拜訪 K大
# total_sales= kd.get_sap_with_relate_company(year_ago_5d,location = 'PH')
# total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])

# target_contact = data_rel_contact[~data_rel_contact['關係狀態'].astype(str).str.contains("配合|離職")]
# target_contact = target_contact[target_contact['職務類別'].astype(str).str.contains("001|002|003|004|005|006|007|010|011|015", na=False)]
# target_contact['職務類別'] = target_contact['職務類別'].astype('string')
# K_invite = pd.merge(data_account[~data_account['公司代號'].isin(set(data_track['company_id'].dropna()) |  # 拜訪
#                                                              set(data_k_2M['company_id'].dropna()) )],   # K大
#                      target_contact[['公司代號','關係狀態','職務類別']], on = '公司代號', how = 'inner')
# K_invite = pd.merge(total_sales, K_invite, on = '公司代號', how = 'inner')

# K_invite = (  K_invite .sort_values('職務類別', ascending=True) .drop_duplicates('公司代號', keep='first'))
# target = ['經營客戶']
# K_invite = K_invite[~K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)].drop(columns=['未稅本位幣','未稅金額_台幣','預計發貨日期'], errors='ignore')

# folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}/raw")
# filename = f"PH_5年有交易的開發_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
# K_invite.to_excel(folder / filename, index=False)
# print(f'{filename}數據導出成功~')



# # 高交易額客戶二個月未拜訪   剔除 兩個月內 拜訪
# total_sales= kd.get_sap_with_relate_company(month_ago_6d,location = 'PH')
# total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])
# target_summary = total_sales.groupby('公司代號')['未稅本位幣'].sum().reset_index()
# target_summary = target_summary[target_summary['未稅本位幣'] > 9000]

# K_invite = pd.merge(data_account[~data_account['公司代號'].isin(set(data_track['company_id'].dropna()) )],  # 拜訪
#                      data_rel_contact[['公司代號','關係狀態']], on = '公司代號', how = 'inner')
# K_invite = pd.merge(target_summary, K_invite, on = '公司代號', how = 'inner')

# K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
# target = ['經營客戶']
# K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]

# folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}/raw")
# filename = f"PH_高交易額客戶二個月未拜訪_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
# K_invite.to_excel(folder / filename, index=False)
# print(f'{filename}數據導出成功~')



# 曆史銷貨排行 剔除 兩個月內 拜訪 K大 交易
sales_in_2month = kd.get_sap_with_relate_company_os(month_ago_2d,location = 'PH')

total_sales= kd.get_sap_with_relate_company_os(year_ago_1d,location = 'PH')
total_sales['預計發貨日期'] = pd.to_datetime(total_sales['預計發貨日期'])
target_summary = total_sales.groupby('公司代號')['未稅本位幣'].sum().reset_index()

K_invite = pd.merge(data_account[~data_account['公司代號'].isin(set(data_track['company_id'].dropna()) |    # 拜訪
                                                            set(data_k_2M['company_id'].dropna()) |        # K大
                                                            set(sales_in_2month['公司代號'].dropna())  )],  # 交易
                                    data_rel_contact[['公司代號','關係狀態']], on = '公司代號', how = 'inner')

K_invite = pd.merge(target_summary, K_invite, on = '公司代號', how = 'inner')
K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]
K_invite = ( K_invite .sort_values('未稅本位幣', ascending=False) .groupby('資料區域名稱', as_index=False) .head(20)) # 每區前20

folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}/raw")
folder.mkdir(parents=True, exist_ok=True)
filename = f"PH_曆史銷貨排行_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
K_invite.to_excel(folder / filename, index=False)
print(f'{filename}數據導出成功~')

