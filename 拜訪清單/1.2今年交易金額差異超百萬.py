
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

month_ago_7d = (datetime.today() - relativedelta(months=7) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_6d = (datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
month_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_6ts = pd.to_datetime((datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date()).timestamp()*1000
year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
jan_first = datetime.today().replace(month=1, day=1).date().strftime("%Y/%m/%d")
last_jan_first = (datetime.today().replace(month=1, day=1) - relativedelta(years=1)).strftime("%Y/%m/%d")



def get_sap_with_relate_company(year_ago_3d):
    # 取得 SAP 銷售資料
    sap = kd.get_data_from_MSSQL(f'''
        SELECT buyer as SAP公司代號, taxfree_basecurr as 未稅本位幣,  planned_shipping_date as 預計發貨日期 
        FROM sap_sales_data 
        WHERE  buyer LIKE 'TW%'
        AND planned_shipping_date >= '{year_ago_3d}'
    ''')
    sap['預計發貨日期'] = pd.to_datetime(sap['預計發貨日期'], errors='coerce')

    # 取得公司對應表，只取需要欄位，避免名稱衝突
    company_map = kd.get_data_from_MSSQL('''
                        SELECT  company_id 公司代號
                            ,sap_company_id SAP公司代號
                            ,company_id_parent 關聯公司
                        FROM [raw_data].[dbo].[crm_related_company]
    ''')

    # 合併後取代公司代號
    merged = pd.merge(sap, company_map, on='SAP公司代號', how='left')
    merged['公司代號'] = merged['關聯公司'].fillna(merged['公司代號'])

    account_type = kd.get_data_from_MSSQL('''
                        SELECT  distinct buyer SAP公司代號
                            ,industry 公司型態
                            FROM sap_sales_data 
                         where industry like '%C%' or industry like '%D%'
    ''')

    merged = pd.merge(merged, account_type, on='SAP公司代號', how='inner')


    # 選擇最終欄位（只保留最終的公司代號）
    result = merged[['公司代號','公司型態', '未稅本位幣', '預計發貨日期']].dropna(subset=['公司代號'])

    return result



# 銷貨數據
total_sales= get_sap_with_relate_company(last_jan_first)


sales_last_year = total_sales[(total_sales['預計發貨日期'] >= last_jan_first) &(total_sales['預計發貨日期'] < jan_first)]
sales_this_year = total_sales[total_sales['預計發貨日期'] >= jan_first]

target_ly = sales_last_year.groupby(['公司代號','公司型態'])['未稅本位幣'].sum().reset_index()
target_ty = sales_this_year.groupby(['公司代號','公司型態'])['未稅本位幣'].sum().reset_index()

# 合併今年與去年的銷售
merged = pd.merge(
    target_ly, target_ty,
    on='公司代號',
    how='inner',
    suffixes=('_ly', '_ty')  # ly = last year, ty = this year
)
merged = merged.drop(columns=['公司型態_ty']).rename(columns={'公司型態_ly': '公司型態'})

# 計算銷售差異（去年 - 今年）
merged['差異'] = merged['未稅本位幣_ly'] - merged['未稅本位幣_ty']

# 篩選出差異大於 100 萬的公司
diff_over_1m = merged[merged['差異'].abs() > 1_000_000]

# 根據diff_over_1m銷貨數據所在公司,並查找公司的各維度數據
all_company_ids = diff_over_1m['公司代號'].dropna().unique().tolist()
batch_size = 100
account_df = pd.DataFrame()

for i in range(0, len(all_company_ids), batch_size):
    batch_ids = all_company_ids[i:i + batch_size]
    target_customer_str = "(" + ",".join(f"'{c}'" for c in batch_ids) + ")"

    xoql = f'''
        select accountCode__c 公司代號, customItem202__c 公司地址, dimDepart.departName 資料區域群組名稱,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號, accountName 公司名稱, customItem322__c 目標客戶類型,
        customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉
        from account
        where dimDepart.departName like '%TW%' 
        and accountCode__c in {target_customer_str}
    '''
    try:
        df = kd.get_data_from_CRM(xoql)
        account_df = pd.concat([account_df, df], ignore_index=True, sort=False)
    except Exception as e:
        print(f"Error on batch {i // batch_size + 1}: {e}")

K_invite = pd.merge(diff_over_1m, account_df, on = '公司代號', how = 'inner')



#  排除近3個月拜訪過
pass_visited =  kd.last_connected(month_ago_3ts, source_type='拜訪')
K_invite = K_invite[~K_invite['公司代號'].isin(set(pass_visited['公司代號']) )]

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
            '''
            )

K_invite = pd.merge(K_invite, contact_related, on = '公司代號', how = 'inner')

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


folder = Path(f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{(datetime.today() + relativedelta(months=1)).strftime('%Y.%m')}")
filename = f"兩年交易差異百萬客戶_{datetime.today().strftime('%Y.%m.%d')}.xlsx"
K_invite.to_excel(folder / filename, index=False)
print('數據導出成功~')