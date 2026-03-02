
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



top_20 = kd.get_sap_with_relate_company(year_ago_1d)
top_20 = top_20.groupby('公司代號')['未稅本位幣'].sum().reset_index()


# 根據top_20銷貨數據所在公司,並查找公司的各維度數據
# 特定C類
all_company_ids = top_20['公司代號'].dropna().unique().tolist()
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

K_invite = pd.merge(top_20, account_df, on = '公司代號', how = 'inner')


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


# 排除"近2個月內有拜訪、K大"
pass_visited =  kd.last_connected(month_ago_2ts, first='all')
K_invite = K_invite[~K_invite['公司代號'].isin(set(pass_visited['公司代號']) )]

# 獲取最近聯係日期
last_connected = kd.last_connected(year_ago_1ts)
K_invite = pd.merge(K_invite, last_connected, on = '公司代號', how = 'left')





# 關聯到大公司,並增加相對應的大小群組說明
group_info = kd.get_data_from_MSSQL('''select  buyer SAP公司代號, s_group_desc 小群組說明, l_group_desc 大群組說明  ,planned_shipping_date
                                    from [clean_data].[dbo].[sap_sales_data_processed]
                                    where buyer like 'TW%'
                                    ''')
group_info.sort_values(by=['SAP公司代號', 'planned_shipping_date'], ascending=[True, False], inplace=True)
group_info = kd.add_relate_company(group_info,"SAP")
group_info = group_info.drop_duplicates(subset=['SAP公司代號'], keep='first')

K_invite = pd.merge(K_invite, group_info[['SAP公司代號','小群組說明','大群組說明']],on ='SAP公司代號',how='left' )



# 數據清洗
K_invite = kd.clean_invalid_entries_visit(K_invite)
K_invite = K_invite.drop_duplicates('公司代號',keep= 'first')
target = ['經營客戶']
K_invite = K_invite[K_invite['目標客戶類型'].astype(str).str.contains('|'.join(target), na=False)]
K_invite['公司代號'].value_counts()

K_invite['排名'] = K_invite.groupby('小群組說明')['未稅本位幣']\
                          .rank(method='first', ascending=False)
K_invite_top20 = K_invite[K_invite['排名'] <= 20].copy()
K_invite_top20.drop(columns=['排名'], inplace=True)

K_invite_top20 = K_invite_top20.sort_values(by='日期', ascending=True, na_position='first')

path = f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{str((datetime.today()+relativedelta(months=1)).date().strftime("%Y.%m"))}/"
K_invite_top20.to_excel(path + "歷史銷貨排行_" + str(datetime.today().strftime("%Y.%m.%d")) + ".xlsx",index=False)
print('數據導出成功~')
