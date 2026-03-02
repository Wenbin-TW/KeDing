
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil
from pathlib import Path
import sys

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


year_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
new_company = kd.get_data_from_CRM(f'''
        select accountCode__c 公司代號, customItem202__c 公司地址,id customItem11__c, dimDepart, dimDepart.departName 資料區域群組名稱,customItem226__c 建檔日期,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號, accountName 公司名稱, customItem322__c 目標客戶類型,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉
        from account
        WHERE dimDepart.departName LIKE '%TW%'      
        and customItem199__c in ('C', 'DC', 'DD', 'DP', 'EC', 'ED', 'FA', 'FB', 'FD', 'GD', 'KD', 'SD', 'SE', 'W','SS')
        ''')
contact_related = kd.get_data_from_CRM(
            f'''
            select name, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 聯絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人

            from customEntity22__c 
            where customItem37__c  like '%TW%'
            ''')
K_invite = pd.merge(new_company, contact_related, on = '公司代號', how = 'inner')
contact = kd.get_data_from_CRM(
            f'''
            select contactCode__c 連絡人代號, customItem194__c 聯絡人普查標簽, customItem196__c 普查貼標日期
            from contact 
            where customItem196__c < {year_ago_1ts}
            ''')
contact = kd.convert_to_date(contact, '普查貼標日期')
K_invite = pd.merge(K_invite, contact, on = '連絡人代號', how = 'left')
K_invite = K_invite.sort_values(by='普查貼標日期', ascending=True, na_position='first')

K_invite_census = K_invite.copy()
K_invite_census, removed_contact_ids = kd.clean_invalid_entries_census(K_invite_census)
print("剔除 聯絡人普查標籤 的公司代號：", removed_contact_ids)
all_removed_ids = list(set( removed_contact_ids))
K_invite_census = K_invite_census[~K_invite_census['連絡人代號'].isin(all_removed_ids)].copy()
K_invite_census['職務類別'].value_counts()
K_invite_contact = K_invite_census[['公司代號','公司名稱','SAP公司代號','連絡人','name','連絡人代號']] .rename(columns={'name': '客關聯代號'})
K_invite_company = K_invite_census[['公司代號','公司名稱','SAP公司代號']]
K_invite_company = K_invite_company.drop_duplicates(subset=['公司代號'], keep='first').reset_index(drop=True)
year_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=0)).date()).timestamp()*1000
day_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=0) + relativedelta(days=-1)).date()).timestamp()*1000

gift_df = kd.get_data_from_CRM(f'''
        select name 型錄編號, accountCode__c 公司代號,customItem31__c 聯絡人代號, appDate__c 申請日期, customItem118__c 實際發放數量
                ,dimDepart.departName 所屬部門,customItem86__c 物品發放名稱
        from customEntity25__c
        where   appDate__c >= {year_ago_one} and  appDate__c < {day_ago_one} and dimDepart.departName like '%TW%'
                ''')
kd.convert_to_date(gift_df, '申請日期')
gift_df["實際發放數量"] = pd.to_numeric(gift_df["實際發放數量"], errors="coerce").fillna(0)

gift_df["寄件(正)"] = (gift_df["實際發放數量"] > 0).astype(int)
gift_df["退件(正)"] = (gift_df["實際發放數量"] < 0).astype(int)


K_invite_contact = K_invite_contact.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
K_invite_company = K_invite_company.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
filename = fr"Z:\06_業管部\04_管理課\張恆碩\普查名單\{datetime.today():%Y_%m}月度普查清單.xlsx"

with pd.ExcelWriter(filename, engine="openpyxl") as writer:
    K_invite_company.to_excel(writer, sheet_name="普查公司", index=False)
    K_invite_contact.to_excel(writer, sheet_name="普查聯絡人", index=False)
    gift_df.to_excel(writer, sheet_name="型錄寄發申請", index=False)

print(f"已輸出到：{filename}")

