import sys
from pathlib import Path
from datetime import datetime, timedelta
import os

import pandas as pd
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd

new_company = kd.get_data_from_CRM(f'''
        select accountCode__c 公司代號, customItem202__c 公司地址, dimDepart.departName 資料區域群組名稱,customItem226__c 建檔日期,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號, parentAccCode1__c 關聯公司代號,accountName 公司名稱, customItem322__c 目標客戶類型,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉
        from account
        where dimDepart.departName like '%TW%'  and (customItem199__c like '%C%' or customItem199__c like '%D%' )
                ''')


'''
select related_contact 客關連數據
'''
contact_related = kd.get_data_from_CRM(
            f'''
            select name, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 聯絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c  空號,
            customItem51__c  停機,customItem52__c  號碼錯誤非本人

            from customEntity22__c 
            where customItem37__c  like '%TW%'
            '''
            )

K_invite = pd.merge(new_company, contact_related, on = '公司代號', how = 'left')

K_invite, removed_data = kd.clean_invalid_entries_text(K_invite)
K_invite = K_invite[K_invite['手機號碼'].apply(lambda x: isinstance(x, str) and len(x) == 10 and x != '0000000000' and x.startswith('09'))]
K_invite = K_invite.drop_duplicates(subset=['手機號碼'], keep='last')

'''
from K大預約表
'''

K_visit_df = kd.get_data_from_CRM('''select name,customItem30__c 是否舉行 ,customItem19__c 連絡人代號, customItem2__c 預約日期
        from customEntity23__c
        ''')


K_visit_df['預約日期'] = pd.to_numeric(K_visit_df['預約日期'], errors='coerce')  
K_visit_df['預約日期'] = K_visit_df['預約日期'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
K_visit_df['預約日期'] = K_visit_df['預約日期'].dt.tz_convert('Asia/Taipei')
K_visit_df['預約日期'] = K_visit_df['預約日期'].dt.strftime('%Y-%m-%d')

current_datetime = datetime.now()
target_date = current_datetime - timedelta(days=30)

K_visit_df['預約日期'] = pd.to_datetime(K_visit_df['預約日期'], errors='coerce')
K_visit_df = K_visit_df[pd.notna(K_visit_df['預約日期'])]
K_visit_df = K_visit_df[K_visit_df['預約日期'] >= target_date]
K_visit_df['是否舉行']= K_visit_df['是否舉行'].astype(str)
K_visit_df = K_visit_df[K_visit_df['是否舉行'].str.contains("是")]

K_invite = K_invite[~K_invite['連絡人代號'].isin(K_visit_df['連絡人代號'].drop_duplicates())]


Pass_user = []
timestamp = int(target_date.timestamp() * 1000)



'''
select 展示館預約/參訪記錄(customEntity43__c)
'''
museum = kd.get_data_from_CRM( f'''
        select customItem23__c.name 客戶關係連絡人,
        customItem51__c 是否到訪,customItem1__c 預約參訪日期
        from customEntity43__c
        where customItem1__c >= {timestamp}
        ''')
museum = museum[museum['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + museum['客戶關係連絡人'].dropna().tolist()))




'''
select  K大預約參會人(customEntity24__c)
'''
booking = kd.get_data_from_CRM( f'''
        select customItem2__c.name 客戶關係連絡人,
        customItem8__c 是否到訪,customItem31__c 預約日期
        from customEntity24__c
        where customItem1__c >= {timestamp}
        ''')


booking = booking[booking['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + booking['客戶關係連絡人'].dropna().tolist()))

K_invite = K_invite.loc[ ~K_invite['name'].isin(Pass_user)]
K_invite = K_invite.drop(columns=['name'])

K_invite['公司型態'].value_counts()



from datetime import datetime
import re
import pandas as pd
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
K_invite = K_invite.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
df_C_EC = K_invite[K_invite['公司型態'].isin(['C', 'EC'])].copy()
df_D = K_invite[K_invite['公司型態'].str.contains('D', na=False)].copy()
df_SE = K_invite[K_invite['公司型態'].str.contains('SE', na=False)].copy()
today_str = datetime.today().strftime('%Y_%m_%d')
output_path = fr"C:\Users\TW0002.TPTWKD\Desktop\K大简讯名单\K大簡訊名單_{today_str}.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    if not df_C_EC.empty:
        df_C_EC.to_excel(writer, sheet_name='C_EC', index=False)
    if not df_D.empty:
        df_D.to_excel(writer, sheet_name='包含D', index=False)
    if not df_SE.empty:
        df_SE.to_excel(writer, sheet_name='包含SE', index=False)

print('數據導出成功啦！（有資料的才匯出）')