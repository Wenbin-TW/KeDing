
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


sample_date = int(datetime(2025,7, 10).timestamp() * 1000)
today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
year_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_one = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_two = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
three_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-3)).timestamp() * 1000)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
five_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-50)).timestamp() * 1000)
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

K_invite, removed_data = kd.clean_invalid_entries_text_規劃組專案(K_invite)
K_invite = K_invite[K_invite['手機號碼'].apply(lambda x: isinstance(x, str) and len(x) == 10 and x != '0000000000' and x.startswith('09'))]
K_invite = K_invite.drop_duplicates(subset=['手機號碼'], keep='last')

Pass_user = []
timestamp = int((datetime.now() - timedelta(days=30)).timestamp() * 1000)
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
        customItem8__c 是否到訪,customItem31__c 預約日期,customItem38__c 上線分鐘數
        from customEntity24__c
        where customItem1__c >= {timestamp}
        ''')

booking['上線分鐘數'] = pd.to_numeric(booking['上線分鐘數'], errors='coerce')

booking = booking[booking['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)& (booking['上線分鐘數'] >= 8)]
Pass_user = list(set(Pass_user + booking['客戶關係連絡人'].dropna().tolist()))

K_invite = K_invite.loc[ ~K_invite['name'].isin(Pass_user)]
K_invite = K_invite.drop(columns=['name'])

K_invite['公司型態'].value_counts()

K_invite['唯一識別'] = ( K_invite['手機號碼'].str.strip().replace('', pd.NA) .combine_first(K_invite['連絡人代號']))
MRK_multiple = kd.get_data_from_CRM (f'''
        select customItem2__c.name 客戶關係連絡人,customItem2__c.contactCode__c__c 連絡人代號,customItem2__c.contactPhone__c__c 手機號碼,
        customItem8__c 是否到訪,customItem31__c 預約日期,customItem38__c 上線分鐘數
        from customEntity24__c
        where customItem31__c >= {month_ago_three}
        ''')
MRK_multiple['唯一識別'] = ( MRK_multiple['手機號碼'].str.strip().replace('', pd.NA) .combine_first(MRK_multiple['連絡人代號']))

MRK_multiple = kd.convert_to_date(MRK_multiple, '預約日期')
MRK_multiple['預約日期'] = pd.to_datetime(MRK_multiple['預約日期'], format='%Y-%m-%d', errors='coerce')
MRK_multiple['上線分鐘數'] = pd.to_numeric(MRK_multiple['上線分鐘數'], errors='coerce')
MRK_multiple_filtered = MRK_multiple[
    (MRK_multiple['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x) & (MRK_multiple['上線分鐘數'] >= 8))
    | (MRK_multiple['預約日期'] >= today)]

K_invite['是否K大'] = K_invite['唯一識別'].isin(MRK_multiple['唯一識別']).map({True: '是', False: '否'})


connected_one_year_all= kd.get_data_from_CRM (f'''
        select
        customItem48__c 客戶關係連絡人,customItem59__c 連絡人代號,customItem49__c 公司型態,customItem176__c 無效電拜訪類型,
        customItem177__c 無效電訪類型,customItem40__c 最近聯繫時間, customItem112__c,
        customItem128__c 觸客類型,customItem55__c 手機號碼,entityType,customItem207__c 講解分鐘數
        from customEntity15__c
        where customItem40__c >= {month_ago_three} and customItem118__c like '%TW%'
        ''')
connected_one_year = connected_one_year_all[
    (connected_one_year_all['無效電拜訪類型'].isna() |
        (connected_one_year_all['無效電拜訪類型'].astype(str).str.strip() == '')
    ) &( connected_one_year_all['無效電訪類型'].isna() |
        (connected_one_year_all['無效電訪類型'].astype(str).str.strip() == '') )]
connected_one_year['唯一識別'] = ( connected_one_year['手機號碼'].str.strip().replace('', pd.NA) .combine_first(connected_one_year['連絡人代號']))
outdoor_test = connected_one_year[(connected_one_year["最近聯繫時間"].astype(float) >= month_ago_three) ]
outdoor_test = outdoor_test[outdoor_test['觸客類型'].astype(str).str.contains("A1", na=False) &
                                outdoor_test['講解分鐘數'].astype(str).str.strip().ne('') ]
K_invite['是否拜訪K大'] = K_invite['唯一識別'].isin(outdoor_test['唯一識別']).map({True: '是', False: '否'})
museum_three_month= kd.get_data_from_CRM (f'''
        select customItem23__c.name 客戶關係連絡人,customItem23__c.contactCode__c__c 連絡人代號, customItem23__c.contactPhone__c__c 手機號碼,
        customItem51__c 是否到訪,customItem1__c 預約日期
        from customEntity43__c
        where customItem1__c >= {month_ago_three}
        ''')
museum_three_month['唯一識別'] = ( museum_three_month['手機號碼'].str.strip().replace('', pd.NA) .combine_first(museum_three_month['連絡人代號']))

museum_three_month = kd.convert_to_date(museum_three_month, '預約日期')
museum_three_month['預約日期'] = pd.to_datetime(museum_three_month['預約日期'], format='%Y-%m-%d', errors='coerce')
museum_one_filtered = museum_three_month[(museum_three_month['是否到訪'].astype(str).str.contains('是', na=False)) | (museum_three_month['預約日期'] >= pd.Timestamp.now())]
K_invite['是否展館K大'] = K_invite['唯一識別'].isin(museum_one_filtered['唯一識別']).map({True: '是', False: '否'})



K_invite = K_invite.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
df_C_EC = K_invite[K_invite['公司型態'].isin(['C', 'EC'])].copy()
df_D = K_invite[K_invite['公司型態'].str.contains('D', na=False)].copy()
df_SE = K_invite[K_invite['公司型態'].str.contains('SE', na=False)].copy()
today_str = datetime.today().strftime('%Y_%m_%d')
output_path = fr"Z:\18_各部門共享區\15_數據中心課\文斌\K大简讯名单\規劃組專案名單{today_str}.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    if not df_C_EC.empty:
        df_C_EC.to_excel(writer, sheet_name='C_EC', index=False)
    if not df_D.empty:
        df_D.to_excel(writer, sheet_name='包含D', index=False)
    if not df_SE.empty:
        df_SE.to_excel(writer, sheet_name='包含SE', index=False)

print('數據導出成功啦！（有資料的才匯出）')