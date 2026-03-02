import os
import sys
import time
import re
import urllib
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd

load_dotenv()
params = urllib.parse.quote_plus(
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')};"
    f"DATABASE={os.getenv('DB_NAME')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASSWORD')}")
engine = create_engine( f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)



user_df = kd.get_data_from_CRM ('select id, name, dimDepart from user')
user_df[user_df['name'] == '易家婕']

exclude_pattern = "|".join(map(re.escape, ["Genesys_廠商", "承攬電訪"]))
label_order = ["經營客戶","開發中","開發客戶","沉默客戶",'無','']
relation_order = ["在職（主要公司）", "在職（配合）", "離職"]
relation_map = {k: i+1 for i, k in enumerate(relation_order)}

kd.賈維斯1號('>>>1. 開始處理每日K大交辦')
sales_target = ["經營客戶","開發中","沉默客戶",'無','','開發客戶','暫封存客戶']
承攬_target = ['不再派發']
gc_target = sales_target

sample_date = int(datetime(2025,1, 1).timestamp() * 1000)
型錄發放日期 = int(datetime(2025,12, 29).timestamp() * 1000)
型錄寄送截止日期 = int(datetime(2026,1, 28).timestamp() * 1000)
專案型錄寄送截止日期 = int(datetime(2025,12, 29).timestamp() * 1000)
today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
year_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_one = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_two = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_six = pd.to_datetime((datetime.today() - relativedelta(months=6) + relativedelta(days=1)).date()).timestamp()*1000
three_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-3)).timestamp() * 1000)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
five_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-5)).timestamp() * 1000)
seven_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-7)).timestamp() * 1000)


總名單 = pd.read_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\K大名單監控具體數據.xlsx", dtype='object')
總名單["relation_rank"] = ( 總名單["關係狀態"] .apply(lambda x: str(x).strip("[]").replace("'", "").strip()) .map(relation_map)  .fillna(5))
總名單["label_rank"] = pd.Categorical( (總名單["目標客戶類型"].astype(str).str.replace(r"^\[|\]$", "", regex=True)  .str.replace("'", "", regex=False)  .str.strip()),   categories=label_order,ordered=True)
總名單['唯一識別'] = ( 總名單['手機號碼'].str.strip().replace('', pd.NA) .combine_first(總名單['連絡人代號']))
總名單 = ( 總名單.sort_values(["relation_rank", "label_rank"], ascending=[True, True]) .dropna(subset=['唯一識別']).drop_duplicates(subset=['唯一識別'], keep='first'))
總名單_copy = 總名單.copy()

new_contact_ids = set(總名單[總名單['半年新建聯絡人'] == '是']["唯一識別"].unique())
control_contact_ids = set( 總名單[ 總名單['客戶付款類型'].astype(str).str.contains("呆帳管制", na=False)]['唯一識別'].unique())

filename = fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\K大經營具體數據.xlsx"
總名單_copy[總名單_copy["目標客戶類型"].astype(str).str.contains("[經營]")].to_excel(filename, index=False)


寄後名單_12月 = pd.read_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\型錄發放_廣發名單.xlsx",sheet_name='202512', dtype='object')
寄後名單_12月['唯一識別'] = '0' + 寄後名單_12月['連絡人手機'].astype(str)
總名單 = 總名單[(~總名單['唯一識別'].isin(寄後名單_12月['唯一識別']))]
today = datetime.today().strftime("%Y-%m-%d")
TW = len(總名單_copy)
update_query = """ UPDATE clean_data.dbo.crm_effective_customer SET TW = :tw_value WHERE Date = :date_value"""
with engine.begin() as connection:
    connection.execute( text(update_query),  {"tw_value": TW, "date_value": today}  )



kd.賈維斯1號('>>>2. 名單數據處理完成, 開始抓取CRM數據')
connected_one_year_all= kd.get_data_from_CRM (f'''
        select
        customItem48__c 客戶關係連絡人,customItem59__c 連絡人代號,customItem49__c 公司型態,customItem176__c 無效電拜訪類型,
        customItem177__c 無效電訪類型,customItem40__c 最近有效聯繫日期, customItem112__c,
        customItem128__c 觸客類型,customItem55__c 手機號碼,entityType,customItem207__c 講解分鐘數,createdBy
        from customEntity15__c
        where customItem40__c >= {year_ago_one} and customItem118__c like '%TW%'
        ''')
connected_one_year_all['唯一識別'] = ( connected_one_year_all['手機號碼'].str.strip().replace('', pd.NA) .combine_first(connected_one_year_all['連絡人代號']))

connected_one_year = connected_one_year_all[
    (connected_one_year_all['無效電拜訪類型'].isna() |
        (connected_one_year_all['無效電拜訪類型'].astype(str).str.strip() == '')
    ) &( connected_one_year_all['無效電訪類型'].isna() |
        (connected_one_year_all['無效電訪類型'].astype(str).str.strip() == '') )]

already_fish_ids = set(connected_one_year_all[connected_one_year_all['entityType'] == '3083500023402378']["唯一識別"].unique())

new_contact_ids = new_contact_ids - set(connected_one_year["唯一識別"].dropna().unique())


connection = connected_one_year_all.copy()
connection = kd.convert_to_date(connection, '最近有效聯繫日期').rename(columns={'最近有效聯繫日期': '最近嘗試觸達日期'})
connection = connection[connection['唯一識別'].notna() & (connection['唯一識別'].astype(str).str.strip() != "")]
connection_last = connection.sort_values(['唯一識別', '最近嘗試觸達日期'], ascending=[True, False])
connection_last = connection_last.drop_duplicates(subset=['唯一識別'], keep='first').reset_index(drop=True)[['唯一識別','最近嘗試觸達日期']]
connected = connected_one_year_all.copy()
connected = kd.convert_to_date(connected, '最近有效聯繫日期')
weekly_failed = connected[
    (pd.to_datetime(connection['最近嘗試觸達日期'], errors='coerce')>= datetime.today() - timedelta(days=30)) &
    (connected['createdBy'] != "3414568030034443") &
    (
        (connected['無效電訪類型'].notna() & (connected['無效電訪類型'].astype(str).str.strip() != "")) |
        (connected['無效電拜訪類型'].notna() & (connected['無效電拜訪類型'].astype(str).str.strip() != "")))]

weekly_failed_id = set(weekly_failed['唯一識別'].astype(str).tolist())
filtered_connected_2025 = connected_one_year_all[
    pd.to_numeric(connected_one_year_all["最近有效聯繫日期"], errors="coerce") > sample_date
].reset_index(drop=True)

filtered_connected_success = filtered_connected_2025[filtered_connected_2025['customItem112__c'].astype(str).str.contains("成功")]
success_ids = set(filtered_connected_success["唯一識別"].unique())
gstat = ( filtered_connected_2025.groupby("唯一識別")["customItem112__c"]
    .agg( total_count = "size",  nonnull_count = lambda s: s.notna().sum(),
        reject_count = lambda s: s.astype(str).str.contains("拒絕", na=False).sum()))
reject_ids = set(gstat.query("reject_count == total_count and nonnull_count == total_count").index)
filtered_3m = filtered_connected_2025[ pd.to_numeric(filtered_connected_2025["最近有效聯繫日期"], errors="coerce") >= month_ago_three]
gstat_3m = ( filtered_3m .groupby("唯一識別")["customItem112__c"] .agg(  total_count="size",  nonnull_count=lambda s: s.notna().sum(),
  reject_count=lambda s: s.astype(str).str.contains("拒絕", na=False).sum()))
reject_3m_ids = set( gstat_3m.query("reject_count == total_count and nonnull_count == total_count").index)
success_then_reject_3m_ids = success_ids & reject_3m_ids
MRK_multiple_new = kd.get_data_from_CRM (f'''
        select customItem2__c.name 客戶關係連絡人,customItem2__c.contactCode__c__c 連絡人代號,customItem2__c.contactPhone__c__c 手機號碼,
        customItem8__c 是否上線,customItem38__c 上線分鐘數,customItem31__c 預約日期,customItem28__c 改約K大預約表
        from customEntity24__c
        where customItem31__c >= {year_ago_one}
        ''')
MRK_multiple_y1 = pd.concat([MRK_multiple_new], ignore_index=True)

MRK_multiple = MRK_multiple_y1[ pd.to_numeric(MRK_multiple_y1['預約日期'], errors='coerce') >= month_ago_three]

MRK_multiple['唯一識別'] = ( MRK_multiple['手機號碼'].str.strip().replace('', pd.NA) .combine_first(MRK_multiple['連絡人代號']))

MRK_multiple = kd.convert_to_date(MRK_multiple, '預約日期')
MRK_multiple['預約日期'] = pd.to_datetime(MRK_multiple['預約日期'], format='%Y-%m-%d', errors='coerce')
MRK_multiple['上線分鐘數'] = pd.to_numeric(MRK_multiple['上線分鐘數'], errors='coerce')
MRK_multiple_filtered = MRK_multiple[
    (MRK_multiple['是否上線'].apply(lambda x: isinstance(x, (list, str)) and '是' in x) & (MRK_multiple['上線分鐘數'] >= 8))
    | (MRK_multiple['預約日期'] >= today)] .sort_values(by='預約日期', ascending=True)   .reset_index(drop=True) 
總名單 = 總名單[(~總名單['唯一識別'].isin(MRK_multiple_filtered['唯一識別']))]






MRK_multiple_y1['唯一識別'] = ( MRK_multiple_y1['手機號碼'].str.strip().replace('', pd.NA) .combine_first(MRK_multiple_y1['連絡人代號']))

MRK_multiple_y1 = kd.convert_to_date(MRK_multiple_y1, '預約日期')
MRK_multiple_y1['預約日期'] = pd.to_datetime(MRK_multiple_y1['預約日期'], format='%Y-%m-%d', errors='coerce')
MRK_multiple_y1['上線分鐘數'] = pd.to_numeric(MRK_multiple_y1['上線分鐘數'], errors='coerce')
MRK_multiple_y1_filtered = MRK_multiple_y1[(MRK_multiple_y1['上線分鐘數'] < 8) &
                   (MRK_multiple_y1['改約K大預約表'] == '')   ] .sort_values(by='預約日期', ascending=True)   .reset_index(drop=True) 
stand_we_up = (  MRK_multiple_y1_filtered  .groupby('唯一識別') .size() .reset_index(name='放鳥次數'))
總名單 = pd.merge(總名單 ,stand_we_up, on = '唯一識別', how = 'left')
總名單_copy = pd.merge(總名單_copy ,stand_we_up, on = '唯一識別', how = 'left')
MRK_multiple = MRK_multiple.sort_values(['唯一識別', '預約日期'])
last_records = MRK_multiple.groupby('唯一識別').tail(1)
not_visited_last = last_records[last_records['是否上線'].astype(str).str.contains("否")]
not_visited_unique_ids = not_visited_last['唯一識別'].dropna().tolist()

總名單['最後一次K大狀態'] = np.where( 總名單['唯一識別'].isin(not_visited_unique_ids),'K大未上線', pd.NA)
總名單_copy['最後一次K大狀態'] = np.where( 總名單_copy['唯一識別'].isin(not_visited_unique_ids),'K大未上線', pd.NA)
outdoor_test = connected_one_year[(connected_one_year["最近有效聯繫日期"].astype(float) >= month_ago_three) ]
outdoor_test = outdoor_test[outdoor_test['觸客類型'].astype(str).str.contains("A1", na=False) &
                                outdoor_test['講解分鐘數'].astype(str).str.strip().ne('') ]
總名單 = 總名單[(~總名單['唯一識別'].isin(outdoor_test['唯一識別']))]
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
總名單 = 總名單[(~總名單['唯一識別'].isin(museum_one_filtered['唯一識別']))]
connected_one_year["最近有效聯繫日期"] = pd.to_numeric(connected_one_year["最近有效聯繫日期"], errors="coerce")
now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
ts_15d = int(pd.to_datetime((now - timedelta(days=15)).date()).timestamp() * 1000)
ts_2m  = int(pd.to_datetime((now - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp() * 1000)
mask_refuse = connected_one_year["customItem112__c"].astype(str).str.contains("拒絕邀約", na=False)
mask_cats   = connected_one_year["公司型態"].astype(str).isin(["C", "DC", "DD"])

mask_15d = connected_one_year["最近有效聯繫日期"] >= ts_2m
mask_3m  = connected_one_year["最近有效聯繫日期"] >= ts_2m
refuse_15d = connected_one_year[ mask_refuse &  mask_cats & mask_15d].copy()
refuse_3m  = connected_one_year[ mask_refuse & ~mask_cats & mask_3m ].copy()

總名單 = 總名單[(~總名單['唯一識別'].isin(refuse_15d['唯一識別']))]
總名單 = 總名單[(~總名單['唯一識別'].isin(refuse_3m['唯一識別']))]

總名單['上一次拒絕'] = np.where((總名單['唯一識別'].isin(refuse_15d['唯一識別'])) | (總名單['唯一識別'].isin(refuse_3m['唯一識別'])),'拒絕K大', pd.NA)
總名單_copy['上一次拒絕'] = np.where((總名單_copy['唯一識別'].isin(refuse_15d['唯一識別'])) | (總名單_copy['唯一識別'].isin(refuse_3m['唯一識別'])),'拒絕K大', pd.NA)
already_test = kd.get_data_from_CRM(
        f'''    
        select id,name, createdAt, customItem42__c,customItem10__c, customItem3__c, approvalStatus,
        entityType,customItem8__c,customItem42__c.name 客戶關係聯絡人代號,customItem120__c, customItem121__c,
        customItem42__c.contactPhone__c__c 手機號碼, customItem42__c.contactCode__c__c 連絡人代號,customItem119__c 無效電訪類型
        from customEntity14__c 
        where entityType in ('2904963933786093','3028348436713387') and createdAt >= {month_ago_three}
        ''')
already_test['唯一識別'] = ( already_test['手機號碼'].str.strip().replace('', pd.NA) .combine_first(already_test['連絡人代號']))
already_test = already_test.loc[already_test['customItem8__c'].astype(str).str.contains("等待|進行中")] 
總名單 = 總名單[(~總名單['唯一識別'].isin(already_test['唯一識別']))]
fish_test = kd.get_data_from_CRM(
        f'''    
        select id,name, createdAt, customItem42__c,customItem10__c, customItem3__c, approvalStatus,
        entityType,customItem8__c,customItem42__c.name 客戶關係聯絡人代號,customItem120__c, customItem121__c,
        customItem42__c.contactPhone__c__c 手機號碼, customItem42__c.contactCode__c__c 連絡人代號,customItem119__c 無效電訪類型,
        customItem45__c 執行內容說明
        from customEntity14__c 
        where entityType in ('3077984163073940') and customItem8__c = 1 
        ''')
fish_test['手機號碼'] = (fish_test['執行內容說明'].str.extract(r'(?:聯絡人電話|手機號碼|手機|電話|號碼)[：:\s]*([\d\-\(\)\s]{6,})'))
fish_test['唯一識別'] = ( fish_test['手機號碼'].str.strip().replace('', pd.NA) .combine_first(fish_test['連絡人代號']))
fish_test = fish_test.loc[fish_test['customItem8__c'].astype(str).str.contains("等待|進行中")] 
總名單 = 總名單[(~總名單['唯一識別'].isin(fish_test['唯一識別']))]
timestamp = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)+ timedelta(days=-3)).timestamp() * 1000)
timestamp2 = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=30)).timestamp() * 1000)

wait = kd.get_data_from_CRM(
            f'''
        select id,name,customItem10__c, customItem3__c,workflowStageName,approvalStatus,customItem8__c,customItem206__c
        ,customItem121__c,customItem65__c, customItem42__c.name 客戶關係連絡人編號,entityType,createdBy,customItem142__c 連絡人代號
        ,customItem42__c.contactPhone__c__c 手機號碼,customItem13__c 交辦完成日期,customItem120__c 期望完成日期,customItem119__c 無效電訪類型
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387') 
        and  ((customItem120__c >= {timestamp} and customItem120__c < {timestamp2}))
            ''')
kd.convert_to_date(wait,'期望完成日期')
wait['唯一識別'] = ( wait['手機號碼'].str.strip().replace('', pd.NA) .combine_first(wait['連絡人代號']))
wait_test = wait.loc[wait['customItem8__c'].astype(str).str.contains("進行中") &  wait['無效電訪類型'].astype(str).str.contains("退休")] 
總名單 = 總名單[(~總名單['唯一識別'].isin(wait_test['唯一識別']))]
connected_latest = (connected_one_year.sort_values('最近有效聯繫日期', ascending=False)
                    .drop_duplicates(subset='唯一識別', keep='first').reset_index(drop=True))[['唯一識別','最近有效聯繫日期']]


總名單 = pd.merge(總名單, connected_latest, on='唯一識別', how='left' )
總名單 = kd.convert_to_date(總名單,'最近有效聯繫日期')
總名單 = 總名單.sort_values(by='最近有效聯繫日期', ascending=True, na_position='first')
總名單 = pd.merge(總名單, connection_last, on = '唯一識別', how = 'left').sort_values(['最近嘗試觸達日期'], ascending=[True], na_position='first')

總名單_copy = pd.merge(總名單_copy, connected_latest, on='唯一識別', how='left' )
總名單_copy = kd.convert_to_date(總名單_copy,'最近有效聯繫日期')
總名單_copy = 總名單_copy.sort_values(by='最近有效聯繫日期', ascending=True, na_position='first')
總名單_copy = pd.merge(總名單_copy, connection_last, on = '唯一識別', how = 'left').sort_values(['最近嘗試觸達日期'], ascending=[True], na_position='first')



def select_today_call_list(df: pd.DataFrame) -> tuple:
    df = df.copy()
    phone = df['手機號碼'].astype(str).str.strip().replace({'nan':'', 'NaN':'', 'None':''})
    df['使用號碼'] = np.where((phone != '') & phone.notna(), phone, df['公司電話'])
    df['最近有效聯繫日期'] = pd.to_datetime(df['最近有效聯繫日期'], errors='coerce')
    cutoff = pd.Timestamp.today() - pd.DateOffset(months=1)
    df['距今排序'] = np.select(
        [ df['最近有效聯繫日期'].notna() & (df['最近有效聯繫日期'] < cutoff),  # 遠
            df['最近有效聯繫日期'].isna()                                   # 空白
        ],[0, 1],default=2  )
    df = df.sort_values(['距今排序', '最近有效聯繫日期'], ascending=[True, True], na_position='last')
    today_call = df.drop_duplicates(subset='使用號碼', keep='first')
    remaining = df.loc[~df.index.isin(today_call.index)]
    today_call = today_call.drop(columns=['距今排序'], errors='ignore')
    remaining = remaining.drop(columns=['距今排序'], errors='ignore')

    return today_call, remaining

today_call, remaining = select_today_call_list(總名單)
today_call = today_call.sort_values(['最近嘗試觸達日期'], ascending=[True], na_position='first')




from datetime import datetime, timedelta
today = datetime.today()
d_7 = today - timedelta(days=7)
def classify(row):
    uid = row["唯一識別"]
    last_try = row["最近嘗試觸達日期"]
    created_by = str(row.get("createdBy", ""))
    invalid_type = str(row.get("無效電訪類型", ""))

    if uid in success_then_reject_3m_ids and uid not in control_contact_ids:
        return "1近一年成功但近三個月全拒絕非呆帳聯絡人"
    
    elif uid in reject_ids and uid not in control_contact_ids:
        return "2近一年全拒絕非呆帳聯絡人"

    elif uid in weekly_failed_id:
        return "7一個月內無效接通聯絡人"

    elif uid in new_contact_ids:
        return "3半年未成功觸達新聯絡人"
    
    elif pd.isna(last_try) or str(last_try).strip() == "":
        return "4近一年未觸達聯絡人"

    elif uid in success_ids:
        return "6近一年成功邀約聯絡人"

    elif uid in already_fish_ids and uid not in weekly_failed_id :
        return "8近一年釣魚簡讯聯络人"
    
    else:
        return "5其他聯絡人"
today_call["名單類別"] = today_call.apply(classify, axis=1)
target_category = "6近一年成功邀約聯絡人"
today_call["sort_no_show"] = np.where(today_call["名單類別"] == target_category, today_call["放鳥次數"], -1)
today_call["sort_no_show"] = today_call["sort_no_show"].fillna(-1) # 確保 NA 變成 -1
today_call["sort_date"] = np.where( today_call["名單類別"] != target_category,  today_call["最近嘗試觸達日期"],  pd.Timestamp.min)
today_call["sort_date"] = pd.to_datetime(today_call["sort_date"], errors='coerce')
today_call["sort_date"] = today_call["sort_date"].fillna(pd.Timestamp.min) # 確保 NA 變成最小時間
today_call = (today_call.sort_values( by=["名單類別", "sort_no_show", "sort_date"],
        ascending=[True, False, True] # 類別(升), 放鳥(降), 日期(升)
    ).drop(columns=["sort_no_show", "sort_date"]).reset_index(drop=True))





from openpyxl.utils import get_column_letter
exclusion_sources = [
    (寄後名單_12月,          '12月底寄後電訪名單-暫不觸達'),
    (MRK_multiple_filtered, '近三個月K大 / 已預約或已到訪(且上線≥8分)'),
    (outdoor_test,          '近3個月外勤成功拜訪'),
    (museum_one_filtered,   '近3個月展館到訪 / 或已預約參觀'),
    (refuse_15d,            '兩個月內拒K（公司型態=C/DC/DD）'),
    (refuse_3m,             '兩個月內拒K（公司型態≠C/DC/DD）'),
    (already_test,          '已有「等待回應」交辦'),
    (fish_test,             '已有「釣魚簡訊」交辦'),
    (wait_test,             '近3天有K大未完成(退休)任務'),
]
excl_sets = []
for df_src, reason in exclusion_sources:
    ids = set(df_src['唯一識別'].dropna().astype(str))
    excl_sets.append((ids, reason))
mark_df =總名單_copy.copy()
mark_df['唯一識別'] = mark_df['唯一識別'].astype(str)

def collect_reasons(uid: str) -> list:
    rs = []
    for ids, reason in excl_sets:
        if uid in ids:
            rs.append(reason)
    return rs

reasons_list = mark_df['唯一識別'].apply(collect_reasons)

mark_df['剔除次數'] = reasons_list.apply(len).astype(int)
mark_df['剔除原因'] = reasons_list.apply(lambda lst: '；'.join(lst) if lst else '')

mark_df['是否剔除'] = mark_df['剔除次數'] > 0

today_ids = set(today_call['唯一識別'].dropna().astype(str)) if '唯一識別' in today_call.columns else set()
mark_df['是否會被觸達'] = mark_df['唯一識別'].astype(str).isin(today_ids)



kd.賈維斯1號('>>>3. 數據清理完成, 開始寫入公槽')
for col in ['上一次拒絕', '最後一次K大狀態']:
    if col not in mark_df.columns and col in 總名單_copy.columns:
        mark_df[col] = 總名單_copy[col]

today_str = datetime.today().strftime("%Y-%m-%d")
output_path = fr"Z:\18_各部門共享區\15_數據中心課\文斌\每日K大剩餘名單\K大名單_帶剔除標記_{today_str}.xlsx"

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    mark_df.to_excel(writer, index=False, sheet_name="全部名單_帶標記")
    today_call.to_excel(writer, index=False, sheet_name="今日撥打")

print(f"已輸出：{output_path}")
today = datetime.today().date()  
TW = len(mark_df)

update_query = """
UPDATE [bi_ready].[dbo].[crm_valid_customer_daily]
   SET [TW] = :tw_value
 WHERE [Date] = :date_value;
"""
with engine.begin() as connection:
    connection.execute(text(update_query), {"tw_value": TW, "date_value": today})
sales_total_original = kd.get_data_from_CRM (f'''
        select customItem1__c customItem10__c, customItem3__c 預估通數,customItem1__c.dimDepart dimDepart,
        customItem2__c 電訪人員類型,customItem1__c.name 電訪人員
        from customEntity42__c
        where customItem5__c = 1
        ''')
sales_total_original['預估通數'] = pd.to_numeric(sales_total_original['預估通數'], errors='coerce').fillna(0).astype(int)
sales_total = sales_total_original.copy()
sales_total = sales_total[sales_total['預估通數'] > 0].reset_index(drop=True)
sales = sales_total.loc[~sales_total['電訪人員'].str.contains(exclude_pattern, regex=True, na=False)]


sales['電訪人員類型'] = sales['電訪人員類型'].astype(str)
sort_order = ['二面人員', '產品顧問', '未接區']

sales_total['電訪人員類型'] = (sales_total['電訪人員類型'].astype(str).str.replace(r"^\['|'\]$", '', regex=True)  )

sales = (sales_total
    .loc[~sales_total['電訪人員'].str.contains(exclude_pattern, regex=True, na=False)]
    .loc[lambda df: df['電訪人員類型'].isin(sort_order)]
    .assign(電訪人員類型=lambda df: pd.Categorical(df['電訪人員類型'], categories=sort_order, ordered=True))
    .sort_values('電訪人員類型'))

sales_gc = sales_total.loc[sales_total['電訪人員'].str.contains("Genesys_廠商")] [['customItem10__c','電訪人員','預估通數','dimDepart']]
kd.賈維斯1號('>>>4. 處理GC寄後電訪')

today_str = datetime.today().strftime('%Y-%m-%d')
gift_df = kd.get_data_from_CRM (f'''
        select id customItem152__c,name 型錄發放申請編號,customItem26__c, account__c.Phone__c 公司電話,customItem106__c
        from customEntity25__c
        where createdAt >= {型錄發放日期} and entityType = '2905332731124037'
            ''')

gift_df = gift_df.loc[~gift_df['customItem106__c'].astype(str).str.contains("退件")]
gift_df1 = gift_df[(gift_df['customItem26__c']=='') ]
gift_df2 = gift_df.drop_duplicates(subset=['customItem26__c'], keep='last')
gift_df = pd.concat([gift_df1,gift_df2])
gift_df = gift_df.drop_duplicates(subset=['型錄發放申請編號'], keep='last')
Tasks_df = kd.get_data_from_CRM (f'''
       select id,name,customItem10__c, customItem3__c,customItem8__c,customItem152__c.name 型錄發放申請編號,
       customItem121__c, customItem45__c, customItem42__c, entityType,customItem49__c 區域代碼,customItem11__c,
       customItem57__c,customItem59__c,customItem39__c,customItem153__c, customItem116__c 目標客戶類型,customItem42__c.contactPhone__c__c 手機號碼
       from customEntity14__c
       where entityType = '2904963933786093' and customItem120__c >= {型錄發放日期}
        and customItem39__c < {型錄寄送截止日期}
            ''')
Tasks_df = Tasks_df[Tasks_df['型錄發放申請編號'].astype(str).str.strip() != '']
df_normal = Tasks_df[
    ~Tasks_df['customItem57__c'].str.contains("建案款") &
    ~Tasks_df['區域代碼'].str.contains("TW-Z")]
df_normal = df_normal[
    ~df_normal['手機號碼'].isin(MRK_multiple_filtered['唯一識別']) &
    ~df_normal['手機號碼'].isin(outdoor_test['唯一識別']) &
    ~df_normal['手機號碼'].isin(museum_one_filtered['唯一識別'])]
df_normal = df_normal.loc[df_normal['customItem8__c'].astype(str).str.contains("等待")]  
df_normal = df_normal.loc[df_normal['customItem121__c'].astype(str).str.contains("第一次|未接1")]  

df_normal['customItem10__c'] = sales_gc['customItem10__c'].iloc[0]
df_normal['dimDepart'] = sales_gc['dimDepart'].iloc[0]
df_normal['customItem120__c'] = today_str
result_df = kd.get_procInstId(df_normal)
withdraw_results = kd.withdraw_tasks(result_df)
time.sleep(10)
df_normal = df_normal[['id','customItem10__c','customItem120__c']]
bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, df_normal)
time.sleep(5)

df_project =  Tasks_df[Tasks_df['customItem57__c'].str.contains("建案款") |  Tasks_df['區域代碼'].str.contains("TW-Z")]
df_project["customItem39__c"] = pd.to_numeric( df_project["customItem39__c"], errors="coerce")
df_project = df_project[df_project['customItem39__c'] < 專案型錄寄送截止日期]
df_project = df_project.loc[df_project['customItem8__c'].astype(str).str.contains("等待")]  
df_project = df_project.loc[df_project['customItem121__c'].astype(str).str.contains("第一次|未接1")]  
df_project['customItem120__c'] = today_str
sales_special = sales_total_original.loc[sales_total_original['電訪人員'].str.contains("易家婕")] [['customItem10__c','電訪人員','預估通數','dimDepart']]
df_project['customItem10__c'] = sales_special['customItem10__c'].iloc[0]
result_df = kd.get_procInstId(df_project)
withdraw_results = kd.withdraw_tasks(result_df)
df_project = df_project[['id','customItem10__c','customItem120__c']]
bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, df_project)
time.sleep(5)
kd.賈維斯1號('>>>5-1. 處理GC每日K大-- 拒絕K大')

gc_名單 = kd.filter_by_target(today_call, '目標客戶類型', sales_target)
gc_名單_拒絕 = gc_名單.loc[gc_名單['名單類別'].str.contains("拒絕")] 
test_MRK = gc_名單_拒絕.copy()
test_MRK['entityType'] = '3028348436713387'
test_MRK['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')


test_MRK['customItem121__c'] = '1' 
test_MRK['customItem115__c'] = [['1']]  * len(test_MRK)
test_MRK['customItem206__c'] = '9'

def s_norm(s):
    s = s.astype("string")
    s = s.fillna("").str.strip()
    s = s.mask(s.isin(["", "NaN", "<NA>"]), "")
    return s
contact = s_norm(test_MRK.get("連絡人", pd.Series([""]*len(test_MRK))))
reject  = s_norm(test_MRK.get("上一次拒絕", pd.Series([""]*len(test_MRK))))
status  = s_norm(test_MRK.get("最後一次K大狀態", pd.Series([""]*len(test_MRK))))
info = np.where(reject != "", reject, status)
suffix = np.where(info != "", " [" + info + "]", "")
test_MRK["customItem3__c"] = "K大邀約" + contact + ' - 拒絕客戶'
test_MRK['customItem10__c'] = sales_gc['customItem10__c'].iloc[0]

rename_map = {}
if '客戶關係連絡人' in test_MRK.columns:
    rename_map['客戶關係連絡人'] = 'customItem42__c'
else:
    print("Warning: '客戶關係連絡人' column not found for renaming.")
test_MRK.rename(columns=rename_map, inplace=True)
final_columns = [
    'customItem42__c',  'entityType',  'customItem120__c', 'customItem3__c',   'customItem10__c',
    'customItem121__c', 'customItem11__c',  'customItem115__c', 'customItem206__c'  ]
existing_final_columns = [col for col in final_columns if col in test_MRK.columns]
test_MRK = test_MRK[existing_final_columns].head(300)
test_MRK['customItem42__c'] = test_MRK['customItem42__c'].astype(str)
已接觸_list = test_MRK['customItem42__c'].dropna().unique().tolist()
mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), '剔除原因'] = mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), \
      '剔除原因'].apply(lambda x: f"{x}；派給個人CRM" if x else "派給GC")
未接觸 = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()
未接觸.to_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\last_K_invite.xlsx", index=False)
print(f"已成功匯出未接觸名單，共 {len(未接觸)} 筆")
已接觸 = gc_名單[gc_名單['客戶關係連絡人'].isin(已接觸_list)].copy()
today_call = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()



ac_token = kd.get_access_token()
bulk_id = kd.ask_bulk_id() 
kd.insert_to_CRM(bulk_id, test_MRK)
time.sleep(10)
kd.賈維斯1號('>>>5-2. 處理GC每日K大')

gc_名單 = kd.filter_by_target(未接觸, '目標客戶類型', gc_target)
gc_名單 = gc_名單.loc[~gc_名單['名單類別'].str.contains("拒絕")] 
test_MRK = gc_名單.copy()
test_MRK['entityType'] = '3028348436713387'
test_MRK['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')


test_MRK['customItem121__c'] = '1' 
test_MRK['customItem115__c'] = [['1']]  * len(test_MRK)
test_MRK['customItem206__c'] = '9'

def s_norm(s):
    s = s.astype("string")
    s = s.fillna("").str.strip()
    s = s.mask(s.isin(["", "NaN", "<NA>"]), "")
    return s
contact = s_norm(test_MRK.get("連絡人", pd.Series([""]*len(test_MRK))))
reject  = s_norm(test_MRK.get("上一次拒絕", pd.Series([""]*len(test_MRK))))
status  = s_norm(test_MRK.get("最後一次K大狀態", pd.Series([""]*len(test_MRK))))
info = np.where(reject != "", reject, status)
suffix = np.where(info != "", " [" + info + "]", "")
test_MRK["customItem3__c"] = "K大邀約" + contact # + suffix


rename_map = {}
if '客戶關係連絡人' in test_MRK.columns:
    rename_map['客戶關係連絡人'] = 'customItem42__c'
else:
    print("Warning: '客戶關係連絡人' column not found for renaming.")
test_MRK.rename(columns=rename_map, inplace=True)
final_columns = [
    'customItem42__c',  'entityType',  'customItem120__c', 'customItem3__c',   
    'customItem121__c', 'customItem11__c',  'customItem115__c', 'customItem206__c'  ]
existing_final_columns = [col for col in final_columns if col in test_MRK.columns]
test_MRK = test_MRK[existing_final_columns]


sales_gc = sales_total.loc[sales_total['電訪人員'].str.contains("Genesys_廠商")] 
預估通數 = int(sales_gc['預估通數'].iloc[0])
要補K大數 = max(預估通數 - len(df_normal), 0)
test_MRK = test_MRK.head(要補K大數)

test_MRK['customItem10__c'] = sales_gc['customItem10__c'].iloc[0]
test_MRK = test_MRK.head(max(sales_gc['預估通數'].iloc[0] - len(df_normal), 0))
test_MRK['customItem42__c'] = test_MRK['customItem42__c'].astype(str)
已接觸_list = test_MRK['customItem42__c'].dropna().unique().tolist()
mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), '剔除原因'] = mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), \
      '剔除原因'].apply(lambda x: f"{x}；派給個人CRM" if x else "派給GC")
未接觸 = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()
未接觸.to_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\last_K_invite.xlsx", index=False)
print(f"已成功匯出未接觸名單，共 {len(未接觸)} 筆")
已接觸 = gc_名單[gc_名單['客戶關係連絡人'].isin(已接觸_list)].copy()
today_call = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()



ac_token = kd.get_access_token()
bulk_id = kd.ask_bulk_id() 
kd.insert_to_CRM(bulk_id, test_MRK)
time.sleep(10)
kd.賈維斯1號('>>>6. 處理電訪和產顧的交辦')

經營開發 = kd.filter_by_target(未接觸, '目標客戶類型', gc_target)
test_MRK = 經營開發.copy()
test_MRK['entityType'] = '3028348436713387'
test_MRK['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')

test_MRK['customItem121__c'] = '1' 
test_MRK['customItem115__c'] = [['1']]  * len(test_MRK)
test_MRK['customItem206__c'] = '9'
test_MRK['customItem10__c'] = None


def s_norm(s):
    s = s.astype("string")
    s = s.fillna("").str.strip()
    s = s.mask(s.isin(["", "NaN", "<NA>"]), "")
    return s
contact = s_norm(test_MRK.get("連絡人", pd.Series([""]*len(test_MRK))))
reject  = s_norm(test_MRK.get("上一次拒絕", pd.Series([""]*len(test_MRK))))
status  = s_norm(test_MRK.get("最後一次K大狀態", pd.Series([""]*len(test_MRK))))
info = np.where(reject != "", reject, status)
suffix = np.where(info != "", " [" + info + "]", "")
test_MRK["customItem3__c"] = "K大邀約" + contact + suffix


rename_map = {}
if '客戶關係連絡人' in test_MRK.columns:
    rename_map['客戶關係連絡人'] = 'customItem42__c'
else:
    print("Warning: '客戶關係連絡人' column not found for renaming.")
test_MRK.rename(columns=rename_map, inplace=True)
final_columns = [
    'customItem42__c',  'entityType',  'customItem120__c', 'customItem3__c',   
    'customItem121__c', 'customItem11__c',  'customItem115__c', 'customItem206__c'  ]
existing_final_columns = [col for col in final_columns if col in test_MRK.columns]
test_MRK = test_MRK[existing_final_columns]

def round_robin_assign(df, n_parts):
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)  # 打亂順序
    parts = [[] for _ in range(n_parts)]
    for idx, row in df.iterrows():
        parts[idx % n_parts].append(row)
    return [pd.DataFrame(p) for p in parts]

assigned_rows = []
start_idx = 0
for _, row in sales.iterrows():
    count = row['預估通數']
    end_idx = min(start_idx + count, len(test_MRK))
    if start_idx >= len(test_MRK):
        break
    assigned_rows.extend([(row['customItem10__c'], row['dimDepart'])] * (end_idx - start_idx))
    start_idx = end_idx

assigned_rows = assigned_rows[:len(test_MRK)]
assigned_rows = np.array(assigned_rows)
test_MRK_type1 = test_MRK[test_MRK['customItem121__c'] == '1'].copy()
test_MRK_type2 = test_MRK[test_MRK['customItem121__c'] == '2'].copy()
n_sales = len(set([r[0] for r in assigned_rows]))
chunks_type1 = round_robin_assign(test_MRK_type1, n_sales)
chunks_type2 = round_robin_assign(test_MRK_type2, n_sales)

final_df_list = []
for i in range(n_sales):
    combined = pd.concat([chunks_type1[i], chunks_type2[i]], ignore_index=True)
    combined = combined.sample(frac=1, random_state=42).reset_index(drop=True)  # 再打亂一下
    final_df_list.append(combined)

final_df = pd.concat(final_df_list, ignore_index=True)
final_df = final_df.iloc[:len(assigned_rows)].copy()
final_df['customItem10__c'] = assigned_rows[:, 0]
final_df['dimDepart'] = assigned_rows[:, 1]
final_df = final_df.astype(str)
test_MRK = final_df
pivot = test_MRK.pivot_table(
    index=['dimDepart','customItem10__c'],    
    columns='customItem121__c', 
    values='customItem42__c',   
    aggfunc='count',           
    fill_value=0
)
print(pivot.describe())
test_MRK['customItem42__c'] = test_MRK['customItem42__c'].astype(str)
已接觸_list = test_MRK['customItem42__c'].dropna().unique().tolist()
mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), '剔除原因'] = mark_df.loc[mark_df['客戶關係連絡人'].isin(已接觸_list), \
      '剔除原因'].apply(lambda x: f"{x}；派給個人CRM" if x else "派給個人CRM")

未接觸 = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()
未接觸.to_excel(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\last_K_invite.xlsx", index=False)
print(f"已成功匯出未接觸名單，共 {len(未接觸)} 筆")
已接觸 = 經營開發[經營開發['客戶關係連絡人'].isin(已接觸_list)].copy()
today_call = today_call[~today_call['客戶關係連絡人'].isin(已接觸_list)].copy()
bulk_id = kd.ask_bulk_id() 
kd.insert_to_CRM(bulk_id, test_MRK)
time.sleep(10)

filtered_df_, summary_df = kd.screen_by_exclusion_sources(mark_df, 承攬_target)

if '是否會被觸達' in filtered_df_.columns and 'label_rank' in filtered_df_.columns:
    touched_rank_summary = (
        filtered_df_[filtered_df_['是否會被觸達'] == True]
        .groupby('label_rank', dropna=False)
        .size()
        .reset_index(name='名單數')
        .sort_values('名單數', ascending=False)
        .reset_index(drop=True)
    )
else:
    touched_rank_summary = pd.DataFrame(columns=['label_rank', '名單數'])

import requests
summary_header = "| 階段 | 起始 | 剔除 | 剩餘 |\n| :--: | :---------------- | -----: | -----: |"
summary_rows = []
for _, row in summary_df[['階段', '起始', '剔除', '剩餘']].iterrows():
    summary_rows.append(f"| {int(row['階段'])} | {row['起始']} | {row['剔除']} | {row['剩餘']} |")

summary_table = "\n".join([summary_header] + summary_rows)
rank_header = "| 分類 | 名單數 |\n| :---- | -----: |"
rank_rows = []
for _, row in touched_rank_summary.iterrows():
    rank_rows.append(f"| {row['label_rank']} | {row['名單數']} |")
rank_table = "\n".join([rank_header] + rank_rows)

today_str = datetime.now().strftime("%Y-%m-%d %H:%M")
message_md = (
    f"#  K大名單各階段剔除數據{today_str}\n"
    + summary_table
    + "\n\n---\n\n"
    + "##  剩餘名單分類\n"
    + rank_table
)
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=be0d8632-c396-443f-95db-cc8711bde920"

payload = {
    "msgtype": "markdown_v2",
    "markdown_v2": {"content": message_md}
}

resp = requests.post(webhook_url, json=payload)
if resp.status_code == 200:
    print(" 已成功推送 Markdown 表格至企微群。")
else:
    print(f" 推送失敗：{resp.status_code}, {resp.text}")

sql_query = (
        f'''    
        select id,name, createdAt, customItem42__c,customItem10__c, customItem3__c, approvalStatus,customItem191__c,
        entityType,customItem8__c,customItem42__c.name 客戶關係聯絡人代號,customItem120__c, customItem121__c,
        customItem42__c.contactPhone__c__c 客戶手機號, customItem42__c.contactCode__c__c 客戶代號
        from customEntity14__c 
        where entityType in ('2904963933786093','3028348436713387') and createdAt >= {month_ago_three}
        and customItem120__c >= {today_begin} and customItem120__c < {today_end}
        ''')
Tasks_df = kd.get_data_from_CRM(sql_query)
kd.賈維斯1號('>>>7. 查重與刪除記錄')

Tasks_df1 = Tasks_df.copy().sort_values(by='id')

Tasks_df1 = Tasks_df1[
    Tasks_df1['customItem8__c'].astype(str).str.contains("等待回應", na=False) &
    Tasks_df1['客戶手機號'].notna() &
    (Tasks_df1['客戶手機號'].astype(str).str.strip() != "")]
duplicate_phones = Tasks_df1['客戶手機號'].value_counts()
duplicate_phones = duplicate_phones[duplicate_phones > 1].index
dup_df = Tasks_df1[Tasks_df1['客戶手機號'].isin(duplicate_phones)].copy()
dup_df = dup_df.sort_values(by='id')
PREFERRED_CUSTOMITEM10 = "3414568030034443"
keep_ids = (
    dup_df.groupby('客戶手機號')
    .apply(lambda g: g[g['customItem10__c'] == PREFERRED_CUSTOMITEM10].iloc[0]
           if any(g['customItem10__c'] == PREFERRED_CUSTOMITEM10)
           else g.iloc[0])
    .reset_index(drop=True)['id'])

dup_df = dup_df[~dup_df['id'].isin(keep_ids)]

time.sleep(10)
result_df = kd.get_procInstId(dup_df)
time.sleep(3)

withdraw_results = kd.withdraw_tasks(result_df)
#### 要刪除了
delete_results = kd.delete_from_CRM(result_df)

kd.賈維斯1號('>>>8. 開始提交任務')
time.sleep(10)

kd.submit_to_crm_tw(sql_query, max_workers=16,attempts_per_id=3,show_progress=True)



kd.賈維斯1號('>>>9. 老大, 每日K大交辦任務提交完成, 撒花~~~~')





