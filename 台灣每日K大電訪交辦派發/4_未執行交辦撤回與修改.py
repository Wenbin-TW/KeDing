
import os
import sys
import time
import json
import threading
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import requests
import pyodbc
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd


month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000

date_to_convert = datetime(2025,5, 1)
timestamp = int(date_to_convert.timestamp() * 1000)

timestamp2 = int(datetime(2025,10, 26).timestamp() * 1000)


kd.賈維斯1號('1.數據初始化完成')
'''
select from Tasks withdraw 
'''
Tasks_df_all = kd.get_data_from_CRM (f'''
        select id,name,customItem10__c, customItem120__c,customItem3__c,workflowStageName,approvalStatus,customItem8__c,customItem206__c
        ,customItem121__c,customItem65__c, customItem42__c.name 客戶關係連絡人編號,entityType,createdBy,customItem119__c 無效電訪類型
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387')
        and createdAt >= {month_ago_three} 
        ''')

Tasks_df1 = Tasks_df_all.loc[Tasks_df_all['entityType'].astype(str).str.contains("3028348436713387", na=False) &
    Tasks_df_all['createdBy'].astype(str).str.contains("3628254003531750", na=False)]

Tasks_df1 = Tasks_df1.loc[Tasks_df1['customItem8__c'].astype(str).str.contains("等待")]  
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem8__c'].astype(str).str.contains("進行中")]  
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem121__c'].astype(str).str.contains("已邀約")] 
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem3__c'].astype(str).str.contains("K大視訊|已邀約")] 
Tasks_df1 = Tasks_df1.loc[~Tasks_df1['customItem65__c'].astype(str).str.contains("已邀")] 

Tasks_df2 =  Tasks_df_all.loc[Tasks_df_all['無效電訪類型'].astype(str).str.contains("未接", na=False) &
    Tasks_df_all['customItem121__c'].astype(str).str.contains("未接", na=False)]


kd.賈維斯1號('2.等待回應交辦與普查獲取成功, 開始撤回並刪除')
result_df = kd.get_procInstId(Tasks_df1)
time.sleep(3)

withdraw_results = kd.withdraw_tasks(result_df)

kd.賈維斯1號('3.撤回成功, 開始刪除並寫入公槽記錄')

delete_results = kd.delete_from_CRM(result_df)
today = datetime.today()
today_str = today.strftime('%Y-%m-%d')
file_date_str = today.strftime('%Y%m%d')

Tasks_df1['撤回日期'] = today_str
output_path = fr"Z:\18_各部門共享區\15_數據中心課\文斌\withdraw\withdraw_{file_date_str}.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    Tasks_df1.to_excel(writer, sheet_name='交辦撤回', index=False)


print(f"已成功輸出至：{output_path}")


kd.賈維斯1號('4.撤回寫入-->Done, Next-->爽約交辦')



time.sleep(30)






user_df = kd.get_data_from_CRM ('select id, name, dimDepart from user')

today = datetime.today()
today_str = today.strftime('%Y-%m-%d')
month_ago_3ts = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
five_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-5)).timestamp() * 1000)
month_ago_1ts = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
one_days_after = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)


Tasks_df_total = kd.get_data_from_CRM (f'''
        select id,name,createdAt , customItem10__c, customItem3__c,customItem8__c,customItem49__c 區域,customItem93__c,
        customItem121__c, customItem120__c 期望完成日期, customItem45__c, customItem42__c,customItem42__c.name,customItem65__c, 
        entityType,dimDepart,customItem11__c,customItem116__c,createdBy
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387') and customItem120__c >= {month_ago_3ts} and customItem120__c < {one_days_after} 
        ''')
Tasks_df = Tasks_df_total.copy()
matched_users = Tasks_df_total[Tasks_df_total['customItem65__c'].astype(str).str.contains("拒絕|完成|非目標", na=False)]['customItem42__c.name'].dropna().tolist()

Tasks_df = Tasks_df.loc[Tasks_df['customItem8__c'].astype(str).str.contains("等待")]  
Tasks_df['customItem121__c']= Tasks_df['customItem121__c'].astype(str)
Tasks_df['customItem116__c']= Tasks_df['customItem116__c'].astype(str)
kd.convert_to_date(Tasks_df,'期望完成日期')
kd.convert_to_date(Tasks_df,'createdAt')
Pass_user = list(set( matched_users))

museum = kd.get_data_from_CRM (f'''
        select customItem23__c.name 客戶關係連絡人,
        customItem51__c 是否到訪,customItem1__c 預約參訪日期
        from customEntity43__c
        where customItem1__c >= {month_ago_3ts}
        ''')

museum = museum[museum['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + museum['客戶關係連絡人'].dropna().tolist()))


booking = kd.get_data_from_CRM (f'''
        select customItem2__c.name 客戶關係連絡人,
        customItem8__c 是否到訪,customItem31__c 預約日期
        from customEntity24__c
        where customItem1__c >= {month_ago_3ts}
        ''')
booking = booking[booking['是否到訪'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)]
Pass_user = list(set(Pass_user + booking['客戶關係連絡人'].dropna().tolist()))

Tasks_df = Tasks_df[~Tasks_df['customItem42__c.name'].isin(Pass_user)]





Tasks_df = Tasks_df[~Tasks_df['customItem65__c'].str.contains("拒絕|完成|非目標", na=False)]
today = datetime.now().date()
Tasks_df1 = Tasks_df.loc[Tasks_df['customItem3__c'].str.contains("已邀|K大視訊")] 
Tasks_df1 = Tasks_df1.drop_duplicates(subset=['customItem42__c'], keep='last')
Tasks_df1 = Tasks_df1[Tasks_df1['期望完成日期'] <= today_str]

Tasks_df2 = Tasks_df1.loc[Tasks_df1['customItem3__c'].astype(str).str.contains("已邀|K大視訊")] 
Tasks_df3 = Tasks_df1.loc[~Tasks_df1['customItem3__c'].str.contains("已邀|K大視訊|新客")]

Combined_Tasks_df = pd.concat([Tasks_df2, Tasks_df3])
Combined_Tasks_df['customItem120__c'] = today_str  
Combined_Tasks_df = Combined_Tasks_df.reset_index(drop=True)
翊盛承攬_list = user_df[user_df['name'].astype(str).str.contains("翊盛|承攬", na=False)]['id'].tolist()
Combined_Tasks_df = Combined_Tasks_df[~Combined_Tasks_df['customItem10__c'].isin(翊盛承攬_list)]
mask = Combined_Tasks_df["customItem10__c"] == "3414568030034443"
Combined_Tasks_df.loc[mask, "customItem10__c"] = Combined_Tasks_df.loc[mask, "createdBy"]
Combined_Tasks_df = Combined_Tasks_df[['id','customItem120__c','customItem10__c']]

bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, Combined_Tasks_df)

kd.賈維斯1號('5.爽約更新成功, Next-->未接1交辦')

time.sleep(10)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)

missed = kd.get_data_from_CRM (f'''
        select id,name,customItem10__c, customItem3__c,customItem8__c,customItem152__c.name 型錄發放申請編號,
       customItem121__c, customItem45__c, customItem42__c, entityType,customItem49__c 區域代碼,customItem11__c,
       customItem57__c,customItem59__c,customItem39__c,customItem153__c, customItem116__c 目標客戶類型, approvalStatus
        from customEntity14__c
        where entityType in ('3028348436713387') and createdAt >= {month_ago_3ts}
        ''')
missed = missed[missed['customItem121__c'].astype(str).str.contains("未接1", na=False)]
missed = missed.loc[missed['customItem8__c'].astype(str).str.contains("等待")]  

missed = missed[~missed['customItem57__c'].str.contains("建案款") & ~missed['區域代碼'].str.contains("TW-Z")]
result_df = kd.get_procInstId(missed)
withdraw_results = kd.withdraw_tasks(result_df)

missed['customItem10__c'] = user_df[user_df['name'] == 'Genesys_廠商']['id'].iloc[0]
missed['customItem120__c'] = today

missed = missed.reset_index(drop = True)

missed = missed[['id','customItem10__c','customItem120__c']]


bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, missed)

time.sleep(10)


kd.賈維斯1號('6.未接1交辦執行人改為GC-->Done, Next-->新客電訪交辦')

missed = kd.get_data_from_CRM (f'''
        select id,name,customItem10__c, customItem3__c,customItem8__c,customItem152__c.name 型錄發放申請編號,
       customItem121__c, customItem45__c, customItem42__c, entityType,customItem49__c 區域代碼,customItem11__c,
       customItem57__c,customItem59__c,customItem39__c,customItem153__c, customItem116__c 目標客戶類型, approvalStatus
        from customEntity14__c
        where entityType in ('3280291971403721') and createdAt >= {month_ago_1ts}
        ''')
missed = missed[missed['customItem121__c'].astype(str).str.contains("未接", na=False)]
missed = missed.loc[missed['customItem8__c'].astype(str).str.contains("等待")]  

result_df = kd.get_procInstId(missed)
withdraw_results = kd.withdraw_tasks(result_df)
missed['customItem120__c'] = today
missed = missed[['id','customItem120__c']]

bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, missed)

time.sleep(10)





kd.賈維斯1號('7.新客電訪交辦-->Done, Next-->承攬的已邀約時間未定')

sales_total = kd.get_data_from_CRM (f'''
        select customItem1__c customItem10__c, customItem3__c 預估通數,customItem1__c.dimDepart dimDepart,customItem4__c,
        customItem2__c 電訪人員類型,customItem1__c.name 電訪人員
        from customEntity42__c
        where customItem5__c = 1
        ''')
承攬 = sales_total.loc[sales_total['customItem4__c'].astype(str).str.contains("承攬")]  
已邀約時間未定 = Tasks_df_total.copy()
已邀約時間未定 = 已邀約時間未定[已邀約時間未定['customItem10__c'].isin(承攬['customItem10__c'])]

已邀約時間未定 = 已邀約時間未定[ 已邀約時間未定['customItem121__c'].astype(str).str.contains("已邀約", na=False)
    & 已邀約時間未定['customItem8__c'].astype(str).str.contains("等待", na=False)]


sales_total1 = kd.get_data_from_CRM('''
        select 
        customItem1__c            customItem10__c,
        customItem3__c            預估通數,
        customItem1__c.dimDepart  dimDepart,
        customItem2__c            電訪人員類型,
        customItem1__c.name       電訪人員
        from customEntity42__c
        where customItem5__c = 1
        ''')
sales_total = kd.get_data_from_CRM('''   
        select customItem5__c.name 電訪人員, customItem25__c 獎金用職級
        from customEntity31__c  where customItem10__c is null''')

sales_total = sales_total[sales_total['獎金用職級'].astype(str).str.contains("電訪專員A", na=False)]
sales_total = pd.merge(sales_total1, sales_total, on = '電訪人員', how = 'right')
sales_total['預估通數'] = pd.to_numeric(sales_total['預估通數'], errors='coerce').fillna(0).astype(int)
sales_total = sales_total[sales_total['預估通數'] > 0].reset_index(drop=True)
sales = sales_total[sales_total['電訪人員類型'].astype(str).str.contains("未接", na=False)]
sales = sales.loc[~sales['電訪人員'].astype(str).str.contains("Genesys_廠商|#|＃|電訪", na=False)]
options = sales['customItem10__c'].dropna().tolist()


已邀約時間未定['customItem10__c'] = np.random.choice(options, size=len(已邀約時間未定), replace=True)
已邀約時間未定['customItem120__c'] = today


已邀約時間未定 = 已邀約時間未定[['id','customItem10__c','customItem120__c']]

result = kd.withdraw_with_delegate(已邀約時間未定, get_access_token_fn=kd.get_access_token)
bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, 已邀約時間未定)




kd.賈維斯1號('8.承攬的已邀約時間未定-->Done, Next-->翊盛未上線交辦')

user_list = user_df[user_df['name'].astype(str).str.contains("翊盛_電訪員", na=False)]['id'].tolist()

today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
five_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-15)).timestamp() * 1000)

missed = kd.get_data_from_CRM (f'''
        select id, name , customItem10__c, customItem120__c,customItem121__c, customItem212__c ,customItem8__c,approvalStatus
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387') and createdAt >= {five_days_ago}
        and customItem120__c >= {today_begin} and customItem120__c < {today_end}
        and customItem10__c  in  ('3859210868133772','3859213635030915','3869002488895888','3869003874684819')
        ''')
missed = missed[missed['customItem121__c'].astype(str).str.contains("未上線", na=False)]



missed['customItem10__c'] = user_df[user_df['name'] == '張淇雁']['id'].iloc[0]
missed['customItem120__c'] = today
missed = missed[['id','customItem10__c','customItem120__c']]

result_df = kd.get_procInstId(missed)
withdraw_results = kd.withdraw_tasks(result_df)

bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, missed)

time.sleep(10)







kd.賈維斯1號('9.翊盛未上線交辦-->Done, Next-->離職人員交辦')

Tasks_df_quit_total = kd.get_data_from_CRM (f'''
        select id,name,createdAt , customItem10__c, customItem3__c,customItem8__c,customItem49__c 區域,customItem93__c,
        customItem121__c, customItem120__c 期望完成日期, customItem45__c, customItem42__c,customItem42__c.name,customItem65__c, 
        entityType,dimDepart,customItem11__c,customItem116__c,createdBy
        from customEntity14__c
        where entityType in ('2904963933786093','3028348436713387') and createdAt >= {month_ago_1ts} 
        ''')


today = datetime.today() + timedelta(days=1)
tomorrow_str = today.strftime('%Y-%m-%d')
user_df = kd.get_data_from_CRM (f'''
                                select id, name, local,dimDepart,customItem182__c 
                                from user 
                                where customItem182__c  >= {month_ago_1ts}  and local = 'TW'
                                ''')


Tasks_df_quit = Tasks_df_quit_total.copy()

Tasks_df_quit = Tasks_df_quit.loc[Tasks_df_quit['customItem8__c'].astype(str).str.contains("等待")]  
Tasks_df_quit = Tasks_df_quit.loc[~Tasks_df_quit['customItem3__c'].astype(str).str.contains("已邀約")]  

filtered_df = Tasks_df_quit[Tasks_df_quit['customItem10__c'].isin(user_df['id'])]



result_df = kd.get_procInstId(filtered_df)
withdraw_results = kd.withdraw_tasks(result_df)


sales_total = kd.get_data_from_CRM (f'''
        select customItem1__c customItem10__c, customItem3__c 預估通數,customItem1__c.dimDepart dimDepart,
        customItem2__c 電訪人員類型,customItem1__c.name 電訪人員
        from customEntity42__c
        where customItem5__c = 1
        ''')
sales_total['預估通數'] = pd.to_numeric(sales_total['預估通數'], errors='coerce').fillna(0).astype(int)
sales_total = sales_total[sales_total['預估通數'] > 0].reset_index(drop=True)
sales = sales_total[sales_total['電訪人員類型'].astype(str).str.contains("未接", na=False)]
sales = sales.loc[
    ~sales['電訪人員'].str.contains("Genesys_廠商", na=False) &
    ~sales['電訪人員'].str.contains("#", na=False)&
    ~sales['電訪人員'].str.contains("＃", na=False)
    ]

options = sales['customItem10__c'].dropna().tolist()

filtered_df['customItem10__c'] = np.random.choice(options, size=len(filtered_df), replace=True)
filtered_df['customItem120__c'] = tomorrow_str

filtered_df = filtered_df[['id','customItem10__c','customItem120__c']]


bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, filtered_df)

Tasks_df_quit_already = Tasks_df_quit_total.copy()
Tasks_df_quit_already = Tasks_df_quit_already.loc[Tasks_df_quit_already['customItem8__c'].astype(str).str.contains("等待")]  
Tasks_df_quit_already = Tasks_df_quit_already.loc[Tasks_df_quit_already['customItem3__c'].astype(str).str.contains("已邀約")]  
filtered_df = Tasks_df_quit_already[Tasks_df_quit_already['customItem10__c'].isin(user_df['id'])]


sales_total1 = kd.get_data_from_CRM('''
        select 
        customItem1__c            customItem10__c,
        customItem3__c            預估通數,
        customItem1__c.dimDepart  dimDepart,
        customItem2__c            電訪人員類型,
        customItem1__c.name       電訪人員
        from customEntity42__c
        where customItem5__c = 1
        ''')
sales_total = kd.get_data_from_CRM('''   
        select customItem5__c.name 電訪人員, customItem25__c 獎金用職級
        from customEntity31__c  where customItem10__c is null''')

sales_total = sales_total[sales_total['獎金用職級'].astype(str).str.contains("電訪專員A", na=False)]
sales_total = pd.merge(sales_total1, sales_total, on = '電訪人員', how = 'right')
sales_total['預估通數'] = pd.to_numeric(sales_total['預估通數'], errors='coerce').fillna(0).astype(int)
sales_total = sales_total[sales_total['預估通數'] > 0].reset_index(drop=True)
sales = sales_total[sales_total['電訪人員類型'].astype(str).str.contains("未接", na=False)]
sales = sales.loc[~sales['電訪人員'].astype(str).str.contains("Genesys_廠商|#|＃|電訪", na=False)]
options = sales['customItem10__c'].dropna().tolist()


filtered_df['customItem10__c'] = np.random.choice(options, size=len(filtered_df), replace=True)
filtered_df['customItem120__c'] = today

filtered_df = filtered_df[['id','customItem10__c','customItem120__c']]

result = kd.withdraw_with_delegate(filtered_df, get_access_token_fn=kd.get_access_token)
bulk_id = kd.ask_bulk_id(operation="update") 
kd.insert_to_CRM(bulk_id, filtered_df)

kd.賈維斯1號('10.離職人員交辦-->Done, 流程結束')




two_weeks_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-14)).timestamp() * 1000)
'''
select from Tasks withdraw 
'''
Tasks_df_all = kd.get_data_from_CRM (f'''
        select id dataId,name,customItem10__c, customItem120__c,customItem3__c,workflowStageName,approvalStatus,customItem8__c,customItem206__c
        ,customItem121__c,customItem65__c, customItem42__c.name 客戶關係連絡人編號,entityType,createdBy,customItem119__c 無效電訪類型,
        customItem93__c 工作內容一,customItem15__c 工作內容二
        from customEntity14__c
        where customItem3__c like '%拒K後電訪交辦%' 
        and approvalStatus = 0 and  customItem8__c = 1
        and customItem210__c <= {two_weeks_ago} 
        ''')


delete_results_census = kd.delete_from_CRM(Tasks_df_all, object_name="customEntity14__c")

kd.賈維斯1號('11.業二交辦自動化刪除完成, 流程結束')
