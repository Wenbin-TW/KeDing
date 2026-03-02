


import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil
import os
from pathlib import Path
import sys
import numpy as np
import time
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import common as kd

DAILY_LIMIT = 50   # 每日派發上限
low_alarm = 50     # 低於此數值會發警報

def clean_driver_df(df):
    clean_cols = ["執行人", "客戶關係連絡人"]
    raw_df = df.copy()
    df[clean_cols] = (df[clean_cols].astype(str).apply(lambda s: s.str.strip().replace({"nan": "", "None": ""})))
    mask_null = (df[clean_cols] == "").any(axis=1)
    removed_null = raw_df[mask_null].copy()
    removed_null["去除原因"] = "執行人 或 客戶關係連絡人為空"
    df_non_null = df[~mask_null].copy()
    dup_mask = df_non_null.duplicated(subset=["客戶關係連絡人"], keep='first')
    removed_dup = df_non_null[dup_mask].copy()
    removed_dup["去除原因"] = "客戶關係連絡人 重複"
    clean_df = df_non_null[~dup_mask].copy()
    removed_df = pd.concat([removed_null, removed_dup], ignore_index=True)
    return clean_df, removed_df


T5_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-5)).timestamp() * 1000)
T7_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-7)).timestamp() * 1000)
T17_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-17)).timestamp() * 1000)
T30_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-30)).timestamp() * 1000)
month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_one = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
yest_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-1)).timestamp() * 1000)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
today = datetime.today().strftime("%Y-%m-%d")
six_months_ago = (datetime.today() - relativedelta(months=6)).strftime("%Y-%m-%d")
yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
today_str = datetime.today().strftime("%Y-%m-%d")

kd.賈維斯1號('1.業1數據初始化完成, 開始獲取基本資料')
區域業務 = pd.read_excel("Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/91.台灣業務負責區域表/台灣業務負責區域_2026.xlsx", dtype='object',sheet_name='區域表(報表用)',skiprows=1 ).ffill()
區域業務 = 區域業務[['小區主管','小區','大區','系統櫃\n事業部','經營業務']].rename(columns={"小區": "區域1", "大區": "區域2"})
區域業務 = 區域業務[區域業務['區域1'].astype(str).str.contains('TW', na=False) &區域業務['區域1'].notna()]
區域業務 = 區域業務.applymap(lambda x: x.strip() if isinstance(x, str) else x)
company_group = kd.get_data_from_MSSQL('select company_id 公司代號,region_group 區域1 from [bi_ready].[dbo].[crm_tw_account_datail]')
contact_df =  kd.get_data_from_MSSQL('select * from [bi_ready].[dbo].[crm_tw_contact_datail]')
user_all = kd.get_data_from_CRM (f''' select id,employeeCode 員工編號 from user ''')
user_df = kd.get_data_from_CRM ('''
        select customItem21__c 員工編號,customItem5__c.name 姓名,customItem2__c.dimDepart.departName 負責區域, customItem25__c 獎金用職級,  customItem9__c 生效日期 ,customItem10__c 失效日期,customItem12__c 人員職位
        from customEntity31__c
        where customItem25__c = 2 and customItem12__c = 1 and customItem10__c is  null
                                and customItem2__c.dimDepart.departName like '%TW%'
        ''')
user_df = pd.merge(user_df, user_all, on = '員工編號', how= 'left')
user_df = pd.merge(user_df, 區域業務, left_on = '負責區域', right_on = '區域1', how = 'left')
kd.賈維斯1號('2.讀取昨日數據並進行更新')

yest_file = (fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\5_派發與回收\{yesterday}交辦留存.xlsx")
if not os.path.exists(yest_file):
    print(f"找不到檔案: {yest_file}，跳過更新步驟。")
else:
    try:
        df_yesterday = pd.read_excel(yest_file, dtype=str)
        df_yesterday["id"] = df_yesterday["id"].astype(str)
    except PermissionError:
        print(f"錯誤：檔案 {yest_file} 正被開啟中，請關閉後再試。")

test_today = kd.get_data_from_CRM( f'''
        select id,name 交辦編號,customItem10__c, dimDepart.departName 負責區域, customItem3__c 工作主旨,customItem8__c 執行狀態 ,approvalStatus
        ,customItem42__c.name 客戶關係連絡人編號,entityType,customItem142__c 連絡人代號,customItem42__c.id 客戶關係連絡人,
        customItem210__c 下交辦日期,customItem118__c 公司代號,customItem11__c.id customItem11__c
        from customEntity14__c
        where entityType in ('2766438431723495')   --業務類型= 一般交辦
        and createdBy = '3628254003531750'
        -- and customItem120__c = {yest_begin}
        and ( customItem3__c like '%司機推廣%' or customItem3__c like '%經營專案-派樣電訪%' or 
        customItem3__c like '%經營專案-14天前拒K%' or customItem3__c like '%經營專案-六個月以上未購%' )
            ''') 

today_col_name = f"執行狀態_{datetime.today().strftime('%Y-%m-%d')}"
import ast

if not test_today.empty:
    update_cols = test_today[["id", "執行狀態"]].copy()
    def parse_status(x):
        if pd.isna(x):
            return ""
        x = str(x).strip()
        if x.startswith("[") and x.endswith("]"):
            try:
                parsed = ast.literal_eval(x)
                if isinstance(parsed, list) and len(parsed) > 0:
                    return str(parsed[0])
                else:
                    return ""
            except Exception:
                return ""
        return x

    update_cols["執行狀態"] = update_cols["執行狀態"].apply(parse_status)

    update_cols = update_cols.rename(columns={"執行狀態": today_col_name})

    if 'df_yesterday' in locals():
        df_merge = df_yesterday.merge(update_cols, on="id", how="left")

        df_merge = df_merge.map(
            lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x
        )

        try:
            df_merge.to_excel(yest_file, index=False, sheet_name="今日可觸達")
            print(f"成功更新檔案：{yest_file}")
        except PermissionError:
            print(f"存檔失敗：請確認 {yest_file} 未被開啟。")
mask_waiting = test_today['執行狀態'].astype(str).str.contains("等待回應", na=False)
tasks_to_withdraw = test_today.loc[mask_waiting]
if not tasks_to_withdraw.empty:
    print(f"發現 {len(tasks_to_withdraw)} 筆等待回應的任務，準備回收...")
    withdraw_file = fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\5_派發與回收\{today_str}撤回刪除留存.xlsx"
    try:
        tasks_to_withdraw.to_excel(withdraw_file, index=False)
        print(f"撤回名單已存檔至: {withdraw_file}")
    except Exception as e:
        print(f"撤回名單存檔失敗: {e}")

    result_df = kd.get_procInstId(tasks_to_withdraw)
    time.sleep(3)
    try:
        withdraw_results = kd.withdraw_tasks(result_df)
        print("撤回完成")
        delete_results = kd.delete_from_CRM(result_df)
        print("刪除完成")
    except Exception as e:
        print(f"回收過程中發生錯誤: {e}")
else:
    print("沒有需要回收的任務。")

kd.賈維斯1號('3.更新並回收完成,開始獲取交辦池')
time.sleep(10)
generaltest_pool = kd.get_data_from_CRM(
            f'''
SELECT name 交辦編號, entityType, customItem8__c status, customItem10__c.name empl, customItem10__c.employeeCode emplID
, customItem13__c date, customItem121__c 電訪狀態, updatedAt 修改日期, customItem116__c 目標客戶類型
, customItem53__c 接收交辦日期, customItem49__c 資料區域, customItem3__c 工作主旨,customItem45__c 執行內容說明
, customItem119__c 無效電訪類型, customItem120__c 期望完成日期, customItem154__c 需下交辦類型
, customItem11__c.accountName 公司名稱, customItem123__c 公司電話, customItem56__c 連絡人, customItem55__c 連絡人手機號
, customItem63__c K大邀約日期時間, createdAt 創建日期, customItem42__c.name 客戶關係連絡人, customItem118__c 公司代號
FROM customEntity14__c
WHERE  customItem13__c >= {month_ago_three} and
        ((entityType = '3082115462568332' and customItem3__c like '%K大後%' ) or
        (entityType = '2766438431723495' and (customItem3__c like '司機推廣%' or customItem3__c like '派樣%'
          or customItem3__c like '%經營專案%' or customItem3__c like '%系統補名單%'  or customItem3__c like '%六個月以上未購%') ) )
            ''') 
keywords = ["K大後", "經營專案", "六個月以上未購", "司機推廣"]
sys_kicked = generaltest_pool[~generaltest_pool['status'].astype(str).str.contains("等待回應")]
mask_keyword = sys_kicked["工作主旨"].astype(str).str.contains("|".join(keywords))
sys_kicked = sys_kicked[mask_keyword]
kd.convert_to_date(sys_kicked,'date')
kd.賈維斯1號('4.獲取交辦池完成,開始處理手動交辦--司機推廣')
driver_file = r"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\1_司機"
driver_keyword = "司機推廣名單派發"
driver_path = kd.get_latest_excel(driver_file, driver_keyword)
driver_df = pd.read_excel(driver_path, dtype='object').rename(columns={"客戶關係連絡人編號": "客戶關係連絡人"})
driver_df["文件名"] = os.path.splitext(os.path.basename(driver_path))[0]
driver_df["id"] = driver_df["文件名"] + "_" + driver_df["客戶關係連絡人"].astype(str)

driver_df = driver_df.drop_duplicates(subset=['SAP客代','客戶關係連絡人','上傳日期','工作主旨','工作內容(二)'], keep='first')
clean_df, removed_df = clean_driver_df(driver_df)
driver_df = pd.concat([clean_df, removed_df], ignore_index=True)
kd.write_to_sql(clean_df, 'clean_data','generaltest_driver',  if_exists="update", dedup_keys=['id'])
time.sleep(10)
driver_sql = f"""
                SELECT *
                FROM [clean_data].[dbo].[generaltest_driver]
                WHERE '{today}' BETWEEN 上傳日期 AND 最後派發日期
                AND (LEN(去除原因) < 1 OR 去除原因 IS NULL)
                ORDER BY 上傳日期
                """
today_driver = kd.get_data_from_MSSQL(driver_sql)



driver_kicked = generaltest_pool[generaltest_pool['status'].astype(str).str.contains("任務完成|進行中")]
driver_recent = driver_kicked["date"].astype(float)>= month_ago_one
driver_keyword = driver_kicked["工作主旨"].astype(str).str.contains( "K大後|司機推廣", na=False)
driver_invalid = driver_kicked["無效電訪類型"].isna() | (driver_kicked["無效電訪類型"].astype(str).str.strip() == "")
driver_kicked = driver_kicked[driver_keyword & driver_invalid & driver_recent]

case_map = (  driver_kicked.groupby("客戶關係連絡人")["交辦編號"].apply(lambda x: ",".join(str(i) for i in x.unique())) .to_dict())

today_driver["去除原因"] = today_driver["去除原因"].fillna("")
def append_kd_reason(row):
    contact = row["客戶關係連絡人"]
    if contact in case_map:  
        case_ids = case_map[contact]
        reason = f"已完成交辦：{case_ids}"
        if row["去除原因"]:
            return f"{row['去除原因']}，{reason}"
        else:
            return reason
    else:
        return row["去除原因"]

today_driver["去除原因"] = today_driver.apply(append_kd_reason, axis=1)
kd.write_to_sql(today_driver, 'clean_data', 'generaltest_driver', if_exists="update", dedup_keys=['id'])
time.sleep(10)


update_reason = today_driver[["id", "去除原因"]].copy()
driver_df = driver_df.merge(update_reason, on="id", how="left", suffixes=("", "_new"))
driver_df["去除原因"] = driver_df.apply(lambda row: row["去除原因_new"] if pd.notna(row["去除原因_new"]) and row["去除原因_new"] != "" else row["去除原因"],axis=1)
driver_df = driver_df.drop(columns=["去除原因_new"])
driver_df.to_excel(f'{driver_file}/數據清理/數據清理_{today}.xlsx',index = False)

today_driver = today_driver.drop_duplicates(subset=['客戶關係連絡人'], keep='first')
expired = driver_df[ (pd.to_datetime(driver_df["最後派發日期"], errors="coerce") < pd.to_datetime(today)) &
    (driver_df["去除原因"].isna() | (driver_df["去除原因"].astype(str).str.strip() == ""))].copy()
expired["去除原因"] = "超出期限，不予派發"
driver_df = driver_df.merge( expired[["id", "去除原因"]], on="id",how="left",suffixes=("", "_expired"))
driver_df["去除原因"] = driver_df.apply( lambda row: row["去除原因_expired"] if pd.notna(row["去除原因_expired"]) else row["去除原因"], axis=1)
driver_df = driver_df.drop(columns=["去除原因_expired"])
driver_df.to_excel(f'{driver_file}/數據清理/數據清理_{today}.xlsx',index = False)
final_update = today_driver[["id", "去除原因"]].copy()
final_update = final_update.dropna(subset=["去除原因"])
final_update = final_update[final_update["去除原因"].astype(str).str.strip() != ""]
kd.write_to_sql(  final_update, 'clean_data', 'generaltest_driver', if_exists="update", dedup_keys=['id'])

time.sleep(10)


today_driver = kd.get_data_from_MSSQL(driver_sql)
today_driver = pd.merge(today_driver, contact_df[['customItem11__c','公司代號','name','客戶關係連絡人']]
                        .rename(columns={'客戶關係連絡人':'customItem42__c','name':'客戶關係連絡人'}), 
                        on = ['公司代號','客戶關係連絡人'], how= 'left')
today_driver["排序"] = 1
today_driver = pd.merge(today_driver, user_df[['負責區域','id','姓名']].rename(columns={'id':'customItem10__c','負責區域':'區域1'}), on = '區域1', how= 'left')



today_driver['entityType'] = '2766438431723495'
today_driver['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')
today_driver['customItem3__c'] = today_driver['工作主旨']
today_driver['customItem45__c'] = today_driver['執行內容說明']
today_driver["customItem124__c"] = f"經營業務{ datetime.now().strftime("%y-%m")}"
today_driver['customItem113__c'] = np.where(today_driver['工作主旨'].astype(str).str.contains("司機推廣", na=False), '1', '2')
today_driver["customItem15__c"] = today_driver['工作內容(二)']
final_columns = [
    'entityType',  'customItem124__c',  'customItem113__c', 'customItem120__c', 'customItem11__c',
    'customItem15__c', 'customItem3__c',  'customItem42__c', 'customItem10__c' ,'customItem45__c','排序','區域1','客戶關係連絡人' ]
driver_test = today_driver[[col for col in final_columns if col in today_driver.columns]]
kd.賈維斯1號('5.司機名單處理完成, 開始處理手動交辦--派樣')
time.sleep(10)
project_file = r"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\2_派樣"
outer = kd.get_data_from_CRM ('''select customItem21__c 員工編號 from customEntity31__c
        where customItem10__c is  null and customItem2__c.dimDepart.departName like '%TW%'
            and customItem25__c in (1,18) ''') #外勤業務/開發業務
outer_list = ",".join([f"'{x}'" for x in (outer["員工編號"].dropna().astype(str).tolist())])
onter_df = kd.get_data_from_CRM (f''' select id from user where employeeCode in ({outer_list})''')
onter_df.loc[len(onter_df)] = "3221972041390421"  # 增加林儀婷
outer_list = ",".join([f"'{x}'" for x in (onter_df["id"].dropna().astype(str).tolist())])


gift_df = kd.get_data_from_CRM(f'''
        select id 發放id, name 型錄禮品申請編號,account__c.accountName 公司名稱, account__c.customItem322__c 目標客戶類型,
            type__c 公司型態, customItem93__c 嘉義寄送日期,dimDepart.departName 區域1,customItem45__c 執行內容說明,
            requestOwner__c ,customItem142__c 產品類別,entityType,customItem25__c customItem42__c,
            customItem25__c.name 客戶關係連絡人,account__c.id customItem11__c,customItem97__c 申請物品
        from customEntity25__c
        where  dimDepart.departName like 'TW%' 
                and entityType = '2906694858215364'         -- 1.業務類型＝型錄/樣板/禮品發放申請
                and type__c in ('C','DC','DD')              -- 2.公司型態＝C/DC/DD
                and account__c.customItem322__c  = 2        -- 3.目標客戶類型=經營客戶
                and (customItem93__c between {T17_days_ago} and {T7_days_ago})         -- 4.嘉義寄送時間=D-7
                and dimDepart.departName not like '%TW-Z%'  -- 5.[排除]所屬部門=TW-Z1~TW-Z7
                and requestOwner__c not in ({outer_list})   -- 6.[排除]申請人=外勤業務/開發業務/林儀婷
                and customItem142__c not like '%贈禮%'      -- 7.[排除]產品類別(文本)=含"贈禮"
                ''')
gift_df["去除原因"] = ''
gift_df = gift_df.loc[gift_df.groupby("customItem42__c")["型錄禮品申請編號"].idxmin()] # 8.[去重]同一客戶關係聯絡人編號（保留型錄禮品申請編號較小值）

def apply_gift_removal_rules(gift_df, today_driver, case_map, sys_kicked):
    gift_df = gift_df.copy()
    gift_df["去除原因"] = gift_df["去除原因"].fillna("")

    dup_mask = gift_df["客戶關係連絡人"].isin(today_driver["客戶關係連絡人"])
    gift_df.loc[dup_mask, "去除原因"] = gift_df.loc[dup_mask, "去除原因"].apply(
        lambda x: "與當日司機名單重複" if x == "" else f"{x}，與當日司機名單重複")

    kicked_pairs = set(
        zip(sys_kicked["客戶關係連絡人"], sys_kicked["執行內容說明"]))
    def _append_kicked_reason(row):
        key = (row["客戶關係連絡人"], row["申請物品"])
        if key in kicked_pairs:
            reason = "同執行內容已經完成"
            return reason if row["去除原因"] == "" else f"{row['去除原因']}，{reason}"
        return row["去除原因"]
    gift_df["去除原因"] = gift_df.apply(_append_kicked_reason, axis=1)

    def _append_kd_reason(row):
        contact = row["客戶關係連絡人"]
        if contact in case_map:
            case_ids = case_map[contact]
            reason = f"已完成K大後交辦：{case_ids}"
            return reason if row["去除原因"] == "" else f"{row['去除原因']}，{reason}"
        return row["去除原因"]
    gift_df["去除原因"] = gift_df.apply(_append_kd_reason, axis=1)
    return gift_df


gift_df = apply_gift_removal_rules(gift_df, today_driver, case_map, sys_kicked)


gift_df = pd.merge(gift_df, user_df[['負責區域','id','姓名']].rename(columns={'id':'customItem10__c','負責區域':'區域1'}), on = '區域1', how= 'left')
gift_df = gift_df.sort_values('嘉義寄送日期', ascending=True, na_position='first')
kd.convert_to_date(gift_df, '嘉義寄送日期' )
gift_df["排序"] = 2

gift_df.to_excel(f'{project_file}/派樣名單_{today}.xlsx',index = False)

gift_df = gift_df[gift_df['去除原因'].isna() | (gift_df['去除原因'].str.strip() == "")]
gift_df['entityType'] = '2766438431723495'
gift_df['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')
gift_df['customItem3__c'] = ( "經營專案-派樣電訪-" + gift_df["嘉義寄送日期"].astype(str).replace("NaT", "").replace("nan", ""))
gift_df['customItem45__c'] = gift_df['申請物品']
gift_df["customItem124__c"] = f"經營業務{ datetime.now().strftime("%y-%m")}"
gift_df['customItem113__c'] = '2'
final_columns = [
    'entityType',  'customItem124__c',  'customItem113__c', 'customItem120__c', 'customItem11__c',
     'customItem3__c',  'customItem42__c', 'customItem10__c' ,'customItem45__c','排序' ,'區域1','客戶關係連絡人' ]
gift_test = gift_df[[col for col in final_columns if col in gift_df.columns]]
kd.賈維斯1號('6.派樣名單處理完成, 開始處理14天拒K')
time.sleep(10)

T14_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-14)).timestamp() * 1000)

經營總名單 = pd.read_excel(r"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\K大經營具體數據.xlsx", dtype='object')
經營總名單 = pd.merge(經營總名單.rename(columns={'資料區域群組名稱':'區域1'}), user_df[['負責區域','id','姓名']].rename(columns={'id':'customItem10__c','負責區域':'區域1'}), on = '區域1', how= 'left')
connected_one_year_all= kd.get_data_from_CRM (f'''
        select
        customItem48__c 客戶關係連絡人,customItem59__c 連絡人代號,customItem49__c 公司型態,customItem176__c 無效電拜訪類型,
        customItem177__c 無效電訪類型,customItem40__c 最近有效聯繫日期, customItem112__c,
        customItem128__c 觸客類型,customItem55__c 手機號碼,entityType,customItem207__c 講解分鐘數,createdBy
        from customEntity15__c
        where customItem40__c = {T14_days_ago} and customItem118__c like '%TW%'
        ''')

connected_one_year_all['唯一識別'] = ( connected_one_year_all['手機號碼'].str.strip().replace('', pd.NA) .combine_first(connected_one_year_all['連絡人代號']))
filtered_connected_success = connected_one_year_all[connected_one_year_all['customItem112__c'].astype(str).str.contains("拒絕")]
filtered_connected_success['14天前拒K'] = True
經營總名單 = pd.merge(經營總名單, filtered_connected_success[['客戶關係連絡人','14天前拒K']], on = '客戶關係連絡人', how = 'left' )
經營總名單[ (經營總名單['14天前拒K'] == 1) ]
sys_kicked['3月內已完成'] = True
經營總名單 = 經營總名單.rename(columns={'客戶關係連絡人':'客戶關係連絡人id',"name": "客戶關係連絡人"})


waiting_list2 = kd.apply_contact_reasons(經營總名單, sys_kicked, today_driver, gift_df)
waiting_list2 = waiting_list2[ (waiting_list2['14天前拒K'] == 1) ]

refused = waiting_list2[(waiting_list2['去除原因'].astype(str).str.strip() == "")]


output_path = fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\4_14天前拒K\經營名單_帶剔除標記_{today_str}.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    waiting_list2.to_excel(writer, index=False, sheet_name="全部名單_帶標記")
    refused.to_excel(writer, index=False, sheet_name="今日可觸達")


waiting_list2['entityType'] = '2766438431723495'
waiting_list2['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')

waiting_list2['customItem121__c'] = '1' 
waiting_list2['customItem115__c'] = [['1']]  * len(waiting_list2)
waiting_list2['customItem206__c'] = '9'
waiting_list2['customItem3__c'] = '經營專案-14天前拒K'

waiting_list2["customItem124__c"] = f"經營業務{ datetime.now().strftime("%y-%m")}"
waiting_list2['customItem113__c'] = '2'
waiting_list2['customItem118__c'] = waiting_list2['公司代號'] 
waiting_list2['customItem42__c'] = waiting_list2['客戶關係連絡人id'] 
waiting_list2["排序"] = 4
final_columns = [
    'entityType',  'customItem124__c',  'customItem113__c', 'customItem120__c', 'customItem11__c',
    'customItem3__c',  'customItem42__c', 'customItem10__c' ,'customItem45__c',"排序",'區域1' ,'客戶關係連絡人' ]
waiting_list2 = waiting_list2[[col for col in final_columns if col in waiting_list2.columns]]







kd.賈維斯1號('7.14天前拒K名單處理完成, 開始處理補名單')
time.sleep(10)


waiting_list = kd.get_data_from_MSSQL(f'''
SELECT
    id                               AS [customItem11__c],
    company_id                       AS [公司代號],
    region_group                     AS [區域1],
    company_shortname                AS [公司簡稱],
    sap_company_id                   AS [SAP公司代號],
    company_name                     AS [公司名稱],
    target_customer_type             AS [目標客戶類型],
    mian_id                          AS [customItem42__c],
    main_contact_state               AS [主要客關連關係狀態],
    main_contact_id                  AS [主要客關連],
    main_contact_id                  AS [客戶關係連絡人],
    common_tag                       AS [公司公用標籤],
    related_company                  AS [關聯公司],
    restricted_flag                  AS [管制],
    related_max_shipped_date_3y      AS [同關聯公司近3年最近發貨日期],
    is_main_related                  AS [主關聯],
    payment_type                     AS [客戶付款類型]

FROM
    [bi_ready].[dbo].[crm_tw_account_datail]
where 
    (common_tag != '間接客戶' or common_tag is null)
    and target_customer_type like '%經營%' 
    and region_group not like '%TW-Z%'
    and (do_not_disturb_contact not like '%勿電訪%' or do_not_disturb_contact is null)
    and (do_not_disturb not like '%勿電訪%' or do_not_disturb is null)
    and company_id = related_company   
    and (company_type like '%C%' or  company_type like '%D%')
    and payment_type  not like '%呆帳管制%'
order by related_max_shipped_date_3y 
''')

invalid_list = kd.get_data_from_MSSQL('SELECT company_id  FROM [clean_data].[dbo].[crm_account_invalid] ')
invalid_ids = (  invalid_list["company_id"] .dropna().astype(str) .unique())
waiting_list = waiting_list[~waiting_list["公司代號"].astype(str).isin(invalid_ids)]

waiting_list = pd.merge(waiting_list, user_df[['負責區域','id','姓名']].rename(columns={'id':'customItem10__c','負責區域':'區域1'}), on = '區域1', how= 'left')

df_removed, waiting_list, df_all = kd.process_waiting_list( waiting_list, sys_kicked,  six_months_ago, today_driver,gift_df,waiting_list2)
output_path = fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\3_系統補名單\經營名單_帶剔除標記_{today_str}.xlsx"
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    df_all.to_excel(writer, index=False, sheet_name="全部名單_帶標記")
    waiting_list.to_excel(writer, index=False, sheet_name="今日可觸達")


waiting_list['entityType'] = '2766438431723495'
waiting_list['customItem120__c'] = datetime.now().strftime('%Y-%m-%d')

waiting_list['customItem121__c'] = '1' 
waiting_list['customItem115__c'] = [['1']]  * len(waiting_list)
waiting_list['customItem206__c'] = '9'
waiting_list['customItem3__c'] = '經營專案-六個月以上未購'

waiting_list["customItem124__c"] = f"經營業務{ datetime.now().strftime("%y-%m")}"
waiting_list['customItem113__c'] = '2'
waiting_list['customItem118__c'] = waiting_list['公司代號'] 
waiting_list["customItem45__c"] = ( "最後一次購買日期為" +waiting_list["同關聯公司近3年最近發貨日期"].dt.strftime("%Y-%m-%d").fillna("無紀錄"))
waiting_list["排序"] = 3

final_columns = [
    'entityType',  'customItem124__c',  'customItem113__c', 'customItem120__c', 'customItem11__c',
    'customItem3__c',  'customItem42__c', 'customItem10__c' ,'customItem45__c',"排序",'區域1','客戶關係連絡人'  ]
waiting_test = waiting_list[[col for col in final_columns if col in waiting_list.columns]]






kd.賈維斯1號('8.系統補名單處理完成, 開始獲取K大後交辦, 並合併補足30筆')
time.sleep(10)
after_K = kd.get_data_from_CRM(
            f'''
        select id,name 交辦編號,customItem10__c
        from customEntity14__c
        where entityType in ('3082115462568332')   --業務類型=K大後電訪交辦 
        and  (customItem120__c < {today_end} )   --期望完成日期
        and  (customItem120__c >= {month_ago_three} )   --期望完成日期
        and customItem8__c = 1                     --執行狀態=等待回應
        -- and approvalStatus = 0                     --審批狀態=待提交
        and customItem3__c like '%K大後%'          -- 工作主旨=包含"K大後"
            ''') 




merged_df = pd.concat([driver_test, gift_test , waiting_test, waiting_list2], ignore_index=True)
merged_df = merged_df[merged_df["customItem42__c"].notna()]
count_df = merged_df.groupby("customItem10__c").size().reset_index(name="可觸達名單")
low_alarm_df = count_df[count_df["可觸達名單"] < low_alarm].reset_index(drop = True)
low_alarm_df = low_alarm_df.merge(user_df, left_on = "customItem10__c",right_on = 'id', how = "left")[['姓名','可觸達名單']].drop_duplicates('姓名').sort_values(by='可觸達名單', ascending=True).reset_index(drop = True)



cols = merged_df.columns.tolist()

after_K["customItem10__c"] = after_K["customItem10__c"].astype(str)
merged_df["customItem10__c"] = merged_df["customItem10__c"].astype(str)

final_list = []

all_executors = (
    set(after_K["customItem10__c"])
    .union(set(merged_df["customItem10__c"]))
)

for executor in all_executors:
    df_k = after_K[after_K["customItem10__c"] == executor].copy()
    k_count = len(df_k)
    if k_count >= DAILY_LIMIT:
        final_list.append(df_k.head(DAILY_LIMIT))
        continue
    need = DAILY_LIMIT - k_count

    df_m = (
        merged_df[merged_df["customItem10__c"] == executor]
        .sort_values("排序", ascending=True)
        .copy()
    )

    df_m = df_m.head(need)
    combined = pd.concat([df_k, df_m], ignore_index=True)
    final_list.append(combined)

final_df = pd.concat(final_list, ignore_index=True)


final_df = final_df.loc[final_df["id"].isnull(), [c for c in cols if c in final_df.columns]].drop(columns=["排序",'區域1','客戶關係連絡人'])

kd.賈維斯1號('9.系統補名單處理完成, 開始上傳')

test_MRK = final_df
ac_token = kd.get_access_token()
bulk_id = kd.ask_bulk_id() 
kd.insert_to_CRM(bulk_id, test_MRK)

time.sleep(10)

kd.賈維斯1號('10.上傳完成, 開始提交並留存數據')
xoql =  f'''
        select id,name 交辦編號,customItem10__c, dimDepart.departName 負責區域, customItem3__c 工作主旨,customItem8__c 執行狀態 ,approvalStatus
        ,customItem42__c.name 客戶關係連絡人編號,entityType,customItem142__c 連絡人代號,customItem42__c.id 客戶關係連絡人,
        customItem210__c 下交辦日期,customItem118__c 公司代號,customItem11__c.id customItem11__c
        from customEntity14__c
        where entityType in ('2766438431723495')   --業務類型= 一般交辦
        and  (customItem120__c = {today_begin} )   --期望完成日期>=D-30天
        and customItem8__c = 1                     --執行狀態=等待回應
        and createdBy = '3628254003531750'
        and ( customItem3__c like '%司機推廣%' or customItem3__c like '%經營專案-派樣電訪%' or 
        customItem3__c like '%經營專案-14天前拒K%' or customItem3__c like '%經營專案-六個月以上未購%' )
            '''

test_today = kd.get_data_from_CRM(xoql) 
kd.submit_df_to_crm_tw(test_today)

time.sleep(10)

test_save = kd.get_data_from_CRM(xoql) 
test_save = pd.merge(test_save, user_df[['負責區域','id','姓名']].rename(columns={'id':'customItem10__c_'}), on = '負責區域', how= 'left')
test_save = test_save.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)

test_save.to_excel(fr"Z:\18_各部門共享區\15_數據中心課\文斌\經營業務\5_派發與回收\{today_str}交辦留存.xlsx", index=False, sheet_name="今日可觸達")


kd.賈維斯1號('11.業1交辦完成, 撒花~')
