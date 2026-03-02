
import pandas as pd
from pathlib import Path
import sys
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from datetime import datetime, timedelta
import itertools
import numpy as np

start_ts_ms = pd.to_datetime("2025-01-01").timestamp() * 1000
start_date = datetime(2025, 1, 1)
end_date = datetime(2025, 12, 31)
company_holidays = [
    (datetime(2025, 1, 25), "Lunar New Year"),
    (datetime(2025, 1, 26), "Lunar New Year"),
    (datetime(2025, 1, 27), "Lunar New Year"),
    (datetime(2025, 1, 28), "Lunar New Year"),
    (datetime(2025, 1, 29), "Lunar New Year"),
    (datetime(2025, 1, 30), "Lunar New Year"),
    (datetime(2025, 1, 31), "Lunar New Year"),
    (datetime(2025, 2, 1),  "Lunar New Year"),
    (datetime(2025, 2, 2),  "Lunar New Year"),
    (datetime(2025, 2, 28), "228 Peace Memorial Day"),
    (datetime(2025, 3, 1),  "228 Peace Memorial Day"),
    (datetime(2025, 3, 2),  "228 Peace Memorial Day"),
    (datetime(2025, 4, 3), "Children's Day and Ching Ming Festival"),
    (datetime(2025, 4, 4), "Children's Day and Ching Ming Festival"),
    (datetime(2025, 4, 5), "Children's Day and Ching Ming Festival"),
    (datetime(2025, 4, 6), "Children's Day and Ching Ming Festival"),
    (datetime(2025, 5, 1), "Labor Day"),
    (datetime(2025, 5, 30), "Dragon Boat Festival"),
    (datetime(2025, 5, 31), "Dragon Boat Festival"),
    (datetime(2025, 6, 1),  "Dragon Boat Festival"),
    (datetime(2025, 9, 27), "Teacher's Day"),
    (datetime(2025, 9, 28), "Teacher's Day"),
    (datetime(2025, 9, 29), "Teacher's Day"),
    (datetime(2025, 10, 4), "Mid-Autumn Festival"),
    (datetime(2025, 10, 5), "Mid-Autumn Festival"),
    (datetime(2025, 10, 6), "Mid-Autumn Festival"),
    (datetime(2025, 10, 10), "National Day"),
    (datetime(2025, 10, 11), "National Day"),
    (datetime(2025, 10, 12), "National Day"),
    (datetime(2025, 10, 24), "Taiwan Retrocession Day"),
    (datetime(2025, 10, 25), "Taiwan Retrocession Day"),
    (datetime(2025, 10, 26), "Taiwan Retrocession Day"),
]
date_list = []
current_date = start_date
while current_date <= end_date:
    date_str = current_date.strftime("%Y-%m-%d")
    is_company_holiday = False
    holiday_name = ""

    for holiday_date, name in company_holidays:
        if current_date == holiday_date:
            is_company_holiday = True
            holiday_name = name
            break

    if current_date.weekday() >= 5 or is_company_holiday:
        day_type = 0  # 休息日
    else:
        day_type = 1  # 工作日

    date_list.append({
        "date": date_str,
        "day_type": day_type,
        "holiday": holiday_name})
    current_date += timedelta(days=1)

df_holiday = pd.DataFrame(date_list)
user_df = kd.get_data_from_CRM(f'''
    SELECT  name,  customItem28__c 離職日期, customItem17__c 部門,customItem7__c employeeid
    FROM customEntity30__c 
''')
user_df = user_df[user_df['部門'].astype(str).str.contains("加盟-業務課開發組", na=False)]
user = kd.get_data_from_CRM(f'''SELECT id,employeeCode employeeid FROM user ''')
user_df = pd.merge(user_df, user[['id', 'employeeid']], on='employeeid', how='left')
user_df = user_df.drop_duplicates(subset="id", keep="last")


kd.convert_to_date(user_df,'離職日期')
user_df["離職日期"] = pd.to_datetime(user_df["離職日期"], errors='coerce')
user_df["name"] = user_df.apply(
    lambda row: f"{row['name']} [離{row['離職日期'].strftime('%m/%d')}]"
    if pd.notnull(row["離職日期"]) else row["name"],axis=1)

employee_ids = user_df['employeeid'].dropna().unique().tolist()
if employee_ids:
    id_list_str = ",".join([f"'{eid}'" for eid in employee_ids])
    leave_df = kd.get_data_from_MSSQL(f'''
        SELECT 
              [employeeid],
              [employeename],
              [startdate],
              [realleavedays]
        FROM [raw_data].[dbo].[hrs_staff_leave]
        WHERE [employeeid] IN ({id_list_str}) AND [realleavedays] = '1.0'
    ''')
else:
    leave_df = pd.DataFrame(columns=['employeeid', 'employeename', 'startdate', 'realleavedays'])


user_list = user_df[['id', 'employeeid', 'name']].dropna().drop_duplicates()

leave_df['startdate'] = pd.to_datetime(leave_df['startdate'])
leave_df['leave_date'] = leave_df['startdate'].dt.strftime("%Y-%m-%d")
attendance_rows = []

for _, row in user_list.iterrows():
    emp_crm_id = row['id']
    emp_id = row['employeeid']
    emp_name = row['name']
    
    leave_dates = set(leave_df.loc[leave_df['employeeid'] == emp_id, 'leave_date'])

    for _, day in df_holiday.iterrows():
        date = day['date']
        is_working_day = day['day_type'] == 1

        day_type = 1 if is_working_day and date not in leave_dates else 0

        attendance_rows.append({
            '日期': date,
            'employeeid': emp_id,
            '執行人id': emp_crm_id,  # 加這行
            '人員': emp_name,
            'day_type': day_type})


df_attendance = pd.DataFrame(attendance_rows)

task =  kd.get_data_from_CRM (f'''
SELECT
  name 海外交辦管理編號,customItem9__c 公司代號,entityType 業務類型,customItem2__c 工作主旨,customItem1__c 執行人,customItem1__c 執行人id,
  customItem22__c 執行狀態,customItem38__c 電訪類型,customItem135__c 電訪結果,ownerId 所有人,createdBy 創建人,
  customItem132__c 名單來源,customItem150__c 名單來源細項,dimDepart 所屬部門,customItem36__c 建檔日期,customItem7__c 期望完成日期,
  customItem6__c 內容說明,createdAt 創建日期
FROM customEntity47__c
where customItem7__c >= {start_ts_ms} and  entityType = '3641251293040620'
  ''')
task = task[(task['執行人id'].isin(user_list['id'])) & (task['工作主旨'] == 'Invite 1-1 Meeting')]
task["業務類型"] = task["業務類型"].replace("3641251293040620", "加盟每日交辦")

user = kd.get_data_from_CRM(f'''SELECT id, name, local, dimDepart, customItem182__c 離職日期, employeeCode employeeid FROM user ''')
id_to_name = dict(zip(user["id"], user["name"]))
task["執行人"] = task["執行人"].map(id_to_name).fillna(task["執行人"])
task["創建人"] = task["創建人"].map(id_to_name).fillna(task["創建人"])
task["所有人"] = task["所有人"].map(id_to_name).fillna(task["所有人"])
task= kd.convert_to_date(task,'建檔日期')
task= kd.convert_to_date(task,'期望完成日期')
task= kd.convert_to_date(task,'創建日期')


task = task.applymap(str)
for col in ['執行狀態', "名單來源", "名單來源細項", "電訪結果", "電訪類型"]:
    task[col] = task[col].str.strip("[]'\" ").replace("nan", "")

match_df = pd.DataFrame({
    "撥打": ["接通，掛電話", "未接", "沒興趣", "1-1 K大", "邀約說明會(大場)", "下次聯絡 (加盟用)", "號碼無效"],
    "接通狀態": ["沒興趣", "1-1 K大", "邀約說明會(大場)", "下次聯絡 (加盟用)", None, None, None],
    "邀約狀態": ["1-1 K大", None, None, None, None, None, None]})
撥打清單 = set(match_df["撥打"].dropna())
接通清單 = set(match_df["接通狀態"].dropna())
邀約清單 = set(match_df["邀約狀態"].dropna())

task["撥打"] = task.apply(lambda row: 1 if row["電訪類型"] in 撥打清單 or row["電訪結果"] in 撥打清單 else 0, axis=1)
task["接通"] = task.apply(lambda row: 1 if row["電訪類型"] in 接通清單 or row["電訪結果"] in 接通清單 else 0, axis=1)
task["邀約"] = task.apply(lambda row: 1 if row["電訪結果"] in 邀約清單 else 0, axis=1)

task["執行人"] = task["執行人id"].map(user_df.set_index("id")["name"])

task_all = task.copy()

task_undo = task[( task['執行狀態'].astype(str).str.contains("等待回應", na=False))]

full_dates = df_attendance["日期"].unique()
full_people = df_attendance["執行人id"].unique()

date_pairs = list(itertools.product(full_dates, full_dates, full_people))  # (交辦日, 完成日, 員工)
matrix_base = pd.DataFrame(date_pairs, columns=["期望完成日期", "建檔日期", "執行人id"])

task_undo["創建日期"] = pd.to_datetime(task_undo["創建日期"]).dt.strftime("%Y-%m-%d")
task_undo["期望完成日期"] = pd.to_datetime(task_undo["期望完成日期"]).dt.strftime("%Y-%m-%d")
task_count = task_undo.groupby(["期望完成日期", "建檔日期", "執行人id"]).size().reset_index(name="任務數")
matrix_full = matrix_base.merge(task_count, on=["期望完成日期", "建檔日期", "執行人id"], how="left")
matrix_full["任務數"] = matrix_full["任務數"].fillna(0).astype(int)
task_count_all  = task_undo.groupby(["期望完成日期", "建檔日期"]).size().reset_index(name="任務數").assign(執行人id="ALL")   
full_people = df_attendance["執行人id"].unique().tolist()
if "ALL" not in full_people:
    full_people.append("ALL")

date_pairs = list(itertools.product(full_dates, full_dates, full_people))
matrix_base = pd.DataFrame(date_pairs,columns=["期望完成日期", "建檔日期", "執行人id"])

task_count_final = pd.concat([task_count, task_count_all], ignore_index=True)

matrix_full = (matrix_base.merge(task_count_final,on=["期望完成日期", "建檔日期", "執行人id"],how="left").fillna({"任務數": 0}))
matrix_full["任務數"] = matrix_full["任務數"].astype(int)

matrix_full = matrix_full.rename(columns={"期望完成日期": "創建日期","建檔日期": "完成日期"})
mask_all = matrix_full["執行人id"] == "ALL"
df_company  = matrix_full[mask_all].copy()
df_personal = matrix_full[~mask_all].copy()
df_company = (
    df_company
      .merge(df_holiday[["date", "day_type"]]
                 .rename(columns={"date": "創建日期", "day_type": "day_type_創建"}),
             on="創建日期", how="left")
      .merge(df_holiday[["date", "day_type"]]
                 .rename(columns={"date": "完成日期", "day_type": "day_type_完成"}),
             on="完成日期", how="left")
)
attc = df_attendance.rename(columns={"日期": "日期_tmp", "day_type": "day_type_tmp"})
df_personal = (
    df_personal
      .merge(attc[["執行人id", "日期_tmp", "day_type_tmp"]]
                 .rename(columns={"日期_tmp": "創建日期", "day_type_tmp": "day_type_創建"}),
             on=["執行人id", "創建日期"], how="left")
      .merge(attc[["執行人id", "日期_tmp", "day_type_tmp"]]
                 .rename(columns={"日期_tmp": "完成日期", "day_type_tmp": "day_type_完成"}),
             on=["執行人id", "完成日期"], how="left")
)
for col in ["day_type_創建", "day_type_完成"]:
    df_company[col]  = df_company[col].fillna(1).astype(int)
    df_personal[col] = df_personal[col].fillna(1).astype(int)
matrix_full = pd.concat([df_company, df_personal], ignore_index=True)
matrix_full["創建日_短"] = pd.to_datetime(matrix_full["創建日期"]).dt.strftime("%m/%d") + \
    matrix_full["day_type_創建"].apply(lambda x: " (休)" if x == 0 else "")

matrix_full["完成日_短"] = pd.to_datetime(matrix_full["完成日期"]).dt.strftime("%m/%d") + \
    matrix_full["day_type_完成"].apply(lambda x: " (休)" if x == 0 else "")
matrix_full["創建日期_dt"] = pd.to_datetime(matrix_full["創建日期"])
matrix_full["完成日期_dt"] = pd.to_datetime(matrix_full["完成日期"])

def get_week_range_string(date, offset_weeks=0):
    """
    回傳格式：yy年ww周 (mm/dd mm/dd)
    offset_weeks = -1 表示上週
    """
    if pd.isnull(date):
        return None

    adjusted_date = date + timedelta(weeks=offset_weeks)
    iso_year, iso_week, _ = adjusted_date.isocalendar()

    yy = str(iso_year)[-2:]  # 取年份後兩位
    monday = adjusted_date - timedelta(days=adjusted_date.weekday())
    sunday = monday + timedelta(days=6)

    return f"{yy}年{iso_week:02d}周 ({monday.strftime('%m/%d')} ~ {sunday.strftime('%m/%d')})"

matrix_full["創建週顯示"] = matrix_full["創建日期_dt"].apply(lambda x: get_week_range_string(x, 0))
matrix_full["完成週顯示"] = matrix_full["完成日期_dt"].apply(lambda x: get_week_range_string(x, 0))
today = datetime.today()
start_date = today - timedelta(weeks=20)
start_date = start_date - timedelta(days=start_date.weekday())  # 調整為週一
start_datetime = datetime.combine(start_date, datetime.min.time())  # 設為 00:00:00
end_date = today + timedelta(weeks=1)
end_date = end_date + timedelta(days=6 - end_date.weekday())  # 調整為週日
end_datetime = datetime.combine(end_date, datetime.max.time())  # 設為 23:59:59.999999

start_str = start_datetime.strftime("%Y-%m-%d")
end_str = end_datetime.strftime("%Y-%m-%d")
matrix_filtered = matrix_full[(matrix_full["創建日期"] >= start_str) & (matrix_full["創建日期"] <= end_str) &
                              (matrix_full["完成日期"] >= start_str) & (matrix_full["完成日期"] <= end_str)].copy()


matrix_filtered["創建日期_dt"] = pd.to_datetime(matrix_filtered["創建日期"])
matrix_filtered["完成日期_dt"] = pd.to_datetime(matrix_filtered["完成日期"])

matrix_filtered = matrix_filtered.rename(columns={
    '創建日期': 'create_date',
    '完成日期': 'complete_date',
    '執行人id': 'assignee_id',
    '任務數': 'task_count',
    'day_type_創建': 'day_type_create',
    'day_type_完成': 'day_type_complete',
    '創建日_短': 'create_date_short',
    '完成日_短': 'complete_date_short',
    '創建日期_dt': 'create_date_dt',
    '完成日期_dt': 'complete_date_dt',
    '創建週顯示': 'create_week_display',
    '完成週顯示': 'complete_week_display'})

matrix_filtered = pd.merge(matrix_filtered,user_df[['id','name']].rename(columns={'id': 'assignee_id', 'name': 'assignee_name'}),on='assignee_id',how='left')
matrix_filtered['assignee_name'] = np.where(matrix_filtered['assignee_id'] == 'ALL','ALL',matrix_filtered['assignee_name'])
matrix_filtered = matrix_filtered[~matrix_filtered["assignee_id"].isin(matrix_filtered.groupby("assignee_id")["task_count"].sum().loc[lambda s: s == 0].index)]


kd.write_to_sql(df=matrix_filtered,db_name='bi_ready',table_name='crm_tesk_os_trans_undo',if_exists='replace')

task = task[( task['執行狀態'].astype(str).str.contains("任務完成", na=False))]

full_dates = df_attendance["日期"].unique()
full_people = df_attendance["執行人id"].unique()

date_pairs = list(itertools.product(full_dates, full_dates, full_people))  # (交辦日, 完成日, 員工)
matrix_base = pd.DataFrame(date_pairs, columns=["期望完成日期", "建檔日期", "執行人id"])

task["創建日期"] = pd.to_datetime(task["創建日期"]).dt.strftime("%Y-%m-%d")
task["期望完成日期"] = pd.to_datetime(task["期望完成日期"]).dt.strftime("%Y-%m-%d")
task_count = task.groupby(["期望完成日期", "建檔日期", "執行人id"]).size().reset_index(name="任務數")
matrix_full = matrix_base.merge(task_count, on=["期望完成日期", "建檔日期", "執行人id"], how="left")
matrix_full["任務數"] = matrix_full["任務數"].fillna(0).astype(int)
task_count_all  = task.groupby(["期望完成日期", "建檔日期"]).size().reset_index(name="任務數").assign(執行人id="ALL")   
full_people = df_attendance["執行人id"].unique().tolist()
if "ALL" not in full_people:
    full_people.append("ALL")

date_pairs = list(itertools.product(full_dates, full_dates, full_people))
matrix_base = pd.DataFrame(date_pairs,columns=["期望完成日期", "建檔日期", "執行人id"])

task_count_final = pd.concat([task_count, task_count_all], ignore_index=True)

matrix_full = (matrix_base.merge(task_count_final, on=["期望完成日期", "建檔日期", "執行人id"],how="left").fillna({"任務數": 0}))
matrix_full["任務數"] = matrix_full["任務數"].astype(int)

matrix_full = matrix_full.rename(columns={"期望完成日期": "創建日期","建檔日期": "完成日期"})
mask_all = matrix_full["執行人id"] == "ALL"
df_company  = matrix_full[mask_all].copy()
df_personal = matrix_full[~mask_all].copy()
df_company = (
    df_company
      .merge(df_holiday[["date", "day_type"]]
                 .rename(columns={"date": "創建日期", "day_type": "day_type_創建"}),
             on="創建日期", how="left")
      .merge(df_holiday[["date", "day_type"]]
                 .rename(columns={"date": "完成日期", "day_type": "day_type_完成"}),
             on="完成日期", how="left"))
attc = df_attendance.rename(columns={"日期": "日期_tmp", "day_type": "day_type_tmp"})
df_personal = (
    df_personal
      .merge(attc[["執行人id", "日期_tmp", "day_type_tmp"]]
                 .rename(columns={"日期_tmp": "創建日期", "day_type_tmp": "day_type_創建"}),
             on=["執行人id", "創建日期"], how="left")
      .merge(attc[["執行人id", "日期_tmp", "day_type_tmp"]]
                 .rename(columns={"日期_tmp": "完成日期", "day_type_tmp": "day_type_完成"}),
             on=["執行人id", "完成日期"], how="left"))
for col in ["day_type_創建", "day_type_完成"]:
    df_company[col]  = df_company[col].fillna(1).astype(int)
    df_personal[col] = df_personal[col].fillna(1).astype(int)
matrix_full = pd.concat([df_company, df_personal], ignore_index=True)
matrix_full["創建日_短"] = pd.to_datetime(matrix_full["創建日期"]).dt.strftime("%m/%d") + \
    matrix_full["day_type_創建"].apply(lambda x: " (休)" if x == 0 else "")

matrix_full["完成日_短"] = pd.to_datetime(matrix_full["完成日期"]).dt.strftime("%m/%d") + \
    matrix_full["day_type_完成"].apply(lambda x: " (休)" if x == 0 else "")
matrix_full["創建日期_dt"] = pd.to_datetime(matrix_full["創建日期"])
matrix_full["完成日期_dt"] = pd.to_datetime(matrix_full["完成日期"])

def get_week_range_string(date, offset_weeks=0):
    """
    回傳格式：yy年ww周 (mm/dd mm/dd)
    offset_weeks = -1 表示上週
    """
    if pd.isnull(date):
        return None

    adjusted_date = date + timedelta(weeks=offset_weeks)
    iso_year, iso_week, _ = adjusted_date.isocalendar()

    yy = str(iso_year)[-2:]  # 取年份後兩位
    monday = adjusted_date - timedelta(days=adjusted_date.weekday())
    sunday = monday + timedelta(days=6)

    return f"{yy}年{iso_week:02d}周 ({monday.strftime('%m/%d')} ~ {sunday.strftime('%m/%d')})"

matrix_full["創建週顯示"] = matrix_full["創建日期_dt"].apply(lambda x: get_week_range_string(x, 0))
matrix_full["完成週顯示"] = matrix_full["完成日期_dt"].apply(lambda x: get_week_range_string(x, 0))
today = datetime.today()
start_date = today - timedelta(weeks=20)
start_date = start_date - timedelta(days=start_date.weekday())  # 調整為週一
start_datetime = datetime.combine(start_date, datetime.min.time())  # 設為 00:00:00
end_date = today + timedelta(weeks=1)
end_date = end_date + timedelta(days=6 - end_date.weekday())  # 調整為週日
end_datetime = datetime.combine(end_date, datetime.max.time())  # 設為 23:59:59.999999

start_str = start_datetime.strftime("%Y-%m-%d")
end_str = end_datetime.strftime("%Y-%m-%d")
matrix_filtered = matrix_full[(matrix_full["創建日期"] >= start_str) & (matrix_full["創建日期"] <= end_str) &
                              (matrix_full["完成日期"] >= start_str) & (matrix_full["完成日期"] <= end_str)].copy()


matrix_filtered["創建日期_dt"] = pd.to_datetime(matrix_filtered["創建日期"])
matrix_filtered["完成日期_dt"] = pd.to_datetime(matrix_filtered["完成日期"])
matrix_filtered["超時"] = 0
matrix_filtered["date_gap"] = None 
for idx, row in matrix_filtered.iterrows():
    start_date = row["創建日期_dt"]
    end_date = row["完成日期_dt"]
    if pd.isnull(start_date) or pd.isnull(end_date):
        continue
    if start_date > end_date:
        matrix_filtered.at[idx, "date_gap"] = -1
        continue
    if row["任務數"] <= 0:
        continue
    if row["執行人id"] == "ALL":
        mask = (df_holiday["date"] > start_date.strftime("%Y-%m-%d")) & \
               (df_holiday["date"] < end_date.strftime("%Y-%m-%d")) & \
               (df_holiday["day_type"] == 1)
        working_days = df_holiday[mask]
    else:
        personal_days = df_attendance[
            (df_attendance["執行人id"] == row["執行人id"]) &
            (df_attendance["日期"] > start_date.strftime("%Y-%m-%d")) &
            (df_attendance["日期"] < end_date.strftime("%Y-%m-%d")) &
            (df_attendance["day_type"] == 1)]
        working_days = personal_days

    matrix_filtered.at[idx, "date_gap"] = len(working_days)

    if len(working_days) >= 1:
        matrix_filtered.at[idx, "超時"] = 1


if "超時" not in matrix_filtered.columns:
    matrix_filtered["超時"] = 0
prev_week_df = matrix_filtered[["創建日期", "完成日期", "執行人id", "任務數", "超時"]].copy()
prev_week_df["創建日期"] = pd.to_datetime(prev_week_df["創建日期"]) + timedelta(days=7)
prev_week_df["完成日期"] = pd.to_datetime(prev_week_df["完成日期"]) + timedelta(days=7)
matrix_filtered["創建日期"] = pd.to_datetime(matrix_filtered["創建日期"])
matrix_filtered["完成日期"] = pd.to_datetime(matrix_filtered["完成日期"])

prev_week_df = prev_week_df.rename(columns={ "任務數": "上週任務數", "超時": "上週超時數"})
matrix_filtered = matrix_filtered.merge(prev_week_df,how="left",on=["創建日期", "完成日期", "執行人id"])
matrix_filtered["上週任務數"] = matrix_filtered["上週任務數"].fillna(0).astype(int)
matrix_filtered["上週超時數"] = matrix_filtered["上週超時數"].fillna(0).astype(int)


matrix_filtered = matrix_filtered.rename(columns={
    '創建日期': 'create_date',
    '完成日期': 'complete_date',
    '執行人id': 'assignee_id',
    '任務數': 'task_count',
    'day_type_創建': 'day_type_create',
    'day_type_完成': 'day_type_complete',
    '創建日_短': 'create_date_short',
    '完成日_短': 'complete_date_short',
    '創建日期_dt': 'create_date_dt',
    '完成日期_dt': 'complete_date_dt',
    '創建週顯示': 'create_week_display',
    '完成週顯示': 'complete_week_display',

    '超時': 'overdue',
    'date_gap': 'date_gap',
    '上週任務數': 'prev_week_task_count',
    '上週超時數': 'prev_week_overdue_count'
})

matrix_filtered = pd.merge(matrix_filtered,user_df[['id','name']].rename(columns={'id': 'assignee_id', 'name': 'assignee_name'}),on='assignee_id',how='left')
matrix_filtered['assignee_name'] = np.where(matrix_filtered['assignee_id'] == 'ALL','ALL',matrix_filtered['assignee_name'])
matrix_filtered["query_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

matrix_filtered = matrix_filtered[~matrix_filtered["assignee_id"].isin(matrix_filtered.groupby("assignee_id")["task_count"].sum().loc[lambda s: s == 0].index)]


task_all = task_all.rename(columns={
    '海外交辦管理編號': 'task_id',
    '公司代號': 'company_id',
    '業務類型': 'business_type',
    '工作主旨': 'task_subject',
    '執行人': 'executor_name',
    '執行人id': 'executor_id',
    '執行狀態': 'execution_status',
    '所有人': 'owner',
    '創建人': 'creator',
    '所屬部門': 'department',
    '建檔日期': 'record_date',
    '期望完成日期': 'expected_finish_date',
    '內容說明': 'description',
    '創建日期': 'create_date',
    '電訪結果': 'call_result',
    '名單來源': 'lead_source',
    '名單來源細項': 'lead_source_detail',
    '電訪類型': 'call_type',
    '撥打': 'called_flag',
    '接通': 'connected_flag',
    '邀約': 'invited_flag'})



kd.write_to_sql(df=matrix_filtered,db_name='bi_ready',table_name='crm_tesk_os_trans',if_exists='replace')

kd.write_to_sql(df=task_all,db_name='bi_ready',table_name='crm_tesk_os',if_exists='replace')
