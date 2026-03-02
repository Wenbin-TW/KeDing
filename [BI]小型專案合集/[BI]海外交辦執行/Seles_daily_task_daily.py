
import requests
import pandas as pd
import re
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from datetime import datetime,timedelta
import numpy as np
from sqlalchemy import create_engine, text
from pathlib import Path
import sys
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd

start_ts_ms_yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
end_ts_7_days = datetime.combine(datetime.now().date() + timedelta(days=7), datetime.min.time()).timestamp() * 1000
start_ts_ms = pd.to_datetime("2025-01-30").timestamp() * 1000
data_time=  datetime.now().strftime("%Y-%m-%d %H:%M")




def 判斷是否處理(row):
    type_str = str(row['電訪類型']).strip()
    result_str = str(row['電訪結果']).strip()
    if type_str not in ['nan', '', '[]'] or result_str not in ['nan', '', '[]']:
        return "已處理"
    else:
        return "未處理"

def 判斷是否觸達(row):
    type_str = str(row['電訪類型']).strip()
    result_str = str(row['電訪結果']).strip()
    
    if type_str not in ['nan', '', '[]'] or result_str not in ['nan', '', '[]']:
        return f"{type_str} / {result_str}"
    else:
        return "未處理"

def 清理觸達結果(s):
    s = str(s).strip()
    s = re.sub(r"[\[\]'\"\s]+", "", s)
    if s.lower() == "nan":
        return ""
    s = re.sub(r"[\/／,，\s]+", "", s)
    return s.strip()


def stringify_lists(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
    return df
g_retry_429_ids = set()

def query_single_auto_advance(entity_api_key: str, data_id: int, stage_flg: bool, token: str) -> dict:
    url = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/creekflow/history/filter"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "entityApiKey": entity_api_key,
        "dataId": data_id,
        "stageFlg": str(stage_flg).lower()}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        records = response.json().get("data", [])

        if not records:
            return {"dataId": data_id, "endAt": None}

        proc_ids = [r["procInstId"] for r in records if r.get("procInstId")]
        if not proc_ids:
            return {"dataId": data_id, "endAt": None}

        latest_proc_id = max(proc_ids)

        valid = [
            r for r in records
            if r.get("procInstId") == latest_proc_id
            and r.get("opinion") in ("System Auto Advance", "系统自动推进")
            and str(r.get("usertaskInstStatus")) == "2"
            and str(r.get("status")) == "6"
            and str(r.get("operateType")) in ("2", "2.0")
            and r.get("endAt") is not None
        ]

        if valid:
            latest = max(valid, key=lambda x: x["endAt"])
            return {"dataId": data_id, "endAt": latest["endAt"]}
        else:
            return {"dataId": data_id, "endAt": None}

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            global g_retry_429_ids
            g_retry_429_ids.add(data_id)
        return {"dataId": data_id, "endAt": None}
    except Exception:
        return {"dataId": data_id, "endAt": None}

def _run_parallel_batch(entity_api_key, ids_to_query, stage_flg, token, max_workers):
    batch_results = []
    global g_retry_429_ids
    g_retry_429_ids = set() 

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(query_single_auto_advance, entity_api_key, data_id, stage_flg, token): data_id
            for data_id in ids_to_query
        }

        for future in as_completed(futures):
            result = future.result()
            print(result) 
            if result:
                batch_results.append(result)
                
    return batch_results

def get_auto_advance_times_parallel(
    entity_api_key: str, 
    data_ids: list[int], 
    stage_flg: bool = False,
    fast_workers: int = 5,
    slow_workers: int = 2,
    max_429_retries: int = 3, 
    retry_delay: int = 1 
) -> pd.DataFrame:

    token = kd.get_access_token()
    all_results = []
    
    if not data_ids:
        print("警告：傳入的 data_ids 列表為空。")
        return pd.DataFrame()

    ids_to_process = set(data_ids)
    
    print(f"開始")
    start_time = time.perf_counter()
    print(f"\n快速查詢，使用 {fast_workers} 個線程...")
    phase1_results = _run_parallel_batch(entity_api_key, ids_to_process, stage_flg, token, fast_workers)
    all_results.extend(phase1_results)
    ids_for_429_retry = g_retry_429_ids
    for i in range(max_429_retries):
        if not ids_for_429_retry:
            break
        print(f"\n【429重試 第 {i+1}/{max_429_retries} 輪】發現 {len(ids_for_429_retry)} 筆429錯誤，等待 {retry_delay} 秒後重試...")
        time.sleep(retry_delay)
        retry_results = _run_parallel_batch(entity_api_key, ids_for_429_retry, stage_flg, token, slow_workers)
        all_results.extend(retry_results)
        ids_for_429_retry = g_retry_429_ids # 更新下一輪要重試的列表
    temp_df = pd.DataFrame(all_results)
    successful_ids = set(temp_df.dropna(subset=['endAt'])['dataId'].unique())
    failed_ids = ids_to_process - successful_ids

    if failed_ids:
        print(f"對 {len(failed_ids)} 筆查詢失敗或無結果的ID，進行慢速穩定重試（使用 {slow_workers} 線程）...")
        phase2_results = _run_parallel_batch(entity_api_key, failed_ids, stage_flg, token, slow_workers)
        all_results.extend(phase2_results)
    print("Done~~~~~~~")

    if not all_results:
        print("所有查詢均未返回有效數據。")
        return pd.DataFrame()

    final_df = pd.DataFrame(all_results)
    final_df.sort_values('endAt', na_position='first', inplace=True)
    final_df.drop_duplicates(subset=['dataId'], keep='last', inplace=True)
    final_df.dropna(subset=['endAt'], inplace=True)
    
    end_time = time.perf_counter()
    print(f"任務總耗時: {end_time - start_time:.2f} 秒")
    print(f"最終成功獲取 {len(final_df)} 筆有效數據。")
    
    return final_df
task = kd.get_data_from_CRM (f'''
SELECT
  id, name 海外交辦管理編號,customItem9__c 公司代號,entityType 業務類型,customItem2__c 工作主旨,customItem1__c 執行人,customItem1__c 執行人id,
  customItem22__c 執行狀態,customItem38__c 電訪類型,customItem135__c 電訪結果,ownerId 所有人,createdBy 創建人,customItem117__c.name K大課程版本,
  customItem132__c 名單來源,customItem150__c 名單來源細項,dimDepart 所屬部門,customItem36__c 建檔日期,customItem7__c 期望完成日期,
  customItem6__c 內容說明,createdAt 創建日期
FROM customEntity47__c
where customItem7__c >= {start_ts_ms} and customItem7__c < {end_ts_7_days} and   entityType in ('3641251293040620','3471049768147352')
  ''')

user = kd.get_data_from_CRM(f'''SELECT id, customItem181__c name, local, dimDepart, customItem182__c 離職日期, employeeCode employeeid FROM user ''')
id_to_name = dict(zip(user["id"], user["name"]))
task["執行人"] = task["執行人"].map(id_to_name).fillna(task["執行人"])

kd.convert_to_datetime(task,'建檔日期')
kd.convert_to_datetime(task,'期望完成日期')
kd.convert_to_datetime(task,'創建日期')


task['觸達結果'] = task.apply(判斷是否觸達, axis=1)
task['是否觸達'] = task['觸達結果'].apply(lambda x: 0 if any(keyword in str(x) for keyword in ['未接', '郵件聯繫', '未處理']) else 1)
task['觸達結果'] = task['觸達結果'].apply(清理觸達結果)

task['是否處理'] = task.apply(判斷是否處理, axis=1)
task['業務類型'] = task['業務類型'].map({'3641251293040620': '加盟每日交辦','3471049768147352': '加盟新客交辦'})


def normalize_whitespace(text):
    if pd.isna(text):
        return ''
    return re.sub(r'\s+', ' ', str(text).replace('\u3000', ' ')).strip()

task['工作主旨_clean'] = task['工作主旨'].apply(normalize_whitespace)
task['K大課程版本_clean'] = task['K大課程版本'].fillna('').apply(normalize_whitespace)
task.loc[ (task['工作主旨_clean'] == 'Invite Webinar') & (task['K大課程版本_clean'] == ''),'K大課程版本_clean'] = 'General'

conditions = [
    task['工作主旨_clean'] == 'Invite 1-1 Meeting',
    task['工作主旨_clean'] == 'Revised Proposal Pricing',
    (task['工作主旨_clean'] == 'Invite Webinar') & (task['K大課程版本_clean'] == 'General'),
    (task['工作主旨_clean'] == 'Invite Webinar') & (task['K大課程版本_clean'] == 'Industry-insider Distributor')]

choices = ['Invite 1-1 Meeting','Revised Proposal','G - Invite Webinar','IID - Invite Webinar']
task['任務類型_別名'] = np.select(conditions, choices, default='')
task['超時'] = (task['建檔日期'] > task['期望完成日期']).astype(int)


task['是否邀約'] = task['觸達結果'].apply(lambda x: 1 if any(keyword in str(x) for keyword in ['1/1 K大', '對降價有興趣', '邀約說明會(大場)', '邀約說明會(小場)']) else 0)
valid_subjects = ['Invite 1-1 Meeting', 'Invite Webinar', 'Revised Proposal Pricing']
task = task[task['工作主旨_clean'].isin(valid_subjects)]
task = task[task['任務類型_別名'].notna() & (task['任務類型_別名'].str.strip() != '')]
task = task[task['執行人'].notna() & (task['執行人'].str.strip() != '')]
column_mapping = {
    'id': 'id',
    '海外交辦管理編號': 'overseas_task_id',
    '公司代號': 'company_id',
    '業務類型': 'business_type',
    '工作主旨': 'task_subject',
    '執行人': 'assignee_name',
    '執行人id': 'assignee_id',
    '執行狀態': 'execution_status',
    '電訪結果': 'call_result',
    '所有人': 'owner_id',
    '創建人': 'created_by',
    'K大課程版本': 'k_course_version',
    '名單來源': 'source',
    '名單來源細項': 'source_detail',
    '所屬部門': 'department',
    '建檔日期': 'created_date',
    '期望完成日期': 'expected_due_date',
    '內容說明': 'content_note',
    '創建日期': 'created_datetime',
    '電訪類型': 'call_type',
    '觸達結果': 'touch_result',
    '是否觸達': 'is_touched',
    '是否處理': 'is_processed',
    '任務類型_別名': 'task_type_alias',
    '超時': 'is_overdue',
    '是否邀約': 'is_invited',
    'endAt': 'end_at',
    '提交時間': 'submit_hour',
    '提交日期': 'submit_date',
    '查詢時間': 'query_time'
}

old_history = kd.get_data_from_MSSQL('select id, end_at, submit_hour, submit_date, query_time from [bi_ready].[dbo].[crm_overseas_franchise_tasks]')
task = task.merge(old_history, on='id', how='left', suffixes=('', '_old'))
task = task.rename(columns=column_mapping)
task = stringify_lists(task)

kd.write_to_sql(df=task, db_name='bi_ready', table_name='crm_overseas_franchise_tasks', if_exists='replace') 

task_all = task.copy()
task = task[task['created_date'] >= start_ts_ms_yesterday].drop(columns=['end_at', 'submit_hour', 'submit_date', 'query_time'])
df_all_history = get_auto_advance_times_parallel("customEntity47__c", task['id'].tolist())
kd.convert_to_datetime(df_all_history,'endAt')
df_all_history['提交時間'] = pd.to_datetime(df_all_history['endAt'], errors='coerce').dt.strftime('%H')
df_all_history['提交日期'] = pd.to_datetime(df_all_history['endAt'], errors='coerce').dt.strftime('%Y-%m-%d')


merged_df = task.merge(df_all_history.rename(columns={'dataId': 'id'}), on='id', how='left')
merged_df['查詢時間'] = data_time
task_all = task_all.rename(columns=column_mapping)
merged_df = merged_df.rename(columns=column_mapping)
merged_df = stringify_lists(merged_df)

kd.write_to_sql(df=merged_df,db_name='bi_ready',table_name='crm_overseas_franchise_tasks',if_exists='update', dedup_keys=['id'], keep='new')



target_types = task_all['task_type_alias'].dropna().unique()
created = task_all[['created_date', 'assignee_name']].rename(columns={'created_date': 'date'})
expected = task_all[['expected_due_date', 'assignee_name']].rename(columns={'expected_due_date': 'date'})

task_all_date = task_all['created_date'].dropna().unique()
all_people = task_all['assignee_name'].dropna().unique()
extra_date_people = pd.MultiIndex.from_product([task_all_date, all_people], names=['date', 'assignee_name']).to_frame(index=False)
combined = pd.concat([created, expected, extra_date_people], ignore_index=True)
people_by_day = combined.dropna().drop_duplicates()
complete_index = people_by_day.merge(pd.DataFrame({'task_type_alias': target_types}), how='cross').rename(columns={'date': 'created_date'})



unprocessed = task_all[task_all['is_processed'] == '未處理'].copy()
unprocessed = unprocessed.merge(pd.DataFrame({'task_all_date': task_all_date}), how='cross')
unprocessed['is_overdue'] = (pd.to_datetime(unprocessed['expected_due_date'], errors='coerce') <
                              pd.to_datetime(unprocessed['task_all_date'], errors='coerce')).astype(int)
task_all_actual = (unprocessed[unprocessed['is_overdue'] == 1]
    .groupby(['task_all_date', 'assignee_name', 'task_type_alias']).size()
    .reset_index(name='overdue_count').rename(columns={'task_all_date': 'created_date'}))

task_all_final = (complete_index.merge(task_all_actual, on=['created_date', 'assignee_name', 'task_type_alias'], how='left')
         .fillna({'overdue_count': 0}))


total_test = kd.get_data_from_MSSQL('select * from [bi_ready].[dbo].[crm_overseas_franchise_tasks] where len(assignee_name)>0 and len(task_type_alias)>0')



hours_types = [f"{i:02d}" for i in range(24)] 
target_types = total_test['task_type_alias'].dropna().unique()
created = total_test[['created_date', 'assignee_name']].rename(columns={'created_date': 'date'})
expected = total_test[['expected_due_date', 'assignee_name']].rename(columns={'expected_due_date': 'date'})
combined = pd.concat([created, expected], ignore_index=True)
people_by_day = combined[['date', 'assignee_name']].dropna().drop_duplicates()
complete_index = people_by_day.merge(pd.DataFrame({'task_type_alias': target_types}),how='cross'  )
complete_hours = people_by_day.merge(pd.DataFrame({'task_type_alias': hours_types}),how='cross'  )
complete_index = pd.concat([complete_index, complete_hours], ignore_index=True)


assigned_task = (total_test .groupby(['expected_due_date', 'task_type_alias', 'assignee_name'])
                    .size().reset_index(name='task_count'))

overdue_task = (total_test[total_test['is_overdue'] == 1].groupby(['expected_due_date', 'task_type_alias', 'assignee_name'])
                    .size().reset_index(name='task_count'))


completed_task = (total_test[total_test['is_processed'] == '已處理'] .groupby(['created_date', 'task_type_alias', 'assignee_name'])
                    .size().reset_index(name='task_count'))


touched_task = (total_test[total_test['is_touched'] == 1] .groupby(['created_date', 'task_type_alias', 'assignee_name'])
                    .size().reset_index(name='task_count'))
invited_task = (total_test[total_test['is_invited'] == 1] .groupby(['created_date', 'task_type_alias', 'assignee_name'])
                    .size().reset_index(name='task_count'))

hourly_task = (total_test[total_test['submit_hour'].notna()].groupby(['created_date', 'submit_hour', 'assignee_name'])
                    .size().reset_index(name='task_count')
                    .rename(columns={'submit_hour': 'task_type_alias'}))


final = complete_index.copy()

def merge_with_full_index(df, value_col):
    return (final
            .merge(df, how='left', left_on=['date', 'task_type_alias', 'assignee_name'],
                   right_on=['created_date' if 'created_date' in df.columns else 'expected_due_date','task_type_alias', 'assignee_name'])
            .drop(columns=['created_date', 'expected_due_date'], errors='ignore')
            .rename(columns={'task_count': value_col})
            .fillna({value_col: 0}))
final = merge_with_full_index(assigned_task, 'assigned_count')
final = merge_with_full_index(task_all_final, 'overdue_count')
final = merge_with_full_index(completed_task, 'completed_count')
final = merge_with_full_index(touched_task, 'touched_count')
final = merge_with_full_index(invited_task, 'invited_count')
final = merge_with_full_index(hourly_task, 'hourly_count')

final['completion_rate'] = final.apply(
    lambda x: x['completed_count'] / (x['assigned_count'] + x['overdue_count']) 
    if (x['assigned_count'] + x['overdue_count']) != 0 else 0,axis=1)
final['touch_rate'] = final.apply(lambda x: x['touched_count'] / x['completed_count'] if x['completed_count'] != 0 else 0, axis=1)
final['invite_rate'] = final.apply(lambda x: x['invited_count'] / x['completed_count'] if x['completed_count'] != 0 else 0, axis=1)
final = stringify_lists(final)
final['query_time'] = data_time
kd.write_to_sql(df=final, db_name='bi_ready', table_name='crm_overseas_franchise_trans', if_exists='replace') 
tt = total_test.copy()
tt['created_date'] = pd.to_datetime(tt['created_date']).dt.normalize()
tt['expected_due_date'] = pd.to_datetime(tt['expected_due_date']).dt.normalize()
hours_types = [f"{i:02d}" for i in range(24)]
target_types = tt['task_type_alias'].dropna().unique()

created  = tt[['created_date', 'assignee_name']].rename(columns={'created_date': 'date'})
expected = tt[['expected_due_date', 'assignee_name']].rename(columns={'expected_due_date': 'date'})
combined = pd.concat([created, expected], ignore_index=True)
people_by_day = combined[['date', 'assignee_name']].dropna().drop_duplicates()
all_dates = sorted(people_by_day['date'].unique())

def build_task_board_for_date(d):
    day_people = people_by_day.loc[people_by_day['date'] == d, 'assignee_name'].unique()
    if len(day_people) == 0:
        return pd.DataFrame(columns=['date','assignee_name','task_type_alias','section','value'])
    base = pd.MultiIndex.from_product(
        [[d], day_people, target_types],
        names=['date','assignee_name','task_type_alias']
    ).to_frame(index=False)
    def full_count(mask, col_name):
        idx = pd.MultiIndex.from_product(
            [day_people, target_types],
            names=['assignee_name','task_type_alias']
        )
        s = tt.loc[mask].groupby(['assignee_name','task_type_alias']).size()
        s = s.reindex(idx, fill_value=0).rename(col_name).reset_index()
        return s
    assigned  = full_count(tt['expected_due_date'] == d, 'assigned')
    overdue   = full_count((tt['expected_due_date'] < d) & (tt['is_processed'] != '已處理'), 'overdue')
    completed = full_count((tt['is_processed'] == '已處理') & (tt['created_date'] == d), 'completed')
    connected = full_count((tt['is_touched']  == 1)       & (tt['created_date'] == d), 'connected')
    invited   = full_count((tt['is_invited']  == 1)       & (tt['created_date'] == d), 'invited')

    by_type = (base
               .merge(assigned,  on=['assignee_name','task_type_alias'], how='left')
               .merge(overdue,   on=['assignee_name','task_type_alias'], how='left')
               .merge(completed, on=['assignee_name','task_type_alias'], how='left')
               .merge(connected, on=['assignee_name','task_type_alias'], how='left')
               .merge(invited,   on=['assignee_name','task_type_alias'], how='left')
              ).fillna(0)

    by_type['total_task'] = by_type['assigned'] + by_type['overdue']

    parts = []
    for col, section_name in [
        ('assigned',   'Assigned Task'),
        ('overdue',    'Overdue Task'),
        ('total_task', 'Total Task'),
    ]:
        p = by_type[['date','assignee_name','task_type_alias', col]].copy()
        p['section'] = section_name
        p = p.rename(columns={col: 'value'})
        parts.append(p)

    p = by_type[['date','assignee_name','task_type_alias','completed']].copy()
    p['section'] = 'Completed Task'; p = p.rename(columns={'completed':'value'})
    parts.append(p)

    agg = by_type.groupby(['date','assignee_name'], as_index=False)[['completed','total_task']].sum()
    p = agg[['date','assignee_name']].copy()
    p['value'] = np.where(agg['total_task'] != 0, agg['completed'] / agg['total_task'], 0.0)
    p['task_type_alias'] = pd.NA
    p['section'] = 'Completed Rate'
    parts.append(p)

    p = by_type[['date','assignee_name','task_type_alias','connected']].copy()
    p['section'] = 'Connected Task'; p = p.rename(columns={'connected':'value'})
    parts.append(p)

    agg = by_type.groupby(['date','assignee_name'], as_index=False)[['connected','completed']].sum()
    p = agg[['date','assignee_name']].copy()
    p['value'] = np.where(agg['completed'] != 0, agg['connected'] / agg['completed'], 0.0)
    p['task_type_alias'] = pd.NA
    p['section'] = 'Connected Rate'
    parts.append(p)

    p = by_type[['date','assignee_name','task_type_alias','invited']].copy()
    p['section'] = 'Invitation Task'; p = p.rename(columns={'invited':'value'})
    parts.append(p)

    agg = by_type.groupby(['date','assignee_name'], as_index=False)[['invited','completed']].sum()
    p = agg[['date','assignee_name']].copy()
    p['value'] = np.where(agg['completed'] != 0, agg['invited'] / agg['completed'], 0.0)
    p['task_type_alias'] = pd.NA
    p['section'] = 'Invitation Rate'
    parts.append(p)

    return pd.concat(parts, ignore_index=True)
task_board_daily_long = pd.concat(
    [build_task_board_for_date(d) for d in all_dates],
    ignore_index=True
)
mask_rate = task_board_daily_long['section'].str.contains('Rate')
task_board_daily_long.loc[mask_rate, 'value'] = (task_board_daily_long.loc[mask_rate, 'value'] * 100).round(2)


kd.write_to_sql(df=task_board_daily_long, db_name='bi_ready', table_name='crm_overseas_franchise_long', if_exists='replace') 
