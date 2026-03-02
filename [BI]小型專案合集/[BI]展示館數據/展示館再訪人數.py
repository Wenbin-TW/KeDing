
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import pyperclip
import sys
from sqlalchemy import create_engine, text
from pathlib import Path
import sys
import numpy as np
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新")
sys.path.append(str(custom_path))
import common as kd


now = pd.Timestamp.now()
base_month_start = now - pd.DateOffset(months=7)
base_month_start = base_month_start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
base_month_end = (base_month_start + pd.DateOffset(months=1)) - pd.Timedelta(seconds=1)
followup_start = base_month_end + pd.Timedelta(seconds=1)
followup_end = followup_start + pd.DateOffset(months=6) - pd.Timedelta(seconds=1)
print(f"分析目標月份: 從 {base_month_start} 開始")
print(f"後續追蹤期間: 從 {followup_start} 到 {followup_end} (不含)")



sql_query = """
SELECT  [id]
      ,[exh_visit_record_name] 展示館參訪記錄名稱
      ,[exh_area] 展示館區域
      ,[visit_date] 預約參訪日期
      ,[start_time] 
	  , CASE 
  WHEN [start_time] IS NULL THEN NULL
  WHEN [start_time] ='' THEN NULL
  ELSE 
    RIGHT('00' + CAST((CAST([start_time] AS BIGINT) + 28800000) / 3600000 AS VARCHAR), 2) + ':' +
    RIGHT('00' + CAST(((CAST([start_time] AS BIGINT) + 28800000) % 3600000) / 60000 AS VARCHAR), 2)
END AS 開始接待時間

      ,[end_time] 
	 , CASE 
  WHEN [end_time] IS NULL THEN NULL
  WHEN [end_time] ='' THEN NULL
  ELSE 
    RIGHT('00' + CAST((CAST([end_time] AS BIGINT) + 28800000) / 3600000 AS VARCHAR), 2) + ':' +
    RIGHT('00' + CAST(((CAST([end_time] AS BIGINT) + 28800000) % 3600000) / 60000 AS VARCHAR), 2)
END AS 結束接待時間
      ,[num_of_visitors] 實際參訪人數
      ,[reception_minutes] 接待分鐘數
      ,[new_visitors_job_category] 參訪職務類別
      ,[reservation_channel] 預約管道
      ,[company_id] 公司代號
      ,[company_name] 公司名稱
      ,[company_type] 公司型態
      ,[created_by] 創建人
      ,[created_at] 創建日期
      ,[present_minutes] 講解分鐘數
      ,[updatedAT] 修改日期
      ,[receptionist1] 接待人員1
      ,[area_tag] 資料區域
      ,[last_update]
      ,[present_mrk] 是否講解K大
  FROM [raw_data].[dbo].[crm_exhibition_data]

"""
museum_data = kd.get_data_from_MSSQL(sql_query)
museum_data['預約參訪日期'] = pd.to_datetime(museum_data['預約參訪日期'])
museum_data['展示館區域'] = museum_data['展示館區域'].apply(lambda x: x[0] if isinstance(x, list) else x)
museum_data['展示館區域'] = museum_data['展示館區域'].replace('新北旗艦館', '新北旗艦')
museum_data['展示館區域'] = museum_data['展示館區域'].replace('新樹', '新北旗艦')
region_map = {
    '新北旗艦': '台灣','新竹': '台灣','台中': '台灣','台南': '台灣','高雄': '台灣','嘉義': '台灣',
    '展示馆': '大陸','无锡仓库': '大陸','上海': '大陸','深圳': '大陸','無錫': '大陸',
    '新加坡': '海外','馬來西亞': '海外','菲律賓': '海外','印尼': '海外','印度': '海外','泰國': '海外','越南': '海外','香港': '海外'
}
museum_data['區域分類'] = museum_data['展示館區域'].map(region_map)
museum_data = museum_data[museum_data['開始接待時間'].notnull() & museum_data['開始接待時間'] != '']
museum_data = museum_data[museum_data['公司代號'].notnull()]
museum_data = museum_data[museum_data['公司代號'].astype(str).str.strip() != '']
museum_data = museum_data[~museum_data['公司名稱'].astype(str).str.contains('未建檔|未建档|歷年參訪資料|历年参访资料')]


museum_twos = museum_data[ museum_data['資料區域'] == 'TWOS']
museum_cn = museum_data[ museum_data['資料區域'] == 'CN']
invalid_keywords = [
    "已搬", "支援", "支持", "餐休", "留守", "搬迁", "资料不全", "关闭", "施工方", "资料不详",
    "管制", "转行", "倒闭", "关门", "歇业", "闭店", "关店", "已关", "删", "无效", "失效",
    "无望", "停业", "停用", "解散", "撤销", "撤店", "撤离", "注销", "资料暂存", "资料不齐",
    "资料重复", "木工群", "设计师群", "设计群", "业主群", "施工群", "施工方群",
    "包工群",  "三明客户群", "工地群", "客户群", "项目经理群", "兔兔", "测试", "总称"]
museum_cn = museum_cn[~museum_cn['公司名稱'].astype(str).str.contains("|".join(invalid_keywords))]
museum_cn = museum_cn[~museum_cn['公司名稱'].astype(str).str.contains("业主群|业主甲方群")]
museum_cn = museum_cn[museum_cn['公司名稱'].astype(str) != "业主"]

museum_data = pd.concat([museum_twos, museum_cn], ignore_index=True)
museum_data['接待分鐘數'] = pd.to_numeric(museum_data['接待分鐘數'], errors='coerce')
museum_data = museum_data[museum_data['接待分鐘數'] > 10]
museum_data['預約參訪日期'] = pd.to_datetime(museum_data['預約參訪日期'])
museum_data['預約參訪日期_dt'] = museum_data['預約參訪日期']
base_month_df = museum_data[
    (museum_data['預約參訪日期_dt'] >= base_month_start) &
    (museum_data['預約參訪日期_dt'] <= base_month_end)]

followup_df = museum_data[
    (museum_data['預約參訪日期_dt'] >= followup_start) &
    (museum_data['預約參訪日期_dt'] <= followup_end)]
base_group = base_month_df[['區域分類', '公司代號']].dropna().drop_duplicates()
followup_companies = set(followup_df['公司代號'].dropna().unique())
regions = ['集團', '台灣', '大陸', '海外']

result_rows = []
total_unique = base_group['公司代號'].nunique()
total_returned = base_group[base_group['公司代號'].isin(followup_companies)]['公司代號'].nunique()

result_rows.append(['集團', '去重有ID之數量', total_unique if total_unique else '0'])
result_rows.append(['集團', '後6個月有再來', total_returned if total_returned else '0'])

for region in ['台灣', '大陸', '海外']:
    region_df = base_group[base_group['區域分類'] == region]

    unique_count = region_df['公司代號'].nunique()
    returned_count = region_df[region_df['公司代號'].isin(followup_companies)]['公司代號'].nunique()

    result_rows.append([region, '去重有ID之數量', unique_count if unique_count else '0'])
    result_rows.append([region, '後6個月有再來', returned_count if returned_count else '0'])

summary_df = pd.DataFrame(result_rows, columns=['區域分類', '統計項目', '數值'])
區域分類_to_copy = "\n".join(summary_df['數值'].astype(str).tolist())
pyperclip.copy(區域分類_to_copy)

print('數據處理完成~~~~請去目標文件35行進行粘貼即可')
museum_base_clean = base_month_df[['區域分類', '展示館區域', '公司代號']].dropna().drop_duplicates()
museum_followup_companies = set(followup_df['公司代號'].dropna().unique())
taiwan_areas = ['新北旗艦', '新竹', '台中', '嘉義', '台南', '高雄']
cn_areas = ['上海', '无锡仓库', '深圳']
oversea_areas = ['新加坡', '馬來西亞']  

display_rows = []
for area in taiwan_areas:
    area_df = museum_base_clean[
        (museum_base_clean['展示館區域'] == area) & (museum_base_clean['區域分類'] == '台灣')]
    uniq_count = area_df['公司代號'].nunique()
    return_count = area_df[area_df['公司代號'].isin(museum_followup_companies)]['公司代號'].nunique()
    display_rows.append([area, '去重有ID之數量', uniq_count if uniq_count else '0'])
    display_rows.append([area, '後6個月有再來', return_count if return_count else '0'])
display_summary_df = pd.DataFrame([row for row in display_rows if row[0] != '集團'], columns=['展示館區域', '統計項目', '數值'])
展館數據_to_copy = "\n".join(display_summary_df['數值'].astype(str).tolist())
pyperclip.copy(展館數據_to_copy)

print('數據處理完成~~~~請去目標文件54行進行粘貼即可')



display_rows = [['上海/無錫' if row[0] == '大陸' else row[0], row[1], row[2]] for row in display_rows]
group_uniq_count = museum_base_clean['公司代號'].nunique()
group_return_count = museum_base_clean[museum_base_clean['公司代號'].isin(museum_followup_companies)]['公司代號'].nunique()
display_rows.append(['集團', '去重有ID之數量', group_uniq_count if group_uniq_count else '0'])
display_rows.append(['集團', '後6個月有再來', group_return_count if group_return_count else '0'])
taiwan_uniq_count = museum_base_clean[museum_base_clean['區域分類'] == '台灣']['公司代號'].nunique()
taiwan_return_count = museum_base_clean[(museum_base_clean['區域分類'] == '台灣') & (museum_base_clean['公司代號'].isin(museum_followup_companies))]['公司代號'].nunique()
display_rows.append(['台灣', '去重有ID之數量', taiwan_uniq_count if taiwan_uniq_count else '0'])
display_rows.append(['台灣', '後6個月有再來', taiwan_return_count if taiwan_return_count else '0'])
china_uniq_count = museum_base_clean[museum_base_clean['區域分類'] == '大陸']['公司代號'].nunique()
china_return_count = museum_base_clean[(museum_base_clean['區域分類'] == '大陸') & (museum_base_clean['公司代號'].isin(museum_followup_companies))]['公司代號'].nunique()
display_rows.append(['大陸', '去重有ID之數量', china_uniq_count if china_uniq_count else '0'])
display_rows.append(['大陸', '後6個月有再來', china_return_count if china_return_count else '0'])
oversea_uniq_count = museum_base_clean[museum_base_clean['區域分類'] == '海外']['公司代號'].nunique()
oversea_return_count = museum_base_clean[(museum_base_clean['區域分類'] == '海外') &  (museum_base_clean['公司代號'].isin(museum_followup_companies))]['公司代號'].nunique()
display_rows.append(['海外', '去重有ID之數量', oversea_uniq_count if oversea_uniq_count else '0'])
display_rows.append(['海外', '後6個月有再來', oversea_return_count if oversea_return_count else '0'])


display_summary_df = pd.DataFrame(display_rows, columns=['展示館區域', '統計項目', '數值'])
display_summary_df = display_summary_df[display_summary_df['展示館區域'] != '']

mssql_engine = create_engine("mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/bi_ready?driver=ODBC+Driver+17+for+SQL+Server")
with mssql_engine.connect() as conn:
    existing_data = pd.read_sql("SELECT * FROM dbo.exhibition_revisit_data", conn)
existing_data.columns = ['Region', 'Exhibition_Area', 'Reservation_Month', 'Unique_Visit', 'Revisit']

display_summary_df['Region'] = display_summary_df['展示館區域'].apply(
    lambda x: '總計' if x in ['集團', '台灣', '大陸', '海外'] else (
        '台灣' if x in taiwan_areas else ('大陸' if x == '上海/無錫' else '海外')))

display_summary_df['Type'] = display_summary_df['統計項目'].map({
    '去重有ID之數量': '來訪去重',
    '後6個月有再來': '再訪去重'})
display_summary_df['Reservation_Month'] = base_month_start.strftime('%y-%m')
display_summary_df = display_summary_df[['Region', '展示館區域', 'Type', 'Reservation_Month', '數值']]
display_summary_df.columns = ['Region', 'Exhibition_Area', 'Type', 'Reservation_Month', 'Count']
display_summary_df['Count'] = pd.to_numeric(display_summary_df['Count'], errors='coerce').fillna(0)

visit_df = display_summary_df.pivot_table(index=['Region', 'Exhibition_Area', 'Reservation_Month'],columns='Type',values='Count',fill_value=0).reset_index()

if '來訪去重' not in visit_df.columns:
    visit_df['來訪去重'] = 0
if '再訪去重' not in visit_df.columns:
    visit_df['再訪去重'] = 0

visit_df.columns = ['Region', 'Exhibition_Area', 'Reservation_Month', 'Unique_Visit', 'Revisit']
merged_data = pd.concat([existing_data, visit_df], ignore_index=True)

merged_data.sort_values(by='Reservation_Month', ascending=False, inplace=True)
merged_data.drop_duplicates(subset=['Region', 'Exhibition_Area', 'Reservation_Month'], keep='first', inplace=True)
merged_data = merged_data.sort_values(by=['Region', 'Exhibition_Area', 'Reservation_Month']).reset_index(drop=True)

from sqlalchemy import create_engine, Column, String, Integer, MetaData, Table, text
from sqlalchemy.dialects.mssql import NVARCHAR

engine = create_engine("mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/bi_ready?driver=ODBC+Driver+17+for+SQL+Server")

metadata = MetaData()
exhibition_table = Table(
    'exhibition_revisit_data', metadata,
    Column('Region', NVARCHAR(50)),
    Column('Exhibition_Area', NVARCHAR(100)),
    Column('Reservation_Month', NVARCHAR(10)),
    Column('Unique_Visit', Integer),
    Column('Revisit', Integer))

with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS dbo.exhibition_revisit_data"))
    metadata.create_all(conn)  

    data_dicts = merged_data.to_dict(orient='records')
    conn.execute(exhibition_table.insert(), data_dicts)

print("數據已成功追加到資料庫: exhibition_revisit_data")




museum_data = kd.get_data_from_MSSQL(sql_query)
museum_data['展示館區域'] = museum_data['展示館區域'].apply(lambda x: x[0] if isinstance(x, list) else x)
museum_data['展示館區域'] = museum_data['展示館區域'].replace('新北旗艦館', '新北旗艦')
museum_data['展示館區域'] = museum_data['展示館區域'].replace('新樹', '新北旗艦')
now = datetime.now()
first_day_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
first_day_of_last_month = (first_day_of_this_month - pd.DateOffset(months=1))
museum_data['預約參訪日期'] = pd.to_datetime(museum_data['預約參訪日期'])
museum_data = museum_data[
    (museum_data['預約參訪日期'] >= first_day_of_last_month) & 
    (museum_data['預約參訪日期'] < first_day_of_this_month)]
museum_data = museum_data[
    (museum_data['預約參訪日期'].notnull()) & (museum_data['預約參訪日期'] != '') &
    (museum_data['開始接待時間'].notnull()) & (museum_data['開始接待時間'] != '') &
    ~museum_data['公司名稱'].str.contains('兔兔|測試|歷史|测试|历史', na=False)]
museum_data.loc[museum_data['展示館區域'].str.contains('展示馆', na=False), '展示館區域'] = '上海'
museum_data.loc[museum_data['展示館區域'].str.contains('无锡仓库', na=False), '展示館區域'] = '無錫'

museum_twos = museum_data[ museum_data['資料區域'] == 'TWOS']
museum_cn = museum_data[ museum_data['資料區域'] == 'CN']
final_columns = ['展示館參訪記錄名稱', '展示館區域', '預約參訪日期', '開始接待時間', '結束接待時間',
       '實際參訪人數', '接待分鐘數', '參訪職務類別', '預約管道','公司代號', '公司名稱','公司型態', '創建人', '創建日期', '接待人員1']
final_data = museum_data[final_columns]
def visit_type(row):
    if pd.isna(row['參訪職務類別']):
        return row['公司型態']
    if '業主' in str(row['參訪職務類別']):
        return str(row['公司型態']) + '+Z'
    else:
        return row['公司型態']
final_data.loc[:, '來客類型_F'] = final_data.apply(visit_type, axis=1)
final_data = final_data.rename(columns={'公司型態': '客戶類別分類'})




def classify(row):
    customer_type = str(row['客戶類別分類'])  # 確保是字串型別
    visitor_type_f = str(row['來客類型_F'])
    if any(c in customer_type for c in ['C', 'D']):    
        if '+Z' in visitor_type_f:
            return 'CD+Z'                              
        return 'CD'
    return 'Z'
final_data['來客類型_G'] = final_data.apply(classify, axis=1)

final_data['實際參訪人數'] = pd.to_numeric(final_data['實際參訪人數'], errors='coerce')
final_data['接待分鐘數'] = pd.to_numeric(final_data['接待分鐘數'], errors='coerce')
final_data['預約年月'] = pd.to_datetime(final_data['預約參訪日期']).dt.to_period('M').astype(str)

grouped = final_data.groupby(
    ['預約年月','展示館區域'#,'客戶類別分類', '來客類型_F'
     , '來客類型_G'], dropna=False
).agg(
    參訪人數=('實際參訪人數', 'sum')
).reset_index()
grouped['參訪人數'] = grouped['參訪人數'].fillna(0).astype(int)

summary = grouped.groupby('來客類型_G')['參訪人數'].sum().reset_index()
summary = summary.rename(columns={'參訪人數': '總參訪人數'})
summary = "\n".join(map(str, summary['總參訪人數'].tolist()))
pyperclip.copy(summary)

pivot_df = grouped.pivot_table(
    index=['預約年月', '展示館區域'],
    columns='來客類型_G',
    values='參訪人數',
    aggfunc='sum',
    fill_value=0
).reset_index()

pivot_df.columns.name = None
pivot_df = pivot_df[['預約年月', '展示館區域'] + sorted([col for col in pivot_df.columns if col not in ['預約年月', '展示館區域']])]


final_data.rename(columns={'短接待場次': '接待<=10分鐘場次','長接待場次': '接待>10分鐘場次'}, inplace=True)
combined_stats = final_data.groupby(['預約年月', '展示館區域'], dropna=False).agg(
        參訪人數=('實際參訪人數', 'sum'),
        短接待參訪人數=('實際參訪人數', lambda x: x[final_data['接待分鐘數'] <= 10].sum()),
        長接待參訪人數=('實際參訪人數', lambda x: x[final_data['接待分鐘數'] > 10].sum()),
        來客家數=('公司代號', 'nunique'),
        短接待來客家數=('公司代號', lambda x: x[final_data['接待分鐘數'] <= 10].nunique()),
        長接待來客家數=('公司代號', lambda x: x[final_data['接待分鐘數'] > 10].nunique()),
        場次=('展示館參訪記錄名稱', 'nunique'),
        短接待場次=('接待分鐘數', lambda x: (x <= 10).sum()),
        長接待場次=('接待分鐘數', lambda x: (x > 10).sum()),
        總接待時長=('接待分鐘數', 'sum'),
        短接待時長=('接待分鐘數', lambda x: x[x <= 10].sum()),
        長接待時長=('接待分鐘數', lambda x: x[x > 10].sum())
    ).fillna(0).astype({
    '參訪人數': 'int','總接待時長': 'int','短接待場次': 'int','長接待場次': 'int','短接待參訪人數': 'int','長接待參訪人數': 'int','短接待來客家數': 'int','長接待來客家數': 'int',
    '來客家數': 'int', '短接待時長': 'int','長接待時長': 'int'}).reset_index()
custom_order = [
    "新北旗艦", "新竹", "台中", "台南", "高雄", "嘉義",
    "上海", "無錫", "深圳",
    "新加坡", "馬來西亞", "香港", "菲律賓", "印尼", "越南", "印度", "泰國"]

combined_stats['展示館區域'] = pd.Categorical(combined_stats['展示館區域'],categories=custom_order,ordered=True)
combined_stats = combined_stats.sort_values(['預約年月', '展示館區域']).reset_index(drop=True)
data_ready = pivot_df.merge(combined_stats, on=['預約年月', '展示館區域'], how='left')
data_ready['區域分類'] = data_ready['展示館區域'].map(region_map)
open_data = kd.get_data_from_MSSQL('''
    SELECT 
    case when [exhibition_hall]='新樹' then '新北旗艦' else [exhibition_hall] end as 展示館區域
        ,[exh_date] 日期
        , case when [operation_status] like '不%' then 'No' else 'Yes' end 是否營業
    FROM [clean_data].[dbo].[crm_exhibition_opr_hour]
                ''')

open_data['區域分類'] = open_data['展示館區域'].map(region_map)
open_data['日期'] = pd.to_datetime(open_data['日期'])
open_data['月份'] = open_data['日期'].dt.to_period('M')

hall_exhibition_days = open_data[open_data['是否營業'] == 'Yes'].groupby(['月份', '展示館區域']).size().reset_index(name='展示館營業天數')
region_exhibition_days = open_data[open_data['是否營業'] == 'Yes'].groupby(['月份', '區域分類', '展示館區域']).size().reset_index(name='展示館營業天數')
region_max_exhibition = region_exhibition_days.loc[region_exhibition_days.groupby(['月份', '區域分類'])['展示館營業天數'].idxmax()][['月份','區域分類','展示館營業天數']]

group_total = region_max_exhibition.groupby('月份')['展示館營業天數'].max().reset_index()
group_total['區域分類'] = '集團'
final_result = pd.concat([region_max_exhibition, group_total], ignore_index=True)
final_result['區域分類'] = final_result['區域分類'] + '總計'
final_result.rename(columns={'區域分類': '展示館區域'}, inplace=True)

combined_result = pd.concat([final_result, hall_exhibition_days], ignore_index=True)
days = (hall_exhibition_days.rename(columns={'月份': '預約年月','展示館區域': '展示館區域','展示館營業天數': '營業天數'}))
days['預約年月'] = days['預約年月'].astype(str)
data = data_ready.merge(days, on=['預約年月', '展示館區域'], how='left')
data['日均人數']       = data['參訪人數'] / data['營業天數']
data['每組人數']       = data['參訪人數'] / data['場次']
data['每組接待時間']   = data['長接待時長'] / data['長接待場次']
data.replace([np.inf, -np.inf], np.nan, inplace=True)
data = data.round({'日均人數': 2, '每組人數': 2, '每組接待時間': 2})
sum_cols = data.select_dtypes(include='number').columns.tolist()
sum_cols.remove('營業天數')   # 營業日不做加總，另取最大值
region_total = (data.groupby(['預約年月', '區域分類'])[sum_cols].sum().reset_index())
region_days  = (data.groupby(['預約年月', '區域分類'])['營業天數'].max().reset_index())
region_total = region_total.merge(region_days,on=['預約年月', '區域分類'],how='left')
region_total['展示館區域'] = region_total['區域分類'] + '合計'
region_total['日均人數']       = region_total['參訪人數'] / region_total['營業天數']
region_total['每組人數']       = region_total['參訪人數'] / region_total['場次']
region_total['每組接待時間']   = region_total['長接待時長'] / region_total['長接待場次']
region_total = region_total.round({'日均人數': 2, '每組人數': 2, '每組接待時間': 2})
group_total = (data.groupby('預約年月')[sum_cols].sum().reset_index())
group_days  = data.groupby('預約年月')['營業天數'].max().reset_index()
group_total = group_total.merge(group_days, on='預約年月', how='left')
group_total['區域分類']   = '集團'
group_total['展示館區域'] = '集團合計'
group_total['日均人數']       = group_total['參訪人數'] / group_total['營業天數']
group_total['每組人數']       = group_total['參訪人數'] / group_total['場次']
group_total['每組接待時間']   = group_total['長接待時長'] / group_total['長接待場次']
group_total = group_total.round({'日均人數': 2, '每組人數': 2, '每組接待時間': 2})
final = pd.concat([data, region_total, group_total], ignore_index=True)
mask = final['展示館區域'].str.contains('合計', na=False)
final.loc[mask, '區域分類'] = final.loc[mask, '展示館區域']
ordered_cols = [
    '預約年月', '區域分類', '展示館區域','Z', 'CD', 'CD+Z',
    '參訪人數', '短接待參訪人數', '長接待參訪人數','來客家數', '短接待來客家數', '長接待來客家數',
    '場次', '短接待場次', '長接待場次','總接待時長', '短接待時長', '長接待時長',
    '營業天數', '日均人數', '每組人數', '每組接待時間']
final = final.reindex(columns=ordered_cols)
final.sort_values(['預約年月', '區域分類', '展示館區域'], inplace=True, ignore_index=True)


final_column_mapping = {
    '預約年月': 'Reservation_Month',
    '區域分類': 'Region',
    '展示館區域': 'Exhibition_Area',
    'Z': 'Z',
    'CD': 'CD',
    'CD+Z': 'CD_Z',
    '參訪人數': 'Visitors_Count',
    '短接待參訪人數': 'Short_Visit_Visitors',
    '長接待參訪人數': 'Long_Visit_Visitors',
    '來客家數': 'Unique_Visitors',
    '短接待來客家數': 'Short_Visit_Unique_Visitors',
    '長接待來客家數': 'Long_Visit_Unique_Visitors',
    '場次': 'Visit_Sessions',
    '短接待場次': 'Short_Visits',
    '長接待場次': 'Long_Visits',
    '總接待時長': 'Total_Visit_Duration',
    '短接待時長': 'Short_Visit_Duration',
    '長接待時長': 'Long_Visit_Duration',
    '營業天數': 'Operating_Days',
    '日均人數': 'Avg_Visitors_Per_Day',
    '每組人數': 'Visitors_Per_Session',
    '每組接待時間': 'Duration_Per_Session'}
final.rename(columns=final_column_mapping, inplace=True)



with mssql_engine.connect() as conn:
    existing_data = pd.read_sql("SELECT * FROM dbo.exhibition_visit_data", conn)

merged_data = pd.concat([existing_data, final], ignore_index=True)
merged_data.replace('Missing value', pd.NA, inplace=True)

for col in merged_data.columns:
    if col not in ['Reservation_Month', 'Region', 'Exhibition_Area']:
        merged_data[col] = pd.to_numeric(merged_data[col], errors='coerce')

merged_data = merged_data.fillna(0)

merged_data.sort_values(by=['Reservation_Month', 'Region', 'Exhibition_Area'],inplace=True)
merged_data.drop_duplicates(subset=['Reservation_Month', 'Region', 'Exhibition_Area'],keep='first',inplace=True)
merged_data.reset_index(drop=True, inplace=True)
merged_data['Exhibition_Area'] = merged_data['Exhibition_Area'].str.replace('合計', '', regex=False)
merged_data = merged_data.drop_duplicates()


with mssql_engine.begin() as conn:        
    conn.exec_driver_sql("DROP TABLE IF EXISTS dbo.exhibition_visit_data")
    merged_data.to_sql('exhibition_visit_data',con=conn,if_exists='replace',index=False,schema='dbo')

print("已完全重建 dbo.exhibition_visit_data")
area_order = ["新北旗艦", "新竹", "台中", "嘉義", "台南", "高雄", "上海", "新加坡", "印尼"]

def prepare_and_pad(df, col_name, pad_rows):
    temp_df = df.set_index('Exhibition_Area').reindex(area_order)[[col_name]].reset_index()
    temp_df[col_name] = temp_df[col_name].fillna('')
    temp_df = temp_df[[col_name]]
    empty_df = pd.DataFrame({col_name: [''] * pad_rows})
    temp_df = pd.concat([temp_df, empty_df], ignore_index=True)
    temp_df.columns = ['Value']
    return temp_df

visitors_df = prepare_and_pad(final, 'Long_Visit_Visitors', 2)
visits_df = prepare_and_pad(final, 'Long_Visits', 13)
days_df = prepare_and_pad(final, 'Operating_Days', 15)
duration_df = prepare_and_pad(final, 'Long_Visit_Duration', 13)

group_values = final[final['Exhibition_Area'] == '集團合計'][['CD', 'CD_Z', 'Z']].values.flatten()
group_df = pd.DataFrame({'Value': group_values})
result_df = pd.concat([visitors_df, visits_df, days_df, duration_df, group_df], ignore_index=True)
output_text = result_df.to_csv(sep='\t', header=False, index=False)

pyperclip.copy(output_text)
print("已複製到剪貼簿, 滾去72行直接貼上使用! ")

final = final[['Region','Exhibition_Area','Long_Visits']]
final_order = [
    "新北旗艦", "新竹", "台中", "嘉義", "台南", "高雄",
    "", "", "",   # 3 空白行
    "上海", "無錫", "深圳",
    "", "",       # 2 空白行
    "新加坡", "印尼", "其他"
]
area_data = final.set_index('Exhibition_Area')['Long_Visits'].to_dict()
overseas_total = final.loc[final['Exhibition_Area'] == '海外合計', 'Long_Visits'].values[0]
singapore = area_data.get('新加坡', 0)
indonesia = area_data.get('印尼', 0)
other = overseas_total - singapore - indonesia
output_list = []
for area in final_order:
    if area == "":
        output_list.append("")
    elif area == "其他":
        output_list.append(other)
    else:
        output_list.append(area_data.get(area, 0))
result_df = pd.DataFrame({'Long_Visits': output_list})
output_text = result_df.to_csv(sep='\t', header=False, index=False)
pyperclip.copy(output_text)
print("已複製到剪貼簿, 滾去第4行直接貼上使用! ")

