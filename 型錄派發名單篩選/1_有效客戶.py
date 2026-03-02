
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta
from math import ceil

custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd



year_ago_1d = (datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date().strftime("%Y/%m/%d")
year_ago_3d = (datetime.today() - relativedelta(years=3) + relativedelta(days=1)).date().strftime("%Y/%m/%d")

new_company = kd.get_data_from_CRM(f'''
        select accountCode__c 公司代號, customItem202__c 公司地址,id ,  dimDepart.departName 資料區域群組名稱,customItem226__c 建檔日期,createdAt ,
        customItem197__c 公司簡稱, SAP_CompanyID__c SAP公司代號,accountName 公司名稱,customItem322__c 目標客戶類型,
        customItem326__c.name 主要客關連代號,customItem326__c.customItem24__c 主要客關連關係狀態,customItem326__c.customItem109__c 主要客關連勿擾選項,
        customItem199__c 公司型態, customItem291__c 公司勿擾選項,  Phone__c 公司電話, customItem278__c 倒閉,approvalStatus 審核狀態,
        customItem277__c 客戶付款類型,customItem311__c 公司公用標籤
        from account
        WHERE dimDepart.departName LIKE '%TW%'
''',account = "BI")

mian_id = kd.get_data_from_CRM(f'''select id mian_id , name 主要客關連代號 from customEntity22__c''',account = "BI")
new_company = pd.merge(new_company,mian_id,on ='主要客關連代號', how = 'left' )


DM_df = kd.get_data_from_CRM('''                   
        select customItem12__c  公司代號,customItem10__c 地址分類,nation__c 國家,customItem9__c 型錄地址
        from customEntity9__c
             ''',account = "BI")
DM_df['地址分類'] = DM_df['地址分類'].astype(str)
DM_df['國家'] = DM_df['國家'].astype(str)    
DM_df = DM_df.loc[DM_df['地址分類'].astype(str).str.contains("型錄",na = False)]
DM_df = DM_df.loc[DM_df['國家'].astype(str).str.contains("台灣",na = False)]
DM_df = DM_df[['公司代號','型錄地址']].drop_duplicates('公司代號')

new_company = pd.merge(new_company, DM_df ,on ='公司代號',how='left')

company_map = kd.get_data_from_MSSQL('''SELECT  company_id 公司代號  ,company_id_parent 關聯公司  FROM [raw_data].[dbo].[crm_related_company] ''')

total_company = pd.merge(new_company, company_map, on = '公司代號', how = 'left')
total_company['相同關聯公司'] = total_company['關聯公司'].notna() & total_company.duplicated(subset=['關聯公司'], keep=False)

total_company['公司地址重複'] = total_company['公司地址'].notna() & total_company.duplicated(subset=['公司地址'], keep=False)
total_company['型錄地址重複'] = total_company['型錄地址'].notna() & total_company.duplicated(subset=['型錄地址'], keep=False)
total_company['公司地址_len'] = total_company['公司地址'].fillna('').str.len()
total_company['型錄地址_len'] = total_company['型錄地址'].fillna('').str.len()
total_company['公司地址<6無號'] = (total_company['公司地址_len'] < 6) & ~total_company['公司地址'].astype(str).str.contains('號|号', na=False)
total_company['型錄地址<6無號'] = (total_company['型錄地址_len'] < 6) & ~total_company['型錄地址'].astype(str).str.contains('號|号', na=False)
total_company = total_company.drop(columns=['公司地址_len', '型錄地址_len'])


contact_person = kd.get_data_from_CRM(
            f'''
            select name 客戶關係連絡人代號, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 連絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人
            from customEntity22__c 
            where customItem37__c  like '%TW%'
            ''',account = "BI")
norm = lambda x: ''.join(x) if isinstance(x, list) else (str(x) if pd.notna(x) else "")

contact_person["is_left"]    = contact_person["關係狀態"].apply(lambda x: "離職" in norm(x))
contact_person["is_invalid"] = contact_person["連絡人資料無效"].apply(lambda x: "是" in norm(x))

contact_related = (contact_person.groupby("公司代號", as_index=False)
                   .agg(員工數=("客戶關係連絡人代號", "nunique"),
                        員工離職數=("is_left", "sum"),
                        員工無效數=("is_invalid", "sum")))

contact_related["聯絡人是否全為離職"] = contact_related["員工離職數"].eq(contact_related["員工數"])
contact_related["聯絡人是否全為無效"] = contact_related["員工無效數"].eq(contact_related["員工數"])

total_company = pd.merge(total_company, contact_related, on = '公司代號', how = 'left')



def add_invalid_info(df: pd.DataFrame, action: str = "keep") -> pd.DataFrame:
    invalid = kd.get_data_from_MSSQL("""
        SELECT company_id, 剔除原因 
        FROM clean_data.dbo.crm_account_invalid
    """)

    company_map = kd.get_data_from_MSSQL('''
                        SELECT  company_id 公司代號
                            ,sap_company_id SAP公司代號
                            ,company_id_parent 關聯公司
                        FROM [raw_data].[dbo].[crm_related_company]
    ''')
    invalid_ctrl = invalid[invalid["剔除原因"].astype(str).str.contains("(呆帳管制)", na=False)]
    invalid_ctrl = pd.merge(invalid_ctrl, company_map, left_on='company_id', right_on='公司代號', how='left')
    invalid_ids = pd.concat([
        invalid_ctrl['company_id'],
        invalid_ctrl['關聯公司']
    ], ignore_index=True).dropna().astype(str)
    df = df.copy()
    df['管制'] = df['公司代號'].astype(str).isin(invalid_ids)
    df['無效資料區域'] = df['公司代號'].astype(str).isin(
        invalid[invalid["剔除原因"].astype(str).str.contains("資料區域", na=False)]['company_id'].astype(str)
    )
    df['倒閉'] = df['公司代號'].astype(str).isin(
        invalid[invalid["剔除原因"].astype(str).str.contains("倒閉", na=False)]['company_id'].astype(str)
    )
    if action == "drop":
        df = df[df[['管制','無效資料區域','倒閉']].all(axis=1)].reset_index(drop=True)

    return df

total_company = add_invalid_info(total_company)



def mark_invalid_entries_catalog(K_invite: pd.DataFrame, data_type: str = 'company') -> pd.DataFrame:
    def need_company_relation(data_type: str) -> bool:
        return data_type in (None, "company", "stored_value")

    print(f"原始資料筆數: {len(K_invite)}")
    keyword_list = "搬遷|倒閉|歇業|停業|轉行|退休|過世|廢止|解散|燈箱|群組|支援|留守|教育訓練|無效|資料不全|非營業中|測試|兔兔"
    K_invite["是否包含關鍵字"] = (
        K_invite['公司簡稱'].astype(str).str.contains(keyword_list, na=False) |
        K_invite['公司名稱'].astype(str).str.contains(keyword_list, na=False)
    )
    K_invite["是否勿寄型錄"] = K_invite['公司勿擾選項'].astype(str).str.contains("型錄", na=False)
    K_invite["是否勿電訪"] = K_invite['公司勿擾選項'].astype(str).str.contains("電訪", na=False)
    K_invite["CD類"] = K_invite['公司型態'].astype(str).str.contains("C|D", na=False)

    return K_invite
total_company = mark_invalid_entries_catalog(total_company)






total_sales_one = kd.get_sap_with_relate_company(year_ago_1d,related_company = False)
target_summary_one = total_sales_one.groupby('SAP公司代號')['未稅本位幣'].sum().reset_index()
target_summary_one.rename(columns={'未稅本位幣': '近1年交易金額'}, inplace=True)
bins = [10_000, 50_000, 100_000, 300_000, 500_000, 1_000_000, float("inf")]
labels = ["1~5萬", "5~10萬", "10~30萬", "30~50萬", "50~100萬", "100萬以上"]
target_summary_one["金額階梯"] = pd.cut(target_summary_one["近1年交易金額"], bins=bins, labels=labels, right=False)


total_sales_three = kd.get_sap_with_relate_company(year_ago_3d,related_company = False)
target_summary_three = (  total_sales_three.groupby("SAP公司代號").agg( 近3年交易金額=("未稅本位幣", "sum"), 近3年最近發貨日期=("預計發貨日期", "max")).reset_index())

total_company = total_company.merge(target_summary_one,on='SAP公司代號', how='left')
total_company = total_company.merge(target_summary_three,on='SAP公司代號',how='left')


total_sales_one = kd.get_sap_with_relate_company(year_ago_1d)
target_summary_one = total_sales_one.groupby('公司代號')['未稅本位幣'].sum().reset_index()
target_summary_one.rename(columns={'未稅本位幣': '同關聯公司近1年交易金額', '公司代號': '關聯公司',}, inplace=True)
bins = [10_000, 50_000, 100_000, 300_000, 500_000, 1_000_000, float("inf")]
labels = ["1~5萬", "5~10萬", "10~30萬", "30~50萬", "50~100萬", "100萬以上"]
target_summary_one["同關聯公司金額階梯"] = pd.cut(target_summary_one["同關聯公司近1年交易金額"], bins=bins, labels=labels, right=False)


total_sales_three = kd.get_sap_with_relate_company(year_ago_3d)
target_summary_three = (  total_sales_three.groupby("公司代號").agg( 同關聯公司近3年交易金額=("未稅本位幣", "sum"), 同關聯公司近3年最近發貨日期=("預計發貨日期", "max")).reset_index())

target_summary_three.rename(columns={'未稅本位幣': '同關聯公司近3年交易金額', '公司代號': '關聯公司',}, inplace=True)

total_company = total_company.merge(target_summary_one,on='關聯公司', how='left')
total_company = total_company.merge(target_summary_three,on='關聯公司',how='left')


total_company['主關聯'] = ( (total_company['公司代號'].notna()) &
    (total_company['關聯公司'].notna()) & (total_company['公司代號'] == total_company['關聯公司']))
total_company['有聯絡人'] = (total_company['員工數'].notna() & (total_company['員工數'] > 0))
total_company = kd.convert_to_date(total_company,'建檔日期')
total_company = kd.convert_to_date(total_company,'createdAt')

col_map = {
    "公司代號": "company_id",
    "公司地址": "company_address",
    "id": "id",
    "資料區域群組名稱": "region_group",
    "建檔日期": "create_date",
    "createdAt": "createdAt",
    "公司簡稱": "company_shortname",
    "SAP公司代號": "sap_company_id",
    "公司名稱": "company_name",
    "目標客戶類型": "target_customer_type",
    "主要客關連代號": "main_contact_id",
    "主要客關連關係狀態": "main_contact_state",
    "公司型態": "company_type",
    "公司電話": "company_phone",
    "審核狀態": "approval_status",
    "公司公用標籤": "common_tag",
    "倒閉": "closed_flag",
    "公司勿擾選項": "do_not_disturb",
    "主要客關連勿擾選項": "do_not_disturb_contact",
    "型錄地址": "catalog_address",
    "關聯公司": "related_company",
    "相同關聯公司": "same_related_company",
    "公司地址重複": "dup_company_address",
    "型錄地址重複": "dup_catalog_address",
    "公司地址<6無號": "addr_short_nohao",
    "型錄地址<6無號": "catalog_addr_short_nohao",
    "員工數": "employee_count",
    "員工離職數": "employee_left_count",
    "員工無效數": "employee_invalid_count",
    "聯絡人是否全為離職": "all_contacts_left",
    "聯絡人是否全為無效": "all_contacts_invalid",
    "管制": "restricted_flag",
    "無效資料區域": "invalid_region_flag",
    "是否包含關鍵字": "keyword_flag",
    "是否勿寄型錄": "no_catalog_flag",
    "是否勿電訪": "no_call_flag",
    "CD類": "cd_type",
    "近1年交易金額": "amount_1y",
    "金額階梯": "amount_level_1y",
    "近3年交易金額": "amount_3y",
    "近3年最近發貨日期": "max_shipped_date_3y",
    "同關聯公司近1年交易金額": "related_amount_1y",
    "同關聯公司金額階梯": "related_amount_level_1y",
    "同關聯公司近3年交易金額": "related_amount_3y",
    "同關聯公司近3年最近發貨日期": "related_max_shipped_date_3y",
    "主關聯": "is_main_related",
    "有聯絡人": "has_employee",
    "客戶付款類型": "payment_type"
    
}


total_company_rename = total_company.rename(columns=col_map)
total_company_rename["current_date"] = datetime.today().strftime("%Y-%m-%d")


def prepare_df_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    def normalize(x):
        if isinstance(x, (list, tuple, set)):
            return ','.join(map(str, x))
        if pd.isna(x):
            return None
        if isinstance(x, str) and x.strip().lower() in ('nan', 'none', ''):
            return None
        return x
    return df.applymap(normalize)
df_to_write = prepare_df_for_sql(total_company_rename)
kd.write_to_sql(df=df_to_write,db_name='bi_ready',table_name='crm_tw_account_datail',if_exists='replace')


def screen_total_company(df: pd.DataFrame, main_flag_col: str = '主關聯'):
    def _to_bool(s: pd.Series) -> pd.Series:
        """寬鬆轉布林：支援 bool / 0-1 / 'True' 'False' / '是' '否' / 'Y' 'N' / None"""
        if s.dtype == bool:
            return s
        s2 = s.astype(str).str.strip().str.lower()
        true_set  = {'true','1','y','yes','是','t'}
        false_set = {'false','0','n','no','否','f','','none','nan'}
        out = s2.isin(true_set)
        out = out & ~s2.isin(false_set) | s2.isin(true_set)  # 保真，避免未知值影響
        return s2.isin(true_set)

    df_work = df.copy()
    summary = []

    def add_summary(step, start_label, removed_before, df_after, op_note):
        summary.append({
            '階段': step,
            '起始': start_label,
            '剔除': removed_before,
            '剩餘': len(df_after),
            '操作步驟': op_note
        })
    summary.append({'階段': 0, '起始': 'TW客戶', '剔除': '', '剩餘': len(df_work), '操作步驟': '所有數據'})

    step = 1
    col = '無效資料區域'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[~flag].copy()
        add_summary(step, '無效資料區', before - len(df_work), df_work, f'{col}=True 剔除')
    else:
        add_summary(step, '無效資料區', 0, df_work, f'{col} 欄位不存在，跳過')
    step += 1
    col = '倒閉'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[~flag].copy()
        add_summary(step, '標籤是否倒閉', before - len(df_work), df_work, f'{col}=True 剔除')
    else:
        add_summary(step, '標籤是否倒閉', 0, df_work, f'{col} 欄位不存在，跳過')
    step += 1
    col = main_flag_col  # 預設 '是否主關聯'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[flag].copy()
        add_summary(step, '保留主關聯', before - len(df_work), df_work, f'只保留 {col}=True')
    else:
        add_summary(step, '保留主關聯', 0, df_work, f'{col} 欄位不存在，跳過')
    step += 1
    col = '管制'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[~flag].copy()
        add_summary(step, 'SAP管制', before - len(df_work), df_work, f'{col}=True 剔除')
    else:
        add_summary(step, 'SAP管制', 0, df_work, f'{col} 欄位不存在，跳過')
    step += 1
    col = 'CD類'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[flag].copy()
        add_summary(step, '保留C/D類', before - len(df_work), df_work, f'{col}=True 保留')
    else:
        add_summary(step, '保留C/D類', 0, df_work, f'{col} 欄位不存在，跳過')
    step += 1
    col = '是否包含關鍵字'
    if col in df_work.columns:
        before = len(df_work)
        flag = _to_bool(df_work[col])
        df_work = df_work[~flag].copy()
        add_summary(step, '名/簡稱包含特殊字', before - len(df_work), df_work, f'{col}=True 剔除')
    else:
        add_summary(step, '名/簡稱包含特殊字', 0, df_work, f'{col} 欄位不存在，跳過')

    summary_df = pd.DataFrame(summary, columns=['階段', '起始', '剔除', '剩餘', '操作步驟'])
    return df_work.reset_index(drop=True), summary_df




df_out, summary = screen_total_company(total_company)
summary["current_date"] = datetime.today().strftime("%Y-%m-%d")

col_map_stage = {
    "階段": "stage",
    "起始": "phase_label",
    "剔除": "removed_count",
    "剩餘": "remaining_count",
    "操作步驟": "operation_desc",
    "current_date": "current_date",
}
summary_en = summary.rename(columns=col_map_stage)


kd.write_to_sql(
    df=summary_en,db_name='bi_ready',table_name='crm_tw_account_stage',
    if_exists='update', dedup_keys=['phase_label', 'current_date'], keep='new')

