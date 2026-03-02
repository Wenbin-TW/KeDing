
from datetime import datetime
import pandas as pd
from pathlib import Path
import sys
import numpy as np
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd
import re
from datetime import datetime
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE


keywords1 = ['建商']
keywords2 = ['一般']

公司型態 = ['C', 'D', 'FA', 'FB',  'FD']
target = ['經營客戶','開發客戶','開發中','沉默客戶','無']
new_company = kd.get_data_from_MSSQL(f'''  
SELECT
    [current_date]  AS [當前日期],
    id AS [id],
    company_id AS [公司代號],
    related_company AS [關聯公司],
    sap_company_id AS [SAP公司代號],
    company_name AS [公司名稱],
    company_shortname AS [公司簡稱],
    region_group AS [資料區域群組名稱],
    company_type AS [公司型態],
    company_phone AS [公司電話],
    target_customer_type AS [目標客戶類型],
    create_date AS [建檔日期],
    company_address AS [公司地址],
    catalog_address AS [型錄地址],
    main_contact_id AS [主要客關連代號],
    do_not_disturb AS [公司勿擾選項],
    approval_status AS [審核狀態],

    is_main_related AS [主關聯],
    cd_type AS [CD類],
    invalid_region_flag AS [無效資料區域],
    closed_flag AS [倒閉],
    restricted_flag AS [管制],
    keyword_flag AS [是否包含關鍵字],
    same_related_company AS [相同關聯公司],
    addr_short_nohao AS [公司地址<6無號],
    catalog_addr_short_nohao AS [型錄地址<6無號],
    dup_company_address AS [公司地址重複],
    dup_catalog_address AS [型錄地址重複],
    has_employee AS [是否有聯絡人],
    all_contacts_left AS [聯絡人全為離職],
    all_contacts_invalid AS [聯絡人全為無效],
    no_catalog_flag AS [是否勿寄型錄],
    no_call_flag AS [是否勿電訪],

    employee_count AS [員工數],
    employee_left_count AS [員工離職數],
    employee_invalid_count AS [員工無效數],

    amount_1y AS [近1年交易金額],
    amount_level_1y AS [金額階梯],
    amount_3y AS [近3交易金額],
    related_amount_1y AS [同關聯公司近1年交易金額],
    related_amount_level_1y AS [同關聯公司金額階梯],
    related_amount_3y AS [同關聯公司近3年交易金額]

FROM 
    [bi_ready].[dbo].[crm_tw_account_datail];

 ''')


new_company['審核狀態_flag'] = new_company['審核狀態'].astype(str).str.contains('Approved', regex=True, na=False)
contact_main = kd.get_data_from_CRM(
            f'''
            select name 主要客關連代號, contactCode__c__c 主要連絡人代號, customItem2__c.contactName 主要連絡人,
            customItem8__c 公司代號,contactPhone__c__c 主要連絡人手機號碼, customItem109__c 連絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人
            from customEntity22__c 
            where customItem37__c  like '%TW%'
            ''')



def clean_phone_number(phone):
    if pd.isna(phone):
        return None
    phone = re.sub(r'[-\s()]+', '', str(phone))
    if phone.startswith('+886'):
        phone = phone[4:]
    if phone.startswith('009'):
        phone = '09' + phone[3:]
    if phone.startswith('9') and len(phone) == 9:
        phone = '0' + phone
    return phone if len(phone) == 10 else None

contact_main['主要連絡人手機號碼'] = contact_main['主要連絡人手機號碼'].apply(clean_phone_number)
invalid_conditions = {
    "配合": contact_main['關係狀態'].astype(str).str.contains("配合", na=False),
    "離職": contact_main['關係狀態'].astype(str).str.contains("離職", na=False),
    "勿電訪或型錄": contact_main['連絡人勿擾選項'].astype(str).str.contains("勿電訪|型錄", na=False),
    "連絡人過世/退休": contact_main['主要連絡人'].astype(str).str.contains("過世|退休|往生|離世|歿|逝世|去世", na=False),
    "空號": contact_main['空號'].fillna(0).astype(int) == 1,
    "停機": contact_main['停機'].fillna(0).astype(int) == 1,
    "號碼錯誤": contact_main['號碼錯誤非本人'].astype(str).isin(["['是']", "是"]),
    "手機格式錯誤": ~contact_main['主要連絡人手機號碼'].str.match("^09\d{8}$", na=False),
    "資料無效": ~contact_main['連絡人資料無效'].astype(str).str.contains("否", na=False)
}

contact_main['主要連絡人資料是否有效'] = '是'
contact_main['主要連絡人資料無效類別'] = np.nan
invalid_reasons = {index: set() for index in contact_main.index}

for reason, condition in invalid_conditions.items():
    contact_main.loc[condition, '主要連絡人資料是否有效'] = '否'
    for index in contact_main[condition].index:
        invalid_reasons[index].add(reason)

contact_main['主要連絡人資料無效類別'] = contact_main.index.map(lambda idx: ",".join(invalid_reasons[idx]) if invalid_reasons[idx] else np.nan)
contact_main_filter = contact_main[['公司代號','主要客關連代號','主要連絡人','主要連絡人代號','主要連絡人手機號碼','主要連絡人資料是否有效','主要連絡人資料無效類別']]
K_invite = pd.merge(new_company, contact_main_filter, on=['公司代號', '主要客關連代號'], how = 'left')
contact_related = kd.get_data_from_CRM(
            f'''
            select name 客戶關係連絡人代號, customItem2__c.contactName 連絡人, contactCode__c__c 連絡人代號, 
            customItem8__c 公司代號,contactPhone__c__c 手機號碼,
            id 客戶關係連絡人 ,customItem74__c LINEID,customItem95__c 職務類別, customItem109__c 連絡人勿擾選項, 
            customItem42__c 連絡人資料無效, customItem24__c 關係狀態 ,customItem50__c 空號,
            customItem51__c 停機,customItem52__c 號碼錯誤非本人
            from customEntity22__c 
            where customItem37__c  like '%TW%'
            ''')

contact_related = pd.merge(K_invite, contact_related, on = '公司代號', how = 'left')
mask_not_empty = (contact_related['空號'].fillna(0).astype(int) != 1)
mask_not_stopped = (contact_related['停機'].fillna(0).astype(int) != 1)
mask_correct_owner = (contact_related['號碼錯誤非本人'] != "['是']") & (contact_related['號碼錯誤非本人'] != "是")
contact_related = contact_related[mask_not_empty & mask_not_stopped & mask_correct_owner]

contact_related['手機號碼'] = contact_related['手機號碼'].apply(clean_phone_number)

contact_related['關係狀態'] = contact_related['關係狀態'].astype(str)
contact_related['職務類別'] = contact_related['職務類別'].astype(str)
contact_related['連絡人資料無效'] = contact_related['連絡人資料無效'].astype(str)
import pandas as pd, ast, re, numpy as np

def to_list(v):
    """穩定把多值字串轉成 list；遇到空值回傳 []"""
    if pd.isna(v) or v == '':
        return []
    if isinstance(v, list):
        return v
    try:
        parsed = ast.literal_eval(str(v))
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return re.split(r'[;,、\s]\s*', str(v).strip())

def str_true(x) -> bool:
    """把 1 / True / '是' / 'true' / 'y' 都視為 True"""
    if pd.isna(x):
        return False
    if isinstance(x, list):
        return any(str_true(i) for i in x)
    return str(x).strip().lower() in {'1', 'true', '是', 'y'}

def has_kw(lst, kw: str) -> bool:
    """list of str 只要任一元素『包含』 kw 就回 True"""
    return any(isinstance(s, str) and kw in s for s in lst)
contact_related = contact_related[~contact_related['連絡人資料無效'].astype(str).str.contains('是', na=False)]
contact_related['關係狀態'] = contact_related['關係狀態'].apply(to_list)
contact_related['職務類別'] = contact_related['職務類別'].apply(to_list)

job_rank_map = {'老闆': 1, '設計總監': 2, '設計師': 3, '設計助理': 4, '助理設計': 4}
def clean_title(title_list):
    """去掉職務名稱前面的數字和空白"""
    cleaned = [re.sub(r'^\d+\s*', '', title) for title in title_list]
    return cleaned
contact_related['職務類別'] = contact_related['職務類別'].apply(clean_title)
contact_related['job_rank'] = contact_related['職務類別'].apply(
    lambda lst: min((job_rank_map.get(i, 99) for i in lst), default=99))
def pick_contact(group: pd.DataFrame) -> pd.DataFrame:
    """對同一個公司代號的所有連絡人記錄，挑出『第一連絡人』"""
    if str_true(group['主要連絡人資料是否有效'].iloc[0]):
        main_phone = str(group['主要連絡人手機號碼'].iloc[0]).strip()
        if re.match("^09\d{8}$", main_phone):
            group['第一連絡人'] = group['主要連絡人'].iloc[0]
            group['第一連絡人手機號'] = main_phone
            return group
    primary_name = group['主要連絡人'].iloc[0]
    primary_id = group.get('主要連絡人代號', pd.NA).iloc[0]
    sub = group[~((group['連絡人'] == primary_name) | (group['連絡人代號'] == primary_id))].copy()

    if sub.empty:
        group['第一連絡人'] = np.nan
        group['第一連絡人手機號'] = np.nan
        return group
    def find_best_candidate(candidates):
        """根據職位順序與職務找最適合的人"""
        candidates = candidates[candidates['手機號碼'].apply(lambda x: bool(re.match("^09\d{8}$", str(x))))]

        if candidates.empty:
            return None

        candidates['rank'] = candidates['職務類別'].apply(
            lambda lst: min((job_rank_map.get(i, 99) for i in lst), default=99)
        )

        return candidates.loc[candidates['rank'].idxmin()] if not candidates.empty else None
    cand = sub[sub['關係狀態'].apply(has_kw, kw='主要公司')]
    chosen = find_best_candidate(cand)
    if chosen is None:
        cand = sub[sub['關係狀態'].apply(has_kw, kw='配合')]
        chosen = find_best_candidate(cand)
    if chosen is None:
        cand = sub[sub['關係狀態'].apply(has_kw, kw='在職')]
        chosen = find_best_candidate(cand)
    if chosen is None:
        cand = sub[sub['手機號碼'].apply(lambda x: bool(re.match(r'^09\d{8}$', str(x))))]
        chosen = cand.iloc[0] if not cand.empty else None
    if chosen is None:
        main_phone = str(group['主要連絡人手機號碼'].iloc[0]).strip()
        if re.match(r'^\d{10}$', main_phone):
            group['第一連絡人'] = group['主要連絡人'].iloc[0]
            group['第一連絡人手機號'] = main_phone
        else:
            group['第一連絡人'] = np.nan
            group['第一連絡人手機號'] = np.nan
    else:
        group['第一連絡人'] = chosen['連絡人']
        group['第一連絡人手機號'] = str(chosen['手機號碼']).strip()

    return group
contact_related1 = (contact_related
      .groupby('公司代號', group_keys=False, sort=False).apply(pick_contact)
      .drop(columns=['job_rank']).reset_index(drop=True))
valid_first = contact_related1.dropna(subset=['第一連絡人', '第一連絡人手機號'])
first_contact_detail = pd.merge(
    contact_related,
    valid_first[['公司代號', '第一連絡人', '第一連絡人手機號']],
    left_on=['公司代號', '連絡人', '手機號碼'],
    right_on=['公司代號', '第一連絡人', '第一連絡人手機號'],
    how='inner'
).drop_duplicates(subset='公司代號', keep='first')



first_contact_short = (
    first_contact_detail[
        ['公司代號','客戶關係連絡人代號','連絡人','連絡人代號','手機號碼','LINEID','職務類別','關係狀態','連絡人勿擾選項']
    ]
    .rename(columns={
        '客戶關係連絡人代號': '第一客關連',
        '連絡人': '第一連絡人',
        '連絡人代號': '第一連絡人代號',
        '手機號碼': '第一連絡人手機號碼',
        'LINEID': '第一連絡人LINEID',
        '職務類別': '第一連絡人職務類別',
        '關係狀態': '第一連絡人關係狀態',
        '連絡人勿擾選項': '第一連絡人勿擾選項'
    })
)
K_invite = pd.merge(K_invite, first_contact_short, on=['公司代號'], how = 'left')



timestamp = int(datetime(2025,1, 1).timestamp() * 1000)
tracking = kd.get_data_from_CRM(f'''
        select
        accountCode__c 公司代號,
        customItem48__c 客戶關係連絡人,
        customItem48__c.name 客戶關係連絡人代號,
        customItem177__c 無效電訪類型,
        customItem40__c 最近聯繫時間,
        customItem128__c 觸客類型,
        customItem55__c 手機號碼
        from customEntity15__c
        where customItem40__c >= '{timestamp}'
        and customItem118__c like '%TW%'
        ''')


tracking = tracking[tracking['無效電訪類型'].isna()]
tracking = tracking.sort_values(by='最近聯繫時間', ascending=False)
tracking = tracking.drop_duplicates(subset='客戶關係連絡人代號', keep='first')

tracking['最近聯繫時間'] = tracking['最近聯繫時間'].astype(float)
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].apply(lambda x: pd.to_datetime(x / 1000.0, unit='s', utc=True))
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].dt.tz_convert('Asia/Taipei')
tracking['最近聯繫時間'] = tracking['最近聯繫時間'].dt.strftime('%Y-%m-%d')
tracking = tracking[['客戶關係連絡人代號', '最近聯繫時間']]
tracking.columns = ['客戶關係連絡人代號', '最近聯繫時間']

contact_df = pd.merge(K_invite, tracking,left_on='第一客關連', right_on='客戶關係連絡人代號',  how='left').drop(columns=['客戶關係連絡人代號'])



contact_df['公司電話重複'] = contact_df['公司電話'].notna() & contact_df.duplicated(subset=['公司電話'], keep=False)
contact_df['第一連絡人電話重複'] = contact_df['第一連絡人手機號碼'].notna() & contact_df.duplicated(subset=['第一連絡人手機號碼'], keep=False)
contact_df['無電話號碼'] = (contact_df['公司電話'] == '') & (contact_df['第一連絡人手機號碼'] == '')
gift_df = kd.get_data_from_CRM(f'''
        select customItem48__c 公司代號,gift__c.name 物品名稱, qty__c 申請發放數量,createDate__c 建檔日期,customItem43__c 特別備註
        from customEntity28__c
        where  customItem49__c like 'TW%' and gift__c.name like '%型錄%' 
                ''')
if gift_df is None or gift_df.empty:
    contact_df = contact_df.merge(
        pd.DataFrame(columns=['公司代號','接收物品名稱','是否拿過超耐磨一般款','是否拿過超耐磨建案款']),
        on='公司代號', how='left'
    ).merge(
        pd.DataFrame(columns=['公司代號','退回物品與備註']),
        on='公司代號', how='left'
    )
else:
    gift_df['申請發放數量'] = pd.to_numeric(gift_df['申請發放數量'], errors='coerce')

    def parse_mixed_ts(col: pd.Series) -> pd.Series:
        s_num = pd.to_numeric(col, errors='coerce')
        dt = pd.Series(pd.NaT, index=col.index, dtype="datetime64[ns, UTC]")
        m_ms = s_num.between(10**12, 10**14, inclusive="left")
        dt.loc[m_ms] = pd.to_datetime(s_num.loc[m_ms], unit='ms', utc=True)
        m_s = s_num.between(10**9, 10**11, inclusive="left")
        dt.loc[m_s] = pd.to_datetime(s_num.loc[m_s], unit='s', utc=True)
        m_ns = s_num >= 10**14
        dt.loc[m_ns] = pd.to_datetime(s_num.loc[m_ns], unit='ns', utc=True)
        m_rest = dt.isna()
        if m_rest.any():
            dt.loc[m_rest] = pd.to_datetime(col.loc[m_rest], errors='coerce', utc=True)
        try:
            return dt.tz_convert('Asia/Taipei').tz_localize(None)
        except Exception:
            try:
                return dt.tz_localize(None)
            except Exception:
                return dt

    gift_df['建檔日期_dt'] = parse_mixed_ts(gift_df['建檔日期'])
    gift_df = gift_df.reset_index(drop=True)
    gift_df['_row'] = gift_df.index.astype(int)
    gift_df = gift_df.sort_values(
        ['公司代號', '建檔日期_dt', '_row'],
        ascending=[True, False, False],
        kind='mergesort'
    )
    received_tmp = gift_df[gift_df['申請發放數量'] > 0].copy()
    conds = [
        received_tmp['物品名稱'].str.contains('超耐磨木地板型錄', na=False) & received_tmp['物品名稱'].str.contains('設計師', na=False),
        received_tmp['物品名稱'].str.contains('超耐磨木地板型錄', na=False) & received_tmp['物品名稱'].str.contains('建案',   na=False),
    ]
    choices = ['超耐磨一般款', '超耐磨建案款']
    received_tmp['超耐磨分類'] = np.select(conds, choices, default=None)
    def concat_new_to_old(g: pd.DataFrame, col: str) -> str:
        g2 = g.sort_values(['建檔日期_dt', '_row'], ascending=[False, False], kind='mergesort')
        return ', '.join(g2[col].astype(str).tolist())

    received_gifts_per_company = (
        received_tmp
        .groupby('公司代號', sort=False, group_keys=False)
        .apply(lambda g: pd.Series({
            '接收物品名稱': concat_new_to_old(g, '物品名稱'),
            '是否拿過超耐磨一般款': (g['超耐磨分類'] == '超耐磨一般款').any(),
            '是否拿過超耐磨建案款': (g['超耐磨分類'] == '超耐磨建案款').any(),
        }))
        .reset_index()
    )
    gift_df['退回物品+備註'] = (
        gift_df['物品名稱'].astype(str) +
        gift_df['特別備註'].apply(lambda x: f"【{str(x).strip()}】" if pd.notna(x) and str(x).strip() != "" else "")
    )
    returned_tmp = gift_df[gift_df['申請發放數量'] < 0].copy()

    returned_gifts_per_company = (
        returned_tmp
        .groupby('公司代號', sort=False, group_keys=False)
        .apply(lambda g: pd.Series({
            '退回物品與備註': concat_new_to_old(g, '退回物品+備註')
        }))
        .reset_index()
    )
    contact_df = contact_df.merge(received_gifts_per_company, on='公司代號', how='left')
    contact_df = contact_df.merge(returned_gifts_per_company, on='公司代號', how='left')



product_keywords = {
    "是否批批異_2506": "台灣批批異材質型錄-252款-2506-APT08TW",
    "是否批批木_2506": "台灣批批木紋型錄-252款-2506-APT07TW",
    "是否塗裝_2407": "台灣塗裝型錄180款-2407-AKT04TW",
    "是否塗裝_2509": "台灣塗裝型錄180款-2509-AKT04TW",
    "是否超耐磨木6設計師_2411": "超耐磨木地板型錄-6款-設計師版-2411- AFT06TWDN",
    "是否超耐磨木6建案_2411": "超耐磨木地板型錄-6款-建案專用版-2411- AFT06TWCP",
    "是否超耐磨木24設計師_2506": "超耐磨木地板型錄-24款-設計師版-2506- AFT06TWDN",
    "是否超耐磨木24建案_2506": "超耐磨木地板型錄-24款-建案專用版-2506- AFT06TWCP",
    "是否環保木_2309": "環保木地板型錄24款-2309-AFT05TW",
    "是否環保木_2407": "環保木地板型錄24款-2406或2407-AFT05TW",
    "是否塑合板_2506": "科定塑合板型錄-36款-2506-AST01TW"
}


for col_name, keyword in product_keywords.items():
    gift_df[col_name] = gift_df["物品名稱"].str.contains(keyword, na=False)
    temp_df = (
        gift_df[gift_df[col_name] & (gift_df["申請發放數量"] > 0)]
        .groupby("公司代號")
        .size()
        .reset_index(name=col_name)
    )
    temp_df[col_name] = True
    contact_df = contact_df.merge(temp_df[["公司代號", col_name]],
                                  on="公司代號",
                                  how="left")

    contact_df[col_name] = contact_df[col_name].fillna(False)




contact_df_copy = contact_df.copy()


contact_df = contact_df_copy.copy()
df2 = kd.get_data_from_MSSQL('''
            select distinct buyer SAP公司代號,material  from [raw_data].[dbo].[sap_sales_data]
            where material like 'M%' or material like 'F%' or material like 'K3%' 
        ''')
df2 =  kd.add_relate_company(df2,"SAP")
df2['是否交易環保木'] = df2['material'].str.startswith('F')
df2['是否交易超耐磨'] = df2['material'].str.startswith('M')
df2['是否交易手刮'] = df2['material'].str.startswith('K3')
traded_flags = (
    df2.groupby('SAP公司代號')
    .agg({
        '是否交易環保木': 'any',
        '是否交易超耐磨': 'any',
        '是否交易手刮': 'any'
    })
    .reset_index()
)
contact_df = contact_df.merge(traded_flags, on='SAP公司代號', how='left')
contact_df[['是否交易環保木', '是否交易超耐磨', '是否交易手刮']] = contact_df[['是否交易環保木', '是否交易超耐磨', '是否交易手刮']].fillna(False)





year_24_later = pd.to_datetime("2024-01-01").timestamp() * 1000
MRK_multiple = kd.get_data_from_CRM (f'''
        select customItem2__c.name 客戶關係連絡人,customItem7__c 公司代號, customItem2__c.contactCode__c__c 連絡人代號,customItem2__c.contactPhone__c__c 手機號碼,
        customItem8__c 是否上線,customItem31__c 預約日期
        from customEntity24__c
        where customItem1__c >= {year_24_later}
        ''')
MRK_multiple_filtered = MRK_multiple[MRK_multiple['是否上線'].apply(lambda x: isinstance(x, (list, str)) and '是' in x) ][['公司代號']]
MRK_original = kd.get_data_from_CRM (f'''
        select customItem6__c.name name,customItem4__c 公司代號, customItem2__c 最近上線K大上線日期,customItem9__c 手機號碼,
        customItem30__c 是否上線,customItem19__c 連絡人代號
        from customEntity23__c 
        where customItem2__c >= {year_24_later}
        ''')
MRK_filtered = MRK_original[MRK_original['是否上線'].apply(lambda x: isinstance(x, (list, str)) and '是' in x)][['公司代號']]
MRK_df = pd.concat([MRK_multiple_filtered, MRK_filtered], ignore_index=True).drop_duplicates()

MRK_df = kd.add_relate_company(MRK_df).drop_duplicates('公司代號')


contact_df['MRK24_25'] = contact_df['公司代號'].isin(MRK_df['公司代號'])





# contact_safe2 = pd.merge(contact_safe2, sap1 ,left_on ='SAP代號',right_on ='買方',how='inner')
# contact_safe3 = contact_safe1[~contact_safe1['公司代號'].isin(contact_safe2['公司代號'])]
# contact_safe1[contact_safe1['主要連絡人手機號碼']=='0937450299']

# '''
# from 銷貨
# '''
# connection = pymysql.connect(
#         host='192.168.1.253',  # 数据库地址
#         port=3307,
#         user='DATeam',          # 用户名
#         password='Dateam@1234', # 密码
#         database='db01',        # 数据库名称
#         charset='utf8'       # 字符编码
#     )
# cursor = connection.cursor()

# cursor.execute("""SELECT 買方,未稅本位幣, 物料, 物料群組  FROM sap_sales_data 
#                 where 買方 like 'TW%' and 預計發貨日期 >='2024/01/22' 
#                 """)

# results = cursor.fetchall()
# sap = pd.DataFrame(results)
# sap.columns = ["SAP代號", "未稅本位幣","物料","物料群組" ]
# #sap1 = sap.drop_duplicates(subset=['買方'])
# sap1 = sap.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '近1年交易金額')
# contact_safe3  = pd.merge(contact_safe2, sap1 ,on ='SAP代號',how='left')

# cursor.execute("""SELECT 買方,未稅本位幣, 物料, 物料群組  FROM sap_sales_data 
#                 where 買方 like 'TW%' and 預計發貨日期 >='2021/01/22' 
#                 """)

# results = cursor.fetchall()
# sap = pd.DataFrame(results)
# sap.columns = ["SAP代號", "未稅本位幣","物料","物料群組" ]
# #sap1 = sap.drop_duplicates(subset=['買方'])
# sap1 = sap.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '近3年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap1 ,on ='SAP代號',how='left')
# sap2 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('K3', 'KE', 'WE')))]
# sap3 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith('WX1')) &
#     sap['物料'].apply(lambda x: isinstance(x, str) and x.startswith('K3'))]
# sap2 = pd.concat([sap2, sap3])
# sap2 = sap2.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '木地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap2 ,on ='SAP代號',how='left')

# sap3 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('WF', 'F', 'KEF')))]
# sap3 = sap3.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '環保地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap3 ,on ='SAP代號',how='left')

# sap4 = sap[sap['物料群組'].apply(lambda x: isinstance(x, str) and x.startswith(('FM', 'KEM', 'WM')))]
# sap4 = sap4.groupby('SAP代號')['未稅本位幣'].sum().reset_index(name= '超耐磨木地板近三年交易金額')
# contact_safe3  = pd.merge(contact_safe3, sap4 ,on ='SAP代號',how='left')


# contact_safe1.to_excel("C:/Users/11020856/Desktop/jupyter/CD類超耐磨型錄明細1107.xlsx",index = False)
# contact_safe2 = pd.merge(contact_safe, sap1 ,left_on ='SAP代號',right_on ='買方',how='inner')




# #  K大 / 拜訪記錄
# '''
# select from visiting
# '''
# url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
# headers = {
#     "Authorization": f"Bearer {ac_token}",
#     "Content-Type":"application/x-www-form-urlencoded"
#     # Replace with your actual access token
# }


# queryLocator = ''
# tracking = pd.DataFrame()
# while True:
#     data = {
#         "xoql": f'''
#         select
#         accountCode__c 公司代號,customItem40__c 最近拜訪日期,
#         customItem128__c 觸客類型

#         from customEntity15__c
#         where  customItem118__c like '%TW%'
#         and customItem128__c IN (1, 8)
#         ''',
#         "batchCount": 2000,
#         "queryLocator": queryLocator       
#     }
    
#     response2 = requests.post(url_2, headers=headers, data = data)
#     exist = response2.json()
#     data = pd.DataFrame(exist["data"]["records"])
#     tracking = pd.concat([tracking, data], ignore_index=True, sort=False)
    
#     if not exist['queryLocator']:
#         break
#     queryLocator = exist['queryLocator']


# tracking['最近拜訪日期'] = pd.to_numeric(tracking['最近拜訪日期'], errors='coerce')
# tracking['最近拜訪日期'] = pd.to_datetime(tracking['最近拜訪日期'] / 1000, unit='s', utc=True)
# tracking['最近拜訪日期'] = tracking['最近拜訪日期'].dt.tz_convert('Asia/Taipei')

# # 拜訪數據
# tracking_visit = tracking[tracking['觸客類型'].astype(str).str.contains("A1", na=False)]
# latest_visit = tracking_visit.sort_values('最近拜訪日期', ascending=False).drop_duplicates(subset='公司代號')
# latest_visit['最近拜訪日期'] = latest_visit['最近拜訪日期'].dt.date
# latest_visit = latest_visit[['最近拜訪日期', '公司代號']]
# contact_safe2 = pd.merge(contact_safe2, latest_visit ,left_on ='公司代號',right_on ='公司代號',how='left')
# contact_safe2['拜訪_flag'] = contact_safe2['公司代號'].isin(latest_visit['公司代號'])

# # K大數據
# tracking_MRK = tracking[tracking['觸客類型'].astype(str).str.contains("C1", na=False)]
# tracking_MRK = tracking_MRK.rename(columns={'最近拜訪日期': '最近K大日期'})
# latest_MRK = tracking_MRK.sort_values('最近K大日期', ascending=False).drop_duplicates(subset='公司代號')
# latest_MRK['最近K大日期'] = latest_MRK['最近K大日期'].dt.date
# latest_MRK = latest_MRK[['最近K大日期', '公司代號']]
# contact_safe2 = pd.merge(contact_safe2, latest_MRK ,left_on ='公司代號',right_on ='公司代號',how='left')
# contact_safe2['K大_flag'] = contact_safe2['公司代號'].isin(latest_MRK['公司代號'])











# #  案例機會 opportunity
# '''
# select from opportunity
# '''
# url_2 = "https://api-p10.xiaoshouyi.com/rest/data/v2.0/query/xoqlScroll"
# headers = {
#     "Authorization": f"Bearer {ac_token}",
#     "Content-Type":"application/x-www-form-urlencoded"
#     # Replace with your actual access token
# }


# queryLocator = ''
# opportunity = pd.DataFrame()
# while True:
#     data = {
#         "xoql": f'''
#         select accountCode__c 公司代號,
#         customItem224__c 主要連絡人代號,
#         customItem220__c 狀態
#         from opportunity
        
#         ''',
#         "batchCount": 2000,
#         "queryLocator": queryLocator       
#     }
    
#     response2 = requests.post(url_2, headers=headers, data = data)
#     exist = response2.json()
#     data = pd.DataFrame(exist["data"]["records"])
#     opportunity = pd.concat([opportunity, data], ignore_index=True, sort=False)
    
#     if not exist['queryLocator']:
#         break
#     queryLocator = exist['queryLocator']


# opportunity_finished = opportunity[opportunity['狀態'].astype(str).str.contains("結案", na=False)]
# opportunity_unfinished = opportunity[~opportunity['狀態'].astype(str).str.contains("結案", na=False)]

# contact_safe2['案例_flag'] = contact_safe2['公司代號'].isin(opportunity['公司代號'])
# contact_safe2['結案_flag'] = contact_safe2['公司代號'].isin(opportunity_finished['公司代號']) 


contact_df = contact_df.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)

today = datetime.today().strftime('%Y%m%d')  
filename = fr"C:\Users\TW0002.TPTWKD\Desktop\廣發型錄\型錄廣發名單_{today}.xlsx"

contact_df.to_excel(filename, index=False)
