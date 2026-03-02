import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
CUSTOM_PATH = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(CUSTOM_PATH))
import common as kd 
BASE_DIR = r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度"
FILE_CONFIG = {
    "業管客資": r"Z:\18_各部門共享區\03_台灣事業部\TO 圻馨\每日新增ZSD31",
    "呆帳": r"Z:\18_各部門共享區\01_會計課\11.海外子公司共用\呆帳",
    "逾期": r"Z:\18_各部門共享區\03_台灣事業部\6.訊息公佈欄\4.逾期收款明細表",
    "五項指標": r"Z:\18_各部門共享區\03_台灣事業部\TO 圻馨\每日新增ZSD31"
}

def clean_client_id(df, col='客戶編號'):
    return df[col].astype(str).str.strip()

def load_data():
    path = kd.get_latest_excel(FILE_CONFIG["業管客資"], "業管-客資(自動化月結)")
    df_zsd31 = pd.read_excel(path, sheet_name="SAP_Data", header=None)
    df_zsd31 = pd.DataFrame(df_zsd31.values[1:-3], columns=df_zsd31.iloc[0])
    df_zsd31 = df_zsd31[['客戶編號', '名稱1', '搜尋條件1', '信用管制說明', '大群組說明', '小群組說明', '區域', '交易額度', '客戶價格群組']]
    df_zsd31.columns = ['客戶編號', '名稱1', '搜尋條件1', '信用管制說明', '大區', '小區', '區域', '前月交易額度', '類別']
    df_zsd31['客戶編號'] = clean_client_id(df_zsd31)
    df_zsd31['前月交易額度'] = pd.to_numeric(df_zsd31['前月交易額度'], errors='coerce').fillna(0).astype(int)
    zsd31 = df_zsd31[
        (~df_zsd31['客戶編號'].str.contains('P')) & 
        (~df_zsd31['搜尋條件1'].str.contains('建檔錯誤', na=False)) & 
        (df_zsd31['大區'].notna())
    ].copy()
    path_bad_debt = kd.get_latest_excel(FILE_CONFIG["呆帳"], "1號交呆帳明細20")
    df_bad_debt = pd.read_excel(path_bad_debt, sheet_name='呆帳明細', usecols=['客代']).drop_duplicates()
    df_bad_debt['客代'] = clean_client_id(df_bad_debt, '客代')
    path_shared = kd.get_latest_excel(BASE_DIR, "共用額度登記")
    df_shared = pd.read_excel(path_shared, sheet_name="工作表1", usecols=['客戶編號', '共用組數', '分配額度', '額度分配比例'])
    df_shared.columns = ['客戶編號', '共用額度組別', '分配額度', '額度分配比例']
    df_shared['客戶編號'] = clean_client_id(df_shared)
    path_overdue = kd.get_latest_excel(FILE_CONFIG["逾期"], "逾期帳款(製表人陳宗卿 2025")
    df_overdue = pd.concat(pd.read_excel(path_overdue, sheet_name=['北', '中', '南'], header=3, usecols=['客戶', '合計未收']).values())
    df_overdue = df_overdue[df_overdue['客戶'].str.contains('TW', na=False)].copy()
    df_overdue['客戶編號'] = clean_client_id(df_overdue, '客戶')
    df_overdue['當月逾期金額'] = (df_overdue['合計未收'] / 1.05).clip(lower=0).round(0).astype(int)
    
    return zsd31, df_bad_debt, df_shared, df_overdue

def process_logic(zsd31, df_bad_debt, df_shared, df_overdue):
    df = pd.merge(zsd31, df_bad_debt, left_on="客戶編號", right_on="客代", how='left')
    df = pd.merge(df, df_shared, on="客戶編號", how='left')
    df = pd.merge(df, df_overdue[['客戶編號', '當月逾期金額']], on="客戶編號", how='left').fillna({'當月逾期金額': 0})
    two_years_str = (datetime.now() - relativedelta(years=2)).strftime('%Y/%m/%d')
    sql_sales = f"SELECT buyer as 客戶編號, taxfree_basecurr FROM sap_sales_data WHERE buyer LIKE 'TW%' AND planned_shipping_date >= '{two_years_str}'"
    df_sap_sales = kd.get_data_from_MSSQL(sql_sales).groupby('客戶編號')['taxfree_basecurr'].sum().reset_index()
    
    sql_cash = "SELECT KUNNR as 客戶編號, sum(DMBTR) as '已兌現金額_稅後' FROM [SAPdb].[dbo].[ZFI66] WHERE BUKRS = '1000' GROUP BY KUNNR"
    df_cash = kd.get_data_from_MSSQL(sql_cash)
    df_cash['已兌現金額_未稅'] = (df_cash['已兌現金額_稅後'] / 1.05).round(0)
    df = pd.merge(df, df_sap_sales, on='客戶編號', how='left')
    df = pd.merge(df, df_cash[['客戶編號', '已兌現金額_未稅']], on='客戶編號', how='left').fillna(0)
    df['近2年有交易'] = np.where(df['taxfree_basecurr'] > 0, 'V', 'X')
    path_rank = kd.get_latest_excel(BASE_DIR, "級別及額度")
    df_rank = pd.read_excel(path_rank, header=1).dropna(subset=['累計兌現金額(元)'])
    
    def get_rank_credit(money, mode='credit'):
        bins = df_rank['累計兌現金額(元)'].tolist()
        labels = df_rank['信用額度(元)'].tolist() if mode == 'credit' else df_rank['級別'].tolist()
        idx = np.digitize(money, bins) - 1
        return labels[max(0, idx)]
    df['兌現_信用額度'] = df['已兌現金額_未稅'].apply(lambda x: get_rank_credit(x, 'credit') if x > 0 else 250000)
    df['兌現_級別'] = df['已兌現金額_未稅'].apply(lambda x: get_rank_credit(x, 'rank') if x >= 50000 else '現金')
    group_cash = df.groupby('共用額度組別')['已兌現金額_未稅'].sum().reset_index(name='組別總兌現')
    df = pd.merge(df, group_cash, on='共用額度組別', how='left')
    df['組別判定額度'] = df['組別總兌現'].apply(lambda x: get_rank_credit(x, 'credit') if pd.notna(x) else np.nan)
    df['最新交易額度'] = df['兌現_信用額度']
    mask_freeze = (df['已兌現金額_未稅'] <= 50000) | (df['信用管制說明'] == '呆賬管制') | (df['當月逾期金額'] > 0) | (df['近2年有交易'] == 'X')
    df.loc[mask_freeze, '最新交易額度'] = df['前月交易額度']
    
    return df

def export_results(df):
    today = datetime.now().strftime("%Y%m%d")
    output_path = Path(BASE_DIR) / "自動化月結_輸出" / today
    output_path.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path / f"新交易額度客戶明細_{today}.xlsx", index=False)
    with pd.ExcelWriter(output_path / f"sap大批匯入_{today}.xlsx") as writer:
        df[df['最新交易額度'] != df['前月交易額度']].to_excel(writer, sheet_name='額度變更', index=False)

    print(f"處理完成，檔案儲存於: {output_path}")

if __name__ == "__main__":
    zsd31, bad_debt, shared, overdue = load_data()
    final_df = process_logic(zsd31, bad_debt, shared, overdue)
    export_results(final_df)