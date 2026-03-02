
import pytz
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from pathlib import Path
import sys

from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd


yeaterday = pytz.timezone('Asia/Taipei').localize(datetime.now() - timedelta(days=1))
last_month_str = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')
sample_date = int(datetime(2025,1, 1).timestamp() * 1000)
today = pd.to_datetime(datetime.today().strftime('%Y-%m-%d'))
year_ago_one = pd.to_datetime((datetime.today() - relativedelta(years=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_one = pd.to_datetime((datetime.today() - relativedelta(months=1) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_two = pd.to_datetime((datetime.today() - relativedelta(months=2) + relativedelta(days=1)).date()).timestamp()*1000
month_ago_three = pd.to_datetime((datetime.today() - relativedelta(months=3) + relativedelta(days=1)).date()).timestamp()*1000
three_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-3)).timestamp() * 1000)
today_begin = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=0)).timestamp() * 1000)
today_end = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp() * 1000)
five_days_ago = int((datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=-50)).timestamp() * 1000)



test_tw= kd.get_data_from_CRM (f'''
        select id, name 交辦編號,entityType 業務類型, customItem3__c 工作主旨,customItem8__c 執行狀態,customItem10__c.name 執行人,
        customItem11__c.accountCode__c 公司代號, customItem11__c.accountName 公司名稱,  customItem11__c.Phone__c  公司電話,customItem42__c.name 客戶關係聯絡人代號,
        customItem42__c.contactPhone__c__c 聯絡人手機號,customItem56__c 連絡人姓名, customItem120__c 期望完成日期, customItem212__c.name 交辦來源
        from customEntity14__c 
        where customItem8__c in (1,2,3) and createdAt >= {sample_date}

        ''')

kd.convert_to_date(test_tw,'期望完成日期')

test_tw["業務類型"] = test_tw["業務類型"].astype(str).str.replace(r"\.0$", "", regex=True).astype("int64")

busi_type_map = {
    2766438431723495: "一般交辦",
    3018522602084760: "案例交辦",
    2767821733140804: "支援交辦",
    2997165162617236: "電/拜訪交辦",
    2904963933786093: "寄後電訪交辦",
    3028348436713387: "每日K大邀約電訪交辦",
    3077984163073940: "釣魚簡訊交辦",
    3440484496398786: "寄後展館邀約",
    3280291971403721: "新客電訪交辦",
    3082115462568332: "K大後電訪交辦",
    2956210976102731: "進料時程交辦"}

test_tw["業務類型"] = test_tw["業務類型"].map(busi_type_map)
test_tw["聯絡人手機號_clean"] = test_tw["聯絡人手機號"].astype(str).str.strip().replace({"": None, "nan": None})
test_tw["公司電話_clean"] = test_tw["公司電話"].astype(str).str.strip().replace({"": None, "nan": None})
test_tw["手機號出現次數"] = (
    test_tw.groupby("聯絡人手機號_clean")["聯絡人手機號_clean"]
    .transform("count")
    .fillna(0)
    .astype(int)
)
test_tw["公司電話出現次數"] = (
    test_tw.groupby("公司電話_clean")["公司電話_clean"]
    .transform("count")
    .fillna(0)
    .astype(int)
)
test_tw = test_tw.drop(columns=["聯絡人手機號_clean", "公司電話_clean"])

duplicate_rows = test_tw[ (test_tw["手機號出現次數"] > 1) | (test_tw["公司電話出現次數"] > 1)].copy()
unduplicate_rows = test_tw[ (test_tw["手機號出現次數"] <= 1) & (test_tw["公司電話出現次數"] <= 1)].copy()




duplicate_rows = duplicate_rows.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
unduplicate_rows = unduplicate_rows.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
test_tw = test_tw.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
save_dir = r"Z:\06_業管部\04_管理課\台灣業助組\-數中自動化產出數據\每周一8點未關閉交辦"
today = datetime.today()
file_name = f"未關閉交辦_{today.year}_{today.month:02d}_{today.day:02d}.xlsx"
save_path = f"{save_dir}\\{file_name}"
with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
    test_tw.to_excel(writer, index=False, sheet_name="全部交辦")
    duplicate_rows.to_excel(writer, index=False, sheet_name="重複資料")
    unduplicate_rows.to_excel(writer, index=False, sheet_name="不重複資料")

print(f"✔ 檔案已輸出：{save_path}")
