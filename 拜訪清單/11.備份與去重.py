import pandas as pd
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 設定今天日期與資料夾名稱
today_str = datetime.today().strftime('%Y.%m.%d')
month_folder = (datetime.today() + relativedelta(months=1)).strftime('%Y.%m')

#    f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{month_folder}"
# 定位到指定資料夾
folder = Path(    
   f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{month_folder}"
)



from pathlib import Path
import shutil

# === 先備份：把今天的所有檔案複製到「文斌備份用」資料夾 ===
backup_dir = folder / "文斌備份用"
backup_dir.mkdir(parents=True, exist_ok=True)

copied = 0
for src in folder.glob(f"*{today_str}.xlsx"):
    if src.is_file():
        dst = backup_dir / src.name
        # 若已存在就覆蓋；想保留舊檔可改成：if not dst.exists(): 再 copy2
        shutil.copy2(src, dst)
        copied += 1
print(f" 已備份 {copied} 個檔案到：{backup_dir}")



# 1. 讀取主管前50大客戶檔案，保留公司代號
top_customer_path = folder / f"主管前50大客戶_{today_str}.xlsx"
df_top = pd.read_excel(top_customer_path, dtype=str)  # 保留格式
target_company = df_top['公司代號'].dropna().unique().tolist()
print(f" 抓到的公司代號共 {len(target_company)} 筆")

# 2. 遍歷其他檔案，排除主管檔案
for file in folder.glob(f"*{today_str}.xlsx"):
    if "主管前50大客戶" in file.name:
        continue  # 跳過主管那份

    print(f" 處理中: {file.name}")
    df = pd.read_excel(file, dtype=str)  # 讀入所有欄位為字串，保留原始格式

    if '公司代號' not in df.columns:
        print(f" 檔案中沒有 '公司代號' 欄位，略過: {file.name}")
        continue

    # 3. 過濾掉重複公司
    before = len(df)
    df_filtered = df[~df['公司代號'].isin(target_company)]
    after = len(df_filtered)
    print(f" 剔除 {before - after} 筆公司代號，剩下 {after} 筆")

    # 4. 儲存過濾後的檔案
    save_path = file.with_name(file.stem + ".xlsx")
    df_filtered.to_excel(save_path, index=False)
    print(f" 已儲存: {save_path.name}")



# ###########
# # 只處理特定檔案
# import pandas as pd
# from pathlib import Path
# from datetime import datetime
# from dateutil.relativedelta import relativedelta

# # 設定今天日期與資料夾名稱
# today_str = datetime.today().strftime('%Y.%m.%d')  # 今天日期（用來找 4/28的檔案）
# month_folder = (datetime.today() + relativedelta(months=1)).strftime('%Y.%m')

# # 指定資料夾
# folder = Path(
#     f"Z:/02_台灣事業部/1.北區/13.業務管理組/CRM共用資料夾/業助工作資料/業助資料夾/●皓皓●/★每月業務電拜訪★/共用CRM資訊/Temp/電拜訪清單資料/{month_folder}"
# )

# # 1. 固定抓 2025/04/24 的主管前50大客戶
# top_customer_path = folder / "主管前50大客戶_2025.04.24.xlsx"
# df_top = pd.read_excel(top_customer_path, dtype=str)  # 保留格式
# target_company = df_top['公司代號'].dropna().unique().tolist()
# print(f" 抓到的公司代號共 {len(target_company)} 筆")

# # 2. 只處理指定的兩個檔案
# file_names = [
#     f"新建CRM客戶_{today_str}.xlsx",
#     f"近半年未叫貨_{today_str}.xlsx"
# ]

# for file_name in file_names:
#     file = folder / file_name
#     if not file.exists():
#         print(f" 找不到檔案: {file.name}，跳過")
#         continue

#     print(f" 處理中: {file.name}")
#     df = pd.read_excel(file, dtype=str)  # 全部讀成字串

#     if '公司代號' not in df.columns:
#         print(f" 檔案中沒有 '公司代號' 欄位，略過: {file.name}")
#         continue

#     # 過濾掉主管前50大客戶
#     before = len(df)
#     df_filtered = df[~df['公司代號'].isin(target_company)]
#     after = len(df_filtered)
#     print(f" 剔除 {before - after} 筆公司代號，剩下 {after} 筆")

#     # 儲存過濾後的檔案（覆蓋原檔）
#     save_path = file.with_name(file.stem + ".xlsx")
#     df_filtered.to_excel(save_path, index=False)
#     print(f" 已儲存: {save_path.name}")




# import pandas as pd
# from pathlib import Path

# # 1. 設定檔案路徑
# folder = Path(r"C:\Users\TW0002.TPTWKD\Desktop\0620")

# # 2. 主管前50大客戶的檔案（6/20 的版本）
# top_customer_path = folder / "主管前50大客戶_2025.06.20.xlsx"
# df_top = pd.read_excel(top_customer_path, dtype=str)
# target_company = df_top['公司代號'].dropna().unique().tolist()
# print(f" 抓到的公司代號共 {len(target_company)} 筆")

# # 3. 只處理這兩個檔案（6/25 的）
# file_names = [
#     "CD類儲值金客戶_2025.06.25.xlsx",
#     "CD類儲值金未滿10萬客戶_2025.06.25.xlsx"
# ]

# for file_name in file_names:
#     file = folder / file_name
#     if not file.exists():
#         print(f" 找不到檔案: {file.name}，跳過")
#         continue

#     print(f" 處理中: {file.name}")
#     df = pd.read_excel(file, dtype=str)

#     if '公司代號' not in df.columns:
#         print(f" 檔案中沒有 '公司代號' 欄位，略過: {file.name}")
#         continue

#     # 過濾掉主管前50大客戶
#     before = len(df)
#     df_filtered = df[~df['公司代號'].isin(target_company)]
#     after = len(df_filtered)
#     print(f" 剔除 {before - after} 筆公司代號，剩下 {after} 筆")

#     # 覆蓋儲存
#     save_path = file.with_name(file.stem + ".xlsx")
#     df_filtered.to_excel(save_path, index=False)
#     print(f" 已儲存: {save_path.name}")
