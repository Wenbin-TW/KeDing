
import pandas as pd
import subprocess
import sys
import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

TZ_TW = ZoneInfo("Asia/Taipei")

# ================= 基本設定 =================
BASE_PATH = r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新"
EXCEL_PATH = os.path.join(BASE_PATH, "py檔案清單.xlsx")
LOG_DIR = os.path.join(BASE_PATH, "Log")
os.makedirs(LOG_DIR, exist_ok=True)

# 要執行的分類
# RUN_CATEGORIES = ["台灣拜訪", "台灣儲值金",  "台灣專案", "海外拜訪",]
RUN_CATEGORIES = ["台灣拜訪", "台灣專案",]

SLEEP_SECONDS = 5
MAX_RETRY = 3

# ================= Log 設定 =================
today_str = datetime.now(TZ_TW).strftime("%Y%m%d")
log_path = os.path.join(LOG_DIR, f"run_log_{today_str}.log")

def write_log(msg: str):
    ts = datetime.now(TZ_TW).strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")

# ================= 讀取設定檔 =================
df = pd.read_excel(EXCEL_PATH)

# ================= 預計執行清單 =================
to_run = []

for _, row in df.iterrows():
    file_path = row.get("完整路徑")
    is_run = bool(row.get("是否執行", False))
    category = row.get("分類")

    if (
        category in RUN_CATEGORIES
        and is_run
        and isinstance(file_path, str)
        and os.path.exists(file_path)
    ):
        to_run.append(file_path)

print("=== 本次預計執行的 py 檔案 ===")
for i, p in enumerate(to_run, 1):
    print(f"[{i}/{len(to_run)}] {p}")
print("================================")

write_log("=== 本次執行開始 ===")

# ================= 主流程 =================
total = len(to_run)
current = 0

for _, row in df.iterrows():
    file_path = row.get("完整路徑")
    is_run = bool(row.get("是否執行", False))
    category = row.get("分類")

    if category not in RUN_CATEGORIES or not is_run:
        continue

    if not isinstance(file_path, str) or not os.path.exists(file_path):
        write_log(f"[NOT FOUND] 分類={category} | {file_path}")
        continue

    current += 1
    print(f"\n>>> 執行進度 {current}/{total}")
    print(f">>> 分類: {category}")
    print(f">>> 檔案: {file_path}")

    for attempt in range(1, MAX_RETRY + 1):
        print(f">>> 嘗試第 {attempt} 次")
        start_time = time.time()
        write_log(f"[START] 分類={category} | 檔案={file_path} | 第 {attempt} 次")

        try:
            result = subprocess.run(
                [sys.executable, file_path],
                text=True,
                capture_output=True
            )

            if result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode,
                    result.args,
                    output=result.stdout,
                    stderr=result.stderr
                )

            duration = round(time.time() - start_time, 2)
            print(f">>> 成功 完成 {duration}s")
            write_log(
                f"[SUCCESS] 分類={category} | 檔案={file_path} | 耗時={duration}s"
            )
            break

        except subprocess.CalledProcessError as e:
            duration = round(time.time() - start_time, 2)
            stdout = (e.output or "").strip()
            stderr = (e.stderr or "").strip()

            print(f">>> 失敗 耗時={duration}s")

            write_log(
                f"[FAIL] 分類={category} | 檔案={file_path} | "
                f"第 {attempt} 次失敗 | 耗時={duration}s\n"
                f"===== STDOUT =====\n{stdout}\n"
                f"===== STDERR =====\n{stderr}\n"
                f"=================="
            )

            if attempt == MAX_RETRY:
                print(">>> 連續失敗 放棄此檔案")
                write_log(
                    f"[GIVE UP] 分類={category} | 檔案={file_path} | 連續失敗 {MAX_RETRY} 次"
                )

        time.sleep(SLEEP_SECONDS)

    time.sleep(SLEEP_SECONDS)

write_log("=== 本次執行結束 ===")







# import os
# import pandas as pd

# # 目標資料夾
# base_path = r"C:\Users\TW0002.TPTWKD\Desktop\Wenbin\外勤业务\拜訪清單_更新"

# rows = []

# for root, _, files in os.walk(base_path):
#     for file in files:
#         if file.lower().endswith(".py"):
#             full_path = os.path.join(root, file)
#             rows.append({
#                 "檔名": file,
#                 "完整路徑": full_path,
#                 "是否執行": True
#             })

# df = pd.DataFrame(rows)

# # 輸出 Excel
# output_path = os.path.join(base_path, "py檔案清單.xlsx")
# df.to_excel(output_path, index=False)

# print(f"已產生檔案：{output_path}")


