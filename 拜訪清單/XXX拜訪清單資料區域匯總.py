import pandas as pd
from pathlib import Path
folder_path = Path(r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\★每月業務電拜訪★\共用CRM資訊\Temp\電拜訪清單資料\2025.11\文斌備份用")
excel_files = list(folder_path.glob("*.xls*"))
result = []
for file in excel_files:
    df = pd.read_excel(file)
    if '資料區域群組名稱' not in df.columns:
        print(f"欄位不存在，跳過：{file.name}")
        continue
    cnt = ( df['資料區域群組名稱'].value_counts(dropna=False).rename(file.stem) )
    result.append(cnt)
summary_df = pd.concat(result, axis=1).fillna(0).astype(int)
summary_df.to_excel("拜訪清單資料區域匯總202511.xlsx")

summary_df
