# 智聯 v2（列表抽取 + 城市下拉 + 即時入庫）

## 功能
- 解析 `.joblist-box__item` → 地址 / 公司 / 產業 / 規模 / 聯絡人
- 使用頂部城市下拉（`.content-s`）切換城市；不依賴城市代碼
- 翻頁模式自動識別：`/p1` 或 `?p=1`
- cookies 登入（Netscape cookies.txt 或 JSON），只取 `*.zhaopin.com` 條目
- MSSQL 即時 MERGE 入庫（自動建表、補欄位、唯一索引）
- 可中斷續跑；也支援從頭跑（本次清除進度）

## 使用
1. 將 cookies 放入 `.\\data\\cookies_zhaopin.txt`（或在 `.env` 指向 `C:\Users\TW0002.TPTWKD\Downloads\cookies.txt`）
2. 從頭跑：`run_zhilian_v2_from_scratch.bat`；之後續跑用：`run_zhilian_v2.bat`
