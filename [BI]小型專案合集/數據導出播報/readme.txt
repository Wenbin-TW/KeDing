# CRM 數據導出監控與企業微信自動播報系統 開發紀錄與踩坑筆記

### 項目背景

為防範業務與內部人員異常大批量撈取敏感資料（如客戶名單、銷貨明細等），資安與管理層要求針對 CRM 系統的數據導出行為進行監控。專案目標是每日自動抓取台灣與大陸兩套 CRM 系統的後台導出日誌，將這些紀錄與 HR 系統的人事資料進行綁定，並依照不同職級與部門的「合理下載額度」進行比對。當員工近一日或近三十日的下載量超過預設閾值時，系統需自動產出帶有顏色標記的 Excel 報表，並透過 Windows 底層呼叫截圖，將異常清單直接推播至企業微信群組，實現資安風險的即時阻斷與通報。

### 數據流轉邏輯

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'primaryColor': '#fff', 'edgeLabelBackground':'#fcfcfc', 'tertiaryColor': '#f4f4f4', 'lineColor': '#333'}}}%%
graph TD
    classDef start fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef process fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef logic fill:#e8eaf6,stroke:#3f51b5,stroke-width:2px;
    classDef finish fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px;

    subgraph Data_Extraction
        API_TW(台灣 P10 API):::start --> API_Merge(雙平台導出日誌合併)
        API_ML(大陸 SCRM API):::start --> API_Merge
        API_Merge --> Parse_Regex(正則提取檔名與業務模組):::logic
    end

    subgraph Identity_and_Rules
        Parse_Regex --寫入歷史表--> DB_Export[(MSSQL: crm_export_records)]:::process
        DB_Export --> Merge_HR(串接 hrs_staff_info_valid)
        Excel_Rules(Excel: CRM下載需求度表) --> Match_Engine(部門與職級權限匹配引擎):::logic
        Merge_HR --> Match_Engine
    end

    subgraph Aggregation_and_Alert
        Match_Engine --> Calculate(計算日均與月度消耗率)
        Calculate --篩選超標名單--> Format_Excel(OpenPyXL 渲染警示色塊)
        Format_Excel --> COM_Capture(Win32 COM 呼叫 Excel 截圖):::process
    end

    subgraph Notification
        COM_Capture --> WeChat_IMG(企業微信 Webhook 發送圖片):::finish
        Format_Excel --> WeChat_FILE(企業微信 Webhook 傳送實體檔案):::finish
    end

```

### 實作挑戰與卡點

1. 導出日誌的業務模組盲區。銷售易的導出 API 返回的結構非常簡陋，只記錄了誰在什麼時候下載了什麼檔名，並沒有直接標示該檔案屬於哪個業務模組（例如是客戶表還是報價單）。為了解決這個問題，只能透過正則表達式強制去拆解 originFileName，利用字串特徵去反推目標模組，並與程式中寫死的 target_list 進行過濾比對。只要業務端修改了導出範本的命名規則，這段解析就會立刻失效。
2. 複雜且模糊的權限匹配規則。HR 系統的部門名稱經常帶有虛擬組織或是後綴，導致與 Excel 權限表比對時經常對不上。程式中開發了 assign_limit_values_corrected 函式，利用多重欄位（gd4、tmp_departname、jobcodename）進行降級匹配。透過 pandas 的遮罩（mask）機制逐層過濾，雖然效能較差，但這是應對髒資料最直觀的解法。
3. 報表視覺化的自動化困境。主管要求推播到企業微信的不能只是生硬的文字，必須是帶有紅黃藍警示色塊的表格圖片。原本考慮用 matplotlib 畫表，但中文字型與儲存格框線的微調太過痛苦。最後妥協使用了 win32com 函式庫，直接在背景喚醒系統中的 Excel 應用程式，利用 CopyPicture 方法把範圍複製成圖片匯出。

### 技術細節與取捨

* 雙軌閾值檢驗機制。為避免單日偶發性的大量下載引發誤報，系統設計了雙重檢驗。除了計算昨日下載成功數與日上限的比例，還會一併計算近三十天的累計下載量與月上限的比例。利用正則表達式把超標的百分比提取出來，作為後續 OpenPyXL 決定填色（大於百分之一百五十填紅色，大於百分之一百二十填橘色）的依據。
* 資料庫快取與歷史回溯。API 只負責拉取近五天的異動資料並利用 UPSERT 邏輯更新進 MSSQL。計算統計數據時，則是直接從資料庫拉取近六個月的歷史紀錄來跑 pandas 聚合運算。這樣能大幅減少對 CRM 伺服器的 API 請求壓力，也確保了歷史數據的穩定性。

圖表展示了經過條件格式渲染後的報表截圖，可以直接在企業微信群組中一眼看出哪些部門的哪位員工嚴重踩線，輔助稽核人員快速介入調查。
