
# -*- coding: utf-8 -*-


import os
import sys
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


# 0) 匯入kd
custom_path = Path(r"C:\Users\TW0002.TPTWKD\Desktop\項目整理")
sys.path.append(str(custom_path))
import common as kd  # noqa: E402


# 1) 共用路徑
BAD_DEBT_DIR = r"Z:\18_各部門共享區\01_會計課\11.海外子公司共用\呆帳"
BAD_DEBT_KEYWORD = "子公司歷年呆帳客戶名單"  
BAD_DEBT_SHEET = "歷年呆帳(勿改名稱!)"
BAD_DEBT_COL = "客代"

SHARED_CREDIT_DIR = r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度"
SHARED_CREDIT_KEYWORD = "共用額度登記"

OVERDUE_DIR = r"Z:\18_各部門共享區\06_海外業務部\$$每月逾期貨款\逾期帳款報表-每月更新"
OVERDUE_SHEET = "原幣"
OVERDUE_HEADER = 4
OVERDUE_USECOLS = ["客戶", "合計未收"]

COLLECTION_INFO_DIR = SHARED_CREDIT_DIR
COLLECTION_INFO_KEYWORD = "ALL客戶收款清單聯絡資訊"

# 2) 國家設定）
# FROM [SAPdb].[dbo].[ZSD31B]
@dataclass
class CountryConfig:
    code: str
    vkorg: str
    buyer_prefix: List[str]
    tax_rate: float
    base_path: str
    has_bad_debt: bool
    overdue_keyword: str
    level_dir: str
    level_keyword: str
    shared_credit_sheet: Optional[str] = None
    not_monthly_sheets: Optional[List[str]] = None


COUNTRY_CONFIGS: Dict[str, CountryConfig] = {
    "HK": CountryConfig(
        code="HK",
        vkorg="HK00",
        buyer_prefix=["HK"],
        tax_rate=0.0,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\3.  HK 香港",
        has_bad_debt=True,  
        overdue_keyword="月逾期帳款-HK(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\3.  HK 香港",
        level_keyword="月結額度",
        shared_credit_sheet="HK",
        not_monthly_sheets=["HK"],
    ),
    "JP": CountryConfig(
        code="JP",
        vkorg="JP00",
        buyer_prefix=["JP"],
        tax_rate=0.10,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\4.  JP 日本",
        has_bad_debt=True,
        overdue_keyword="月逾期帳款-JP(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\4.  JP 日本",
        level_keyword="月結額度",
        shared_credit_sheet="JP",
        not_monthly_sheets=["JP"],
    ),
    "PH": CountryConfig(
        code="PH",
        vkorg="PH00",
        buyer_prefix=["PH"],
        tax_rate=0.12,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\5.  PH 菲律賓",
        has_bad_debt=True,
        overdue_keyword="月逾期帳款-PH(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\5.  PH 菲律賓",
        level_keyword="月結額度",
        shared_credit_sheet="PH",
        not_monthly_sheets=["PH"],
    ),
    "ID": CountryConfig(
        code="ID",
        vkorg="ID00",
        buyer_prefix=["ID"],
        tax_rate=0.11,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\6.  ID 印尼",
        has_bad_debt=True,
        overdue_keyword="月逾期帳款-ID(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\6.  ID 印尼",
        level_keyword="月結額度",
        shared_credit_sheet="ID",
        not_monthly_sheets=["ID"],
    ),
    "VN": CountryConfig(
        code="VN",
        vkorg="VN00",
        buyer_prefix=["VN"],
        tax_rate=0.08,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\7.  VN 越南",
        has_bad_debt=True,
        overdue_keyword="月逾期帳款-VN(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\7.  VN 越南",
        level_keyword="月結額度",
        shared_credit_sheet="VN",
        not_monthly_sheets=["VN"],
    ),
    "TH": CountryConfig(
        code="TH",
        vkorg="TH00",
        buyer_prefix=["TH"],
        tax_rate=0.07,
        base_path=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\8. TH 泰國",
        has_bad_debt=True,
        overdue_keyword="月逾期帳款-TH(",
        level_dir=r"Z:\02_台灣事業部\1.北區\13.業務管理組\CRM共用資料夾\業助工作資料\業助資料夾\●皓皓●\9.自動化交易額度\★自動化月結\8. TH 泰國",
        level_keyword="月結額度",
        shared_credit_sheet="TH",
        not_monthly_sheets=["TH"],
    ),
}


# 3) 工具函數（共用）
def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def safe_strip_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def build_prefix_like_sql(col: str, prefixes: List[str]) -> str:
    return " OR ".join([f"{col} LIKE '{p}%'" for p in prefixes])


# 4) Data Loaders
def load_zsd31(cfg: CountryConfig) -> pd.DataFrame:
    prefix_sql = build_prefix_like_sql("KUNNR", cfg.buyer_prefix)

    df = kd.get_data_from_MSSQL(f"""
        SELECT
            KUNNR   AS 客戶編號,
            NAME1   AS 名稱1,
            SORT1   AS 搜尋條件1,
            VTEX8   AS 信用管制說明,
            COUNTRY AS 國家碼,
            VTEX07  AS 大群組說明,
            VTEX08  AS 小群組說明,
            KATR9   AS 區域,
            KLIMK   AS 交易額度,
            KONDA   AS 客戶價格群組
        FROM [SAPdb].[dbo].[ZSD31B]
        WHERE vkorg = '{cfg.vkorg}'
          AND KTOKD = 'YB01'
          AND ({prefix_sql})
    """)

    df = df[
        ["客戶編號", "名稱1", "搜尋條件1", "信用管制說明", "國家碼", "大群組說明", "小群組說明", "區域", "交易額度", "客戶價格群組"]
    ].rename(
        columns={
            "大群組說明": "大區",
            "小群組說明": "小區",
            "交易額度": "前月交易額度",
            "客戶價格群組": "類別",
        }
    )

    # 排除 Z 類客戶
    df = df[~df["類別"].astype(str).str.contains("z", case=False, na=False)].copy()

    df["客戶編號"] = safe_strip_series(df["客戶編號"])
    df["前月交易額度"] = pd.to_numeric(df["前月交易額度"], errors="coerce").fillna(0).astype(int)

    return df


def load_bad_debt(cfg: CountryConfig) -> pd.DataFrame:
    if not cfg.has_bad_debt:
        return pd.DataFrame(columns=["客戶編號"])

    path = kd.get_latest_excel(BAD_DEBT_DIR, BAD_DEBT_KEYWORD)
    df = pd.read_excel(path, sheet_name=BAD_DEBT_SHEET, usecols=[BAD_DEBT_COL]).drop_duplicates()
    df = df.rename(columns={BAD_DEBT_COL: "客戶編號"})
    df["客戶編號"] = safe_strip_series(df["客戶編號"])
    df["呆帳客戶"] = df["客戶編號"]
    return df[["客戶編號", "呆帳客戶"]]


def load_shared_credit_registry(cfg: CountryConfig) -> pd.DataFrame:
    """
    共用額度登記表：
    - 若指定 sheet 不存在 → 回傳空 DataFrame（代表該國沒有共用額度）
    - 不視為錯誤
    """
    path = kd.get_latest_excel(SHARED_CREDIT_DIR, SHARED_CREDIT_KEYWORD)
    sheet = cfg.shared_credit_sheet or cfg.code

    try:
        df = pd.read_excel(
            path,
            sheet_name=sheet,
            usecols=["客戶編號", "共用組數", "額度分配比例"],
        )
    except ValueError:
        # sheet 不存在，直接跳過
        print(f"⚠ 共用額度登記表中沒有 {sheet} 分頁，已跳過")
        return pd.DataFrame(columns=["客戶編號", "共用額度組別", "額度分配比例"])

    df = df.rename(columns={"共用組數": "共用額度組別"})
    df["客戶編號"] = df["客戶編號"].astype(str).str.strip()
    df["共用額度組別"] = pd.to_numeric(df["共用額度組別"], errors="coerce").astype("Int64")

    return df



def load_not_monthly_list(cfg: CountryConfig) -> List[str]:
    """
    共用額度登記表裡的不轉月結名單
    這裡改成 cfg.not_monthly_sheets（通常只要該國）
    """
    path = kd.get_latest_excel(SHARED_CREDIT_DIR, SHARED_CREDIT_KEYWORD)
    sheets = cfg.not_monthly_sheets or [cfg.code]

    dfs = []
    for sh in sheets:
        try:
            tmp = pd.read_excel(path, sheet_name=sh, usecols=["不轉月結名單"]).rename(columns={"不轉月結名單": "客戶編號"})
            dfs.append(tmp)
        except Exception:
            # 該國沒有此欄/此 sheet 就略過
            continue

    if not dfs:
        return []

    df = pd.concat(dfs, ignore_index=True)
    df["客戶編號"] = safe_strip_series(df["客戶編號"])
    df = df[df["客戶編號"].notna() & (df["客戶編號"].astype(str).str.strip() != "")]
    return df["客戶編號"].dropna().tolist()

def load_overdue(cfg: CountryConfig) -> pd.DataFrame:
    """
    逾期帳款：
    - 找不到檔案 → 視為沒有逾期，回傳空資料（全部當 0）
    - 不視為錯誤
    """

    try:
        path = get_latest_excel_or_fail(
            OVERDUE_DIR,
            cfg.overdue_keyword,
            country=cfg.code,
            usage="逾期帳款"
        )
    except RuntimeError:
        print(f"⚠ [{cfg.code}] 無逾期帳款資料，已視為 0")
        return pd.DataFrame(
            columns=["客戶編號", "當月逾期金額(依會計每月20號的逾期表)"]
        )

    data = pd.read_excel(
        path,
        sheet_name=OVERDUE_SHEET,
        header=OVERDUE_HEADER,
        usecols=OVERDUE_USECOLS
    )

    df = data.rename(columns={"客戶": "客戶編號"}).copy()
    df = df[df["客戶編號"].notna() & (df["客戶編號"].astype(str).str.strip() != "")]
    df["客戶編號"] = safe_strip_series(df["客戶編號"])

    df["合計未收"] = pd.to_numeric(df["合計未收"], errors="coerce").fillna(0)
    df["當月逾期金額(依會計每月20號的逾期表)"] = (
        df["合計未收"] / (1 + cfg.tax_rate) + 1e-8
    ).round(0)

    df.loc[df["合計未收"] < 0, "當月逾期金額(依會計每月20號的逾期表)"] = 0
    df["當月逾期金額(依會計每月20號的逾期表)"] = (
        df["當月逾期金額(依會計每月20號的逾期表)"]
        .fillna(0)
        .astype(int)
    )

    return df[["客戶編號", "當月逾期金額(依會計每月20號的逾期表)"]]


def load_sales_recent(cfg: CountryConfig, years: int = 2) -> pd.DataFrame:
    start_dt = datetime.now() - relativedelta(years=years)
    start_str = start_dt.strftime("%Y/%m/%d")

    buyer_like_sql = build_prefix_like_sql("buyer", cfg.buyer_prefix)

    sap = kd.get_data_from_MSSQL(f"""
        SELECT
            buyer AS 客戶編號,
            taxfree_basecurr AS 未稅本位幣,
            planned_shipping_date AS 預計發貨日期
        FROM sap_sales_data
        WHERE ({buyer_like_sql})
          AND planned_shipping_date >= '{start_str}'
          AND taxfree_basecurr > 0
    """)

    sap["客戶編號"] = safe_strip_series(sap["客戶編號"])
    sap["未稅本位幣"] = pd.to_numeric(sap["未稅本位幣"], errors="coerce").fillna(0)

    sap_sum = sap.groupby("客戶編號", as_index=False)["未稅本位幣"].sum()
    sap_sum = sap_sum.query("`未稅本位幣` > 0").copy()
    return sap_sum


def load_redeemed_amount(cfg: CountryConfig) -> pd.DataFrame:
    """
    ZFI66 已兌現金額：
    這裡改成用 cfg.buyer_prefix 做 KUNNR 對應（更直接），BUKRS/VKORG 條件也可以保留。
    """
    kunnr_like_sql = build_prefix_like_sql("KUNNR", cfg.buyer_prefix)

    df = kd.get_data_from_MSSQL(f"""
        SELECT
            KUNNR AS 客戶編號,
            SUM(DMBTR) AS [已兌現金額(稅後)]
        FROM [SAPdb].[dbo].[ZFI66]
        WHERE ({kunnr_like_sql})
        GROUP BY KUNNR
    """)

    df["客戶編號"] = safe_strip_series(df["客戶編號"])
    df["已兌現金額(稅後)"] = pd.to_numeric(df["已兌現金額(稅後)"], errors="coerce").fillna(0)

    # 轉未稅：依國家稅率
    df["已兌現金額(未稅)"] = (df["已兌現金額(稅後)"].div(1 + cfg.tax_rate).add(1e-8).round(0))
    df["已兌現金額(未稅)"] = pd.to_numeric(df["已兌現金額(未稅)"], errors="coerce").fillna(0)

    return df[["客戶編號", "已兌現金額(稅後)", "已兌現金額(未稅)"]]


def load_level_tables(cfg: CountryConfig) -> pd.DataFrame:
    """
    讀「月結額度」規範表（業務邏輯）
    - 檔案存在性由 get_latest_excel_or_fail 保證
    """

    path = get_latest_excel_or_fail(
        cfg.level_dir,
        cfg.level_keyword,
        country=cfg.code,
        usage="月結額度規範"
    )

    level_df = pd.read_excel(path, header=1)

    # 取 A,B,C 欄
    t = level_df.iloc[:, 0:3].copy()
    t.columns = ["級別", "累計兌現金額", "交易額度"]
    t = t.dropna(how="all")

    t["累計兌現金額"] = (
        pd.to_numeric(t["累計兌現金額"], errors="coerce")
        .fillna(0)
        .astype("Int64")
    )
    t["交易額度"] = (
        pd.to_numeric(t["交易額度"], errors="coerce")
        .astype("Int64")
    )

    return t



# 5) Strategy（底層可替換）
class BaseStrategy:
    """
    預設策略（共用，穩定版）
    - 可直接整串覆蓋
    - 不會因 HK / 欄位缺失 / 空資料炸掉
    - 未來各國分歧 → 繼承後 override 單一 method
    """

    SALES_YEARS = 2  # 預設兩年銷貨

    
    # 共用額度組別分類
    
    def classify_shared_group_category(
        self,
        shared_df: pd.DataFrame,
        cfg: CountryConfig
    ) -> pd.DataFrame:

        if shared_df is None or shared_df.empty:
            return pd.DataFrame(
                columns=["客戶編號", "共用額度組別", "額度分配比例", "共用額度組別分類"]
            )

        df = shared_df.copy()

        if "共用額度組別分類" not in df.columns:
            df["共用額度組別分類"] = np.nan

        df["共用額度組別分類"] = df["共用額度組別分類"].replace(
            ["", "nan", "None"], np.nan
        )

        # 預設：用客戶編號前兩碼
        df["共用額度組別分類"] = df.apply(
            lambda r: r["客戶編號"][:2]
            if pd.isna(r["共用額度組別分類"])
            else r["共用額度組別分類"],
            axis=1,
        )

        return df

    
    # 近 N 年是否有交易
    
    def apply_recent_sales_flag(
        self,
        df: pd.DataFrame,
        sales_sum: pd.DataFrame
    ) -> pd.DataFrame:

        out = df.merge(sales_sum, on="客戶編號", how="left")

        out["近2年有交易(現金可轉月結)"] = np.where(
            out["未稅本位幣"] > 0,
            "V",
            "X"
        )

        out["近2年有交易(現金可轉月結)"] = out[
            "近2年有交易(現金可轉月結)"
        ].fillna("X")

        return out

    
    # 共用額度：已兌現金額組合計
    
    def apply_shared_group_redeemed_rollup(
        self,
        df: pd.DataFrame
    ) -> pd.DataFrame:

        if df is None or df.empty:
            return df

        if "共用額度組別" not in df.columns:
            return df

        out = df.copy()

        valid = out["共用額度組別"].notna()
        if not valid.any():
            return out

        group_sum = (
            out.loc[valid]
            .groupby("共用額度組別")["已兌現金額(未稅)"]
            .sum()
            .reset_index()
            .rename(columns={"已兌現金額(未稅)": "共用組合計"})
        )

        out = out.merge(group_sum, on="共用額度組別", how="left")

        out["已兌現金額(未稅)"] = np.where(
            out["共用組合計"].notna(),
            out["共用組合計"],
            out["已兌現金額(未稅)"]
        )

        out.drop(columns=["共用組合計"], inplace=True)

        return out

    
    # 級別 → 交易額度
    
    def assign_credit_limit(
        self,
        df: pd.DataFrame,
        level_table: pd.DataFrame
    ) -> pd.DataFrame:

        out = df.copy()

        def calc_limit(row):
            used = row.get("已兌現金額(未稅)", 0)
            ratio = row.get("額度分配比例", np.nan)

            credit = 0
            for _, lv in level_table.iterrows():
                if used >= lv["累計兌現金額"]:
                    credit = lv["交易額度"]
                else:
                    break

            if pd.isna(ratio):
                return int(round(credit, 0))

            return int(round(credit * ratio, 0))

        out["交易額度"] = out.apply(calc_limit, axis=1)
        out["交易額度"] = out["交易額度"].fillna(0).astype(int)
        out.loc[out["交易額度"] == 0, "交易額度"] = 1

        return out

    
    # 額度變化 / 客戶分類（核心穩定版）
    
    def classify_customer(
        self,
        df: pd.DataFrame,
        not_monthly_list: list
    ) -> pd.DataFrame:

        out = df.copy()
        out["額度變化數值"] = out["交易額度"] - out["前月交易額度"]

        def rule(row):
            credit_ctrl = row.get("信用管制說明")
            bad_debt = row.get("呆帳客戶")
            overdue = row.get("當月逾期金額(依會計每月20號的逾期表)", 0)
            recent = row.get("近2年有交易(現金可轉月結)")
            cust_id = row.get("客戶編號")
            diff = row.get("額度變化數值", 0)

            # ---- 額度不變（優先） ----
            if credit_ctrl == "呆賬管制":
                return ("額度不變(管制)", "額度不變")

            if pd.notna(bad_debt) and str(bad_debt).strip():
                return ("額度不變(呆賬)", "額度不變")

            if overdue > 0:
                return ("額度不變(逾期)", "額度不變")

            if recent == "X":
                return ("額度不變(凍結)", "額度不變")

            if cust_id in not_monthly_list:
                return ("額度不變(共額不轉)", "額度不變")

            # ---- 額度變動 ----
            if diff < 0:
                return ("額度下降", "額度下降")

            if diff > 0:
                return ("額度上升", "額度上升")

            return ("額度不變", "額度不變")

        result = out.apply(rule, axis=1)

        out[["客戶分類", "額度變化"]] = pd.DataFrame(
            result.tolist(),
            index=out.index
        )

        # 額度不變 → 回填前月
        mask = out["額度變化"].str.contains("額度不變", na=False)
        out.loc[mask, "交易額度"] = out.loc[mask, "前月交易額度"]
        out.loc[mask, "額度變化數值"] = 0

        return out

    
    # 自動化(新月結 / 額度增加)
    
    def apply_automation(self, df: pd.DataFrame) -> pd.DataFrame:

        out = df.copy()
        out["新信用管制說明"] = out["信用管制說明"]

        # 現金客戶且額度變動 → 月結
        mask_cash = (
            (out["信用管制說明"] == "現金客戶") &
            (out["額度變化"].isin(["額度上升", "額度下降"]))
        )
        out.loc[mask_cash, "新信用管制說明"] = "月結"

        # 原月結，額度上升
        mask_inc = (
            (out["信用管制說明"] == "月結") &
            (out["額度變化"] == "額度上升")
        )
        out.loc[mask_inc, "客戶分類"] = "自動化(額度增加)"

        # 現金 → 月結（新月結）
        mask_new = (
            (out["信用管制說明"] == "現金客戶") &
            (out["額度變化"] == "額度上升")
        )
        out.loc[mask_new, "客戶分類"] = "自動化(新月結客戶)"

        return out


# 若未來某國要特例，照這樣做：
# class JPStrategy(BaseStrategy):
#     SALES_YEARS = 3
#     def classify_customer(...): override


STRATEGY_REGISTRY: Dict[str, BaseStrategy] = {
    "HK": BaseStrategy(),
    "JP": BaseStrategy(),
    "PH": BaseStrategy(),
    "ID": BaseStrategy(),
    "VN": BaseStrategy(),
    "TH": BaseStrategy(),
}


# 6) Export（共用）
def export_main_outputs(df: pd.DataFrame, cfg: CountryConfig, today_yyyymmdd: str) -> str:
    """
    產出「總額度判定」excel
    """
    folder = os.path.join(cfg.base_path, today_yyyymmdd)
    ensure_dir(folder)

    out_path = os.path.join(folder, f"{cfg.code}_總額度判定{today_yyyymmdd}.xlsx")
    df.to_excel(out_path, index=False)
    return out_path


def export_for_huiru(df: pd.DataFrame, cfg: CountryConfig, today_yyyymmdd: str) -> str:
    """
    - Sheet: 改月結 / 額度變更 / 共用 / 轉現金
    """
    folder = os.path.join(cfg.base_path, today_yyyymmdd)
    ensure_dir(folder)

    target_classifications = ["自動化(新月結客戶)", "額度下降", "自動化(額度增加)"]
    filtered_df = df[df["客戶分類"].isin(target_classifications)].copy()

    # 額度變更
    credit_change_columns = [
        "客戶編號",
        "信用控制範圍",
        "信用額度",
        "風險種類",
        "客戶信用群組",
        "前次信用額度(比對用)",
        "GOOGLE申請",
        "比對上次",
        "共用額度組別",
        "備註",
        "變更",
    ]
    credit_change = pd.DataFrame(columns=credit_change_columns)
    credit_change["客戶編號"] = filtered_df["客戶編號"].values
    credit_change["信用額度"] = filtered_df["交易額度"].values
    credit_change["前次信用額度(比對用)"] = filtered_df["前月交易額度"].values
    credit_change["共用額度組別"] = filtered_df.get("共用額度組別", pd.Series([pd.NA] * len(filtered_df))).values
    credit_change["備註"] = filtered_df["客戶分類"].values

    # 改月結
    new_monthly_df = df[df["客戶分類"] == "自動化(新月結客戶)"].copy()
    new_monthly_columns = [
        "客戶代碼",
        "公司代碼",
        "銷售組織",
        "配銷通路",
        "部門",
        "搜尋條件 2",
        "結帳日",
        "寄單日",
        "帳單寄送方式",
        "付款方式",
        "信用管制",
        "付款條件碼_K",
        "付款條件碼_V",
    ]
    new_monthly_table = pd.DataFrame(columns=new_monthly_columns)
    new_monthly_table["客戶代碼"] = new_monthly_df["客戶編號"].values
    new_monthly_table["搜尋條件 2"] = datetime.today().replace(day=1).strftime("%Y-%m-%d")
    new_monthly_table["結帳日"] = "結帳日30日/寄單日5日"
    new_monthly_table["寄單日"] = "票期5040"
    new_monthly_table["帳單寄送方式"] = "99 其他"
    new_monthly_table["付款方式"] = "02 匯款"
    new_monthly_table["信用管制"] = "月結"

    file_name = f"{cfg.code}_sap大批匯入(給惠茹)-{today_yyyymmdd}.xlsx"
    full_path = os.path.join(folder, file_name)

    with pd.ExcelWriter(full_path, engine="openpyxl") as writer:
        new_monthly_table.to_excel(writer, sheet_name="改月結", index=False)
        credit_change.to_excel(writer, sheet_name="額度變更", index=False)

        workbook = writer.book
        ws1 = workbook.create_sheet(title="共用")
        ws1.append(["客戶編號", "共用額度組別", "備註"])
        ws2 = workbook.create_sheet(title="轉現金")
        ws2.append(["客戶編號", "信用管制", "信用額度"])
        workbook.save(full_path)

    return full_path


def export_for_sales(df: pd.DataFrame, cfg: CountryConfig, today_yyyymmdd: str) -> str:

    folder = os.path.join(cfg.base_path, today_yyyymmdd)
    ensure_dir(folder)

    target_classifications = ["自動化(新月結客戶)", "額度下降", "自動化(額度增加)"]
    filtered_df = df[df["客戶分類"].isin(target_classifications)].copy()

    columns_for_sales = [
        "客戶編號",
        "名稱1",
        "搜尋條件1",
        "大區",
        "區域",
        "類別",
        "呆帳客戶",
        "客戶分類",
        "新信用管制說明",
        "交易額度",
        "月結客戶維持現金客戶(業務確認)",
        "維持原因(業務填寫)",
        "收款聯絡人",
        "收款聯絡人電話",
        "個人LINE網址",
        "群组LINE網址",
        "收款備註",
        "業務送帳單/收款需填原因",
        "新月結客結帳日/寄單日",
        "票期(統一5040不可動)",
        "新月結客帳單寄送方式",
        "新月結客付款方式",
        "月結起始日",
        "小區",
    ]
    final_sales_table = pd.DataFrame(columns=columns_for_sales)

    source_columns = [
        "客戶編號",
        "名稱1",
        "搜尋條件1",
        "大區",
        "區域",
        "類別",
        "呆帳客戶",
        "客戶分類",
        "新信用管制說明",
        "交易額度",
        "新月結客結帳日/寄單日",
        "票期(統一5040不可動)",
        "新月結客帳單寄送方式",
        "新月結客付款方式",
        "月結起始日",
        "小區",
    ]

    for col in source_columns:
        if col in filtered_df.columns:
            final_sales_table[col] = filtered_df[col].values

    # 收款聯絡資訊補齊
    info_path = kd.get_latest_excel(COLLECTION_INFO_DIR, COLLECTION_INFO_KEYWORD)
    info_df = pd.read_excel(info_path)
    info_df = info_df[["客戶", "收款連絡人", "收款連絡人電話", "收款人LINE ID", "群組LINE ID", "收款備註"]].drop_duplicates(
        subset="客戶", keep="first"
    )

    final_sales_table["客戶編號"] = safe_strip_series(final_sales_table["客戶編號"])
    info_df["客戶"] = safe_strip_series(info_df["客戶"])

    info_dict = info_df.set_index("客戶").to_dict(orient="index")

    final_sales_table["收款聯絡人"] = final_sales_table["客戶編號"].map(lambda x: info_dict.get(x, {}).get("收款連絡人", pd.NA))
    final_sales_table["收款聯絡人電話"] = final_sales_table["客戶編號"].map(lambda x: info_dict.get(x, {}).get("收款連絡人電話", pd.NA))
    final_sales_table["個人LINE網址"] = final_sales_table["客戶編號"].map(lambda x: info_dict.get(x, {}).get("收款人LINE ID", pd.NA))
    final_sales_table["群组LINE網址"] = final_sales_table["客戶編號"].map(lambda x: info_dict.get(x, {}).get("群組LINE ID", pd.NA))
    final_sales_table["收款備註"] = final_sales_table["客戶編號"].map(lambda x: info_dict.get(x, {}).get("收款備註", pd.NA))

    final_sales_table["月結起始日"] = datetime.today().replace(day=1).strftime("%Y-%m-%d")

    out_path = os.path.join(folder, f"{cfg.code}_新交易額度客戶明細-業務{today_yyyymmdd}.xlsx")
    final_sales_table.to_excel(out_path, index=False)
    return out_path

def get_latest_excel_or_fail(folder, keyword, *, country, usage):

    print(f" [{country}] 取得 {usage}")
    print(f"    路徑: {folder}")
    print(f"    關鍵字: {keyword}")

    path = kd.get_latest_excel(folder, keyword)

    if path is None:
        raise RuntimeError(
            f"\n [{country}] 缺少必要檔案：{usage}\n"
            f"   路徑: {folder}\n"
            f"   關鍵字: {keyword}\n"
        )

    return path

# 7) Engine（主流程：只負責調度）
def run_country(cfg: CountryConfig, strategy: BaseStrategy) -> Dict[str, str]:
    """
    回傳輸出檔路徑 dict
    """
    today_yyyymmdd = datetime.now().strftime("%Y%m%d")

    # 1) ZSD31
    zsd31 = load_zsd31(cfg)

    # 2) 呆帳
    bad_debt = load_bad_debt(cfg)
    df = zsd31.merge(bad_debt, on="客戶編號", how="left")

    # 3) 共用額度登記
    shared = load_shared_credit_registry(cfg)
    shared = strategy.classify_shared_group_category(shared, cfg)
    df = df.merge(shared, on="客戶編號", how="left")
    df = df.drop_duplicates(subset=["客戶編號"], keep="first")

    # 4) 逾期
    overdue = load_overdue(cfg)
    df = df.merge(overdue, on="客戶編號", how="left")
    df["當月逾期金額(依會計每月20號的逾期表)"] = df["當月逾期金額(依會計每月20號的逾期表)"].fillna(0).astype(int)

    # 5) 兩年銷貨
    sales_sum = load_sales_recent(cfg, years=strategy.SALES_YEARS)
    df = strategy.apply_recent_sales_flag(df, sales_sum)

    # 6) 已兌現金額
    redeemed = load_redeemed_amount(cfg)
    df = df.merge(redeemed, on="客戶編號", how="left")
    df["已兌現金額(稅後)"] = df["已兌現金額(稅後)"].fillna(0)
    df["已兌現金額(未稅)"] = df["已兌現金額(未稅)"].fillna(0)

    # 7) 共用組合計（把已兌現金額改成組合計）
    df = strategy.apply_shared_group_redeemed_rollup(df)

    # 8) 規範表 -> 交易額度
    level_table = load_level_tables(cfg)
    df = strategy.assign_credit_limit(df, level_table)

    # 9) 不轉月結名單
    not_monthly_list = load_not_monthly_list(cfg)

    # 10) 額度變化 / 客戶分類
    df = strategy.classify_customer(df, not_monthly_list)

    # 11) 自動化(新月結/額度增加)
    df = strategy.apply_automation(df)

    # 12) 後處理：新月結客戶固定欄位
    df = df.drop_duplicates(subset=["客戶編號"], keep="last").copy()

    d1 = datetime.today().strftime("%Y/%m/01")
    df.loc[df["客戶分類"] == "自動化(新月結客戶)", "月結起始日"] = d1
    df["月結起始日"] = pd.to_datetime(df["月結起始日"], errors="coerce", utc=True).dt.date

    df.loc[df["客戶分類"] == "自動化(新月結客戶)", "新月結客結帳日/寄單日"] = "結帳日30日/寄單日5日"
    df.loc[df["客戶分類"] == "自動化(新月結客戶)", "票期(統一5040不可動)"] = "票期5040"
    df.loc[df["客戶分類"] == "自動化(新月結客戶)", "新月結客帳單寄送方式"] = "99 其他"
    df.loc[df["客戶分類"] == "自動化(新月結客戶)", "新月結客付款方式"] = "02 匯款"

    # 13) 優先排序：自動化(新月結客戶) 放最上
    priority = {"自動化(新月結客戶)": 0}
    df = df.sort_values(by="客戶分類", key=lambda s: s.map(priority).fillna(1))

    # 14) 輸出
    out_main = export_main_outputs(df, cfg, today_yyyymmdd)
    out_huiru = export_for_huiru(df, cfg, today_yyyymmdd)
    out_sales = export_for_sales(df, cfg, today_yyyymmdd)

    return {"main": out_main, "huiru": out_huiru, "sales": out_sales}


def main():
    targets = ["HK", "JP", "PH", "ID", "VN", "TH"]
    # targets = ["JP"]
    results = {}

    for code in targets:
        cfg = COUNTRY_CONFIGS[code]
        strategy = STRATEGY_REGISTRY[code]

        print(f"執行 {code} 自動化月結")
        try:
            out = run_country(cfg, strategy)
            results[code] = out
            print(f" {code} 完成")
            print(f"   - main : {out['main']}")
            print(f"   - huiru: {out['huiru']}")
            print(f"   - sales: {out['sales']}")
        except Exception as e:
            print(f" {code} 失敗：{e}")

    print("\n===== SUMMARY =====")
    for code, out in results.items():
        print(code, out)


if __name__ == "__main__":
    main()
