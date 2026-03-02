
import re
import time
import random
import unicodedata
from typing import List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller
from difflib import SequenceMatcher
SCRIPT_VERSION = "v2.6"
CONN_STR = "mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/raw_data?driver=ODBC+Driver+17+for+SQL+Server"
TABLE_FQN = "[clean_data].[dbo].[spyder_boss]"
BAIDU_MAP_URL = "https://map.baidu.com/"

TRY_LIST_TOP = 12
STRICT_CITY_IN_ADDR = True
RETRY_ON_ERROR = 1
PRINT_DEBUG_MISMATCH = False
SEQ_RATIO_TH = 0.90
JACCARD_TH = 0.85
SUBSTR_MIN_LEN_STRICT = 4
SUBSTR_MIN_LEN_LOOSE = 2
def remove_invisible(s: str) -> str:
    out = []
    for ch in s or "":
        cat = unicodedata.category(ch)
        if cat in ("Cf", "Cc", "Cs"):
            continue
        if ch in ("\u00A0", "\u200B", "\u200C", "\u200D", "\u2060", "\uFEFF", "\u3000"):
            continue
        out.append(ch)
    return "".join(out)

PUNC_SPACE_PAT = re.compile(r"[\s\.\u2026·\-\—\(\)（）【】\[\]、／/‧•]+")

def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s or "")

def _base_clean(s: str) -> str:
    s = remove_invisible(_nfkc(s)).strip()
    s = PUNC_SPACE_PAT.sub("", s)
    return s
BRANCH_END_PAT = re.compile(
    r"(?:分公司|總店|门店|門店|旗舰店|旗艦店|体验店|體驗店|专卖店|專賣店|直营店|直營店|办事处|辦事處|中心|總部|营销中心|營銷中心"
    r"|徐汇|徐匯|浦东|浦東|闵行|閔行|嘉定|宝山|寶山|黄浦|黃浦|长宁|長寧|静安|靜安|虹口|杨浦|楊浦|青浦|松江|金山|奉贤|奉賢)$"
)
PAREN_PAIRS = [
    ("（", "）"), ("(", ")"), ("【", "】"), ("[", "]"), ("《", "》"), ("<", ">")
]
def _remove_all_parenthesized(s: str) -> str:
    if not s:
        return s
    text = s
    changed = True
    while changed:
        changed = False
        for l, r in PAREN_PAIRS:
            pattern = re.compile(rf"{re.escape(l)}[^ {re.escape(l)}{re.escape(r)}]*{re.escape(r)}")
            new_text = pattern.sub("", text)
            if new_text != text:
                text = new_text
                changed = True
    return text
INLINE_LOCATION_PAT = re.compile(
    r"(?:[^\W\d_]{1,6})(?:省|市|区|縣|县|州|盟|旗|镇|鎮|乡|鄉|新区|新區|开发区|開發區|经开区|經開區|高新区|高新區|园区|園區|工业园|工業園)"
)
def _remove_inline_locations(s: str) -> str:
    s = s.replace("公司", "＠公司＠")
    s = INLINE_LOCATION_PAT.sub("", s)
    return s.replace("＠公司＠", "公司")

GENERIC_TOKENS = [
    "有限责任公司","有限公司","股份有限公司","公司","集团","集團",
    "装饰设计工程","裝飾設計工程","装饰工程","裝飾工程","装饰设计","裝飾設計",
    "装饰","裝飾","裝潢","装修","裝修","設計","设计","工程",
    "家居","家装","家裝","建築","建筑","建設","建设",
    "科技","商貿","商贸","貿易","贸易","文化传媒","文化傳媒","傳媒","传媒",
    "廣告","广告","實業","实业","發展","发展","管理","信息","資訊","网络","網絡","服务","服務"
]
def _remove_generic_tokens(s: str) -> str:
    t = remove_invisible(_nfkc(s))
    for g in GENERIC_TOKENS:
        t = t.replace(g, "")
    return PUNC_SPACE_PAT.sub("", t)

TAIL_EQ_PAT = re.compile(r"(有限责任公司|股份有限公司|有限公司|有限)$")

def _strip_branch_suffix(s: str) -> str:
    s = re.sub(r"[（(][^）)]{0,30}[）)]$", "", s)
    s = re.sub(BRANCH_END_PAT, "", s)
    return s

def _normalize_tail_company(s: str) -> str:
    s = _base_clean(_strip_branch_suffix(s))
    return TAIL_EQ_PAT.sub("有限公司", s)

def _remove_city_tokens(s: str, city: Optional[str]) -> str:
    s2 = s or ""
    s2 = _remove_all_parenthesized(s2)
    if city:
        c = city.strip()
        if c:
            s2 = s2.replace(c + "市", "").replace(c, "")
    s2 = _remove_inline_locations(s2)
    return s2
def bigrams(s: str) -> set:
    return {s[i:i+2] for i in range(len(s)-1)} if len(s) >= 2 else {s}
def _core_strict(s: str, city: Optional[str]) -> str:
    s = _remove_city_tokens(s, city)
    s = _remove_generic_tokens(s)
    return _base_clean(s)

def _core_loose(s: str, city: Optional[str]) -> str:
    s = _remove_city_tokens(s, city)
    return _base_clean(s)

def names_match_precise_fuzzy(db_name: str, site_name: str, city: Optional[str]) -> bool:
    a0, b0 = _base_clean(db_name), _base_clean(site_name)
    if a0 and a0 == b0: return True

    a1, b1 = _base_clean(_strip_branch_suffix(db_name)), _base_clean(_strip_branch_suffix(site_name))
    if a1 and a1 == b1: return True

    a2, b2 = _normalize_tail_company(db_name), _normalize_tail_company(site_name)
    if a2 and a2 == b2: return True
    if a2 and b2:
        if (a2 + "公司" == b2) or (b2 + "公司" == a2): return True
        if (a2.endswith("有限公司") and b2.endswith("有限") and a2.startswith(b2)) or \
           (b2.endswith("有限公司") and a2.endswith("有限") and b2.startswith(a2)): return True
    ca, cb = _core_strict(db_name, city), _core_strict(site_name, city)
    use_loose = (len(ca) < SUBSTR_MIN_LEN_STRICT or len(cb) < SUBSTR_MIN_LEN_STRICT)
    if use_loose:
        ca, cb = _core_loose(db_name, city), _core_loose(site_name, city)

    if not ca or not cb:
        return False

    short, long = (ca, cb) if len(ca) <= len(cb) else (cb, ca)
    min_len = SUBSTR_MIN_LEN_LOOSE if use_loose else SUBSTR_MIN_LEN_STRICT
    if len(short) >= min_len and short in long:
        return True

    if SequenceMatcher(None, ca, cb).ratio() >= SEQ_RATIO_TH:
        return True

    ja, jb = bigrams(ca), bigrams(cb)
    jacc = (len(ja & jb) / (len(ja | jb) or 1))
    if jacc >= JACCARD_TH:
        return True

    return False
PHONE_REGEX = re.compile(r"(?:\+?86[-\s]?)?(1[3-9]\d{9}|\d{3,4}-?\d{7,8})")

def extract_phones(text_block: str) -> List[str]:
    if not text_block:
        return []
    m = PHONE_REGEX.findall(text_block)
    m = [x if isinstance(x, str) else x[0] for x in m]
    m = [re.sub(r"\s+", "", x) for x in m]
    out, seen = [], set()
    for x in m:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out
def attach_driver() -> webdriver.Chrome:
    chromedriver_autoinstaller.install()
    opts = webdriver.ChromeOptions()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=opts)
    driver.implicitly_wait(2)
    return driver

def wait_for_search_input(driver: webdriver.Chrome, timeout: int = 8):
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((
            By.CSS_SELECTOR,
            "input#sole-input, input.suggest-input, input[placeholder*='搜'], input[baiduSug], input[type='text']"
        ))
    )

def recover_to_search(driver: webdriver.Chrome):
    try:
        driver.get(BAIDU_MAP_URL)
        wait_for_search_input(driver, timeout=10)
        time.sleep(0.4)
        return True
    except Exception:
        return False

def ensure_search_mode(driver: webdriver.Chrome):
    try:
        wait_for_search_input(driver, timeout=4)
    except Exception:
        recover_to_search(driver)

def open_baidu_map(driver: webdriver.Chrome):
    driver.execute_script("window.open('about:blank','_blank');")
    driver.switch_to.window(driver.window_handles[-1])
    recover_to_search(driver)

def find_search_input(driver: webdriver.Chrome):
    ensure_search_mode(driver)
    candidates = [
        (By.CSS_SELECTOR, "input#sole-input"),
        (By.CSS_SELECTOR, "input.suggest-input"),
        (By.CSS_SELECTOR, "input[placeholder*='搜']"),
        (By.CSS_SELECTOR, "input[baiduSug]"),
        (By.CSS_SELECTOR, "input[type='text']"),
    ]
    for by, sel in candidates:
        try:
            elem = WebDriverWait(driver, 6).until(EC.presence_of_element_located((by, sel)))
            if elem.is_displayed():
                return elem
        except Exception:
            pass
    recover_to_search(driver)
    return WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='text']")))

def wait_results_loaded(driver: webdriver.Chrome):
    try:
        WebDriverWait(driver, 10).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "ul.poilist")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.ml_30.mr_90")),
                EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'电话') or contains(text(),'電話')]")),
            )
        )
    except Exception:
        time.sleep(2)

def first_card(driver: webdriver.Chrome):
    sels = [
        "ul.poilist > li.search-item.base-item",
        "ul.poilist > li[data-stat-code*='poisearch.all.item']",
        "div.ml_30.mr_90",
        ".result"
    ]
    for sel in sels:
        elems = driver.find_elements(By.CSS_SELECTOR, sel)
        if elems:
            return elems[0]
    return None

def read_name_addr_tel_from_card(card) -> Tuple[str, str, str]:
    name, addr, tel_text = "", "", ""
    try:
        links = card.find_elements(By.CSS_SELECTOR, "a.n-blue[data-stat-code*='poisearch']")
        if not links:
            links = card.find_elements(By.CSS_SELECTOR, "a")
        if links:
            link = links[0]
            name = (
                link.get_attribute("data-title")
                or link.get_attribute("aria-label")
                or link.get_attribute("data-name")
                or link.get_attribute("title")
                or link.text
                or ""
            ).strip()

        addr_elem = card.find_elements(By.CSS_SELECTOR, "div.row.addr, .row.addr, .addr, .address")
        if addr_elem:
            addr = addr_elem[0].text.strip()

        tel_elem = card.find_elements(By.CSS_SELECTOR, "div.row.tel, .row.tel, .tel, .phone, .poi-phone")
        if tel_elem:
            tel_text = tel_elem[0].text.strip()
    except Exception:
        pass
    return name, addr, tel_text

def search_and_pick(driver, company_name: str, city: Optional[str], try_list_top: int = TRY_LIST_TOP) -> Tuple[str, List[str], Optional[str], Optional[str]]:
    """
    回傳：(status, phones, matched_name, matched_addr)
    status: 'found' | 'mismatch' | 'not_found' | 'error'
    """
    def _extract_and_check(card):
        site_name, site_addr, tel_text = read_name_addr_tel_from_card(card)
        if site_name and names_match_precise_fuzzy(company_name, site_name, city):
            if STRICT_CITY_IN_ADDR and city:
                if site_addr and (city not in site_addr):
                    return None
            phones = extract_phones((tel_text or "") + "\n" + card.text)
            if phones:
                return ("found", phones, site_name, site_addr)
            try:
                link = card.find_element(By.CSS_SELECTOR, "a.n-blue")
                driver.execute_script("arguments[0].click();", link)
                time.sleep(1.0)
                panel_text = driver.find_element(By.TAG_NAME, "body").text
                phones2 = extract_phones(panel_text)
                if phones2:
                    return ("found", phones2, site_name, site_addr)
            except Exception:
                pass
            return ("not_found", [], site_name, site_addr)
        return None

    try:
        ensure_search_mode(driver)
        query = f"{(city or '').strip()} {company_name}".strip()
        inp = find_search_input(driver)
        inp.click(); inp.send_keys(Keys.CONTROL, "a"); inp.send_keys(Keys.DELETE)
        inp.send_keys(query); inp.send_keys(Keys.ENTER)
        wait_results_loaded(driver)

        card = first_card(driver)
        if card:
            res = _extract_and_check(card)
            if res: return res
        try:
            more = WebDriverWait(driver, 2).until(
                EC.presence_of_element_located((By.XPATH, "//a[contains(.,'查看全部') and contains(.,'条结果')]"))
            )
            driver.execute_script("arguments[0].click();", more)
            time.sleep(1.0)
        except Exception:
            pass

        items = driver.find_elements(
            By.CSS_SELECTOR,
            "ul.poilist > li.search-item.base-item, ul.poilist > li[data-stat-code*='poisearch.all.item']"
        )[:try_list_top]

        for it in items:
            res = _extract_and_check(it)
            if res: return res

        return ("mismatch", [], None, None)
    except Exception:
        return ("error", [], None, None)
def ensure_columns(engine: Engine):
    ddl_tpls = [
        ("phone_baidu",        "NVARCHAR(200) NULL"),
        ("phone_source_url",   "NVARCHAR(200) NULL"),
        ("phone_updated_at",   "DATETIME2 NULL"),
        ("phone_lookup_status","NVARCHAR(50) NULL"),
    ]
    with engine.begin() as conn:
        for col, typ in ddl_tpls:
            sql = f"""
IF COL_LENGTH(N'{TABLE_FQN}', N'{col}') IS NULL
BEGIN
  ALTER TABLE {TABLE_FQN} ADD {col} {typ};
END
"""
            conn.execute(text(sql))

def get_pending_company_city(engine: Engine, limit: int = 6000) -> pd.DataFrame:
    sql = f"""
SELECT TOP (:limit)
    company_name,
    city,
    MAX(NULLIF(LTRIM(RTRIM(address_norm)),'') ) AS any_address
FROM {TABLE_FQN} sb
WHERE (sb.phone_baidu IS NULL OR LTRIM(RTRIM(sb.phone_baidu)) = '')
  AND (sb.phone_lookup_status IS NULL)
GROUP BY company_name, city
ORDER BY company_name, city
"""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params={"limit": limit})

def company_city_has_phone(engine: Engine, company_name: str, city: Optional[str]) -> bool:
    cond_city = "((city = :city) OR (city IS NULL AND :city IS NULL))"
    sql = f"""
SELECT 1
FROM {TABLE_FQN}
WHERE company_name = :name
  AND {cond_city}
  AND (phone_baidu IS NOT NULL AND LTRIM(RTRIM(phone_baidu)) <> '')
"""
    with engine.connect() as conn:
        return conn.execute(text(sql), {"name": company_name, "city": city}).fetchone() is not None

def update_bulk(engine: Engine, company_name: str, city: Optional[str],
                status: str, phone_joined: Optional[str]):
    cond_city = "((city = :city) OR (city IS NULL AND :city IS NULL))"
    with engine.begin() as conn:
        if status == "found" and phone_joined:
            conn.execute(
                text(f"""
UPDATE {TABLE_FQN}
SET phone_baidu        = :phone,
    phone_source_url   = 'https://map.baidu.com',
    phone_updated_at   = SYSDATETIME(),
    phone_lookup_status= 'found'
WHERE company_name = :name
  AND {cond_city}
  AND (phone_baidu IS NULL OR LTRIM(RTRIM(phone_baidu)) = '')
"""),
                {"phone": phone_joined, "name": company_name, "city": city}
            )
        else:
            conn.execute(
                text(f"""
UPDATE {TABLE_FQN}
SET phone_updated_at   = SYSDATETIME(),
    phone_lookup_status= :status
WHERE company_name = :name
  AND {cond_city}
  AND (phone_baidu IS NULL OR LTRIM(RTRIM(phone_baidu)) = '')
"""),
                {"status": status, "name": company_name, "city": city}
            )
def main(limit: int = 6000, min_delay: float = 1.0, max_delay: float = 2.5):
    print(f"== 連線資料庫；SCRIPT_VERSION={SCRIPT_VERSION}")
    engine = create_engine(CONN_STR, pool_pre_ping=True)
    ensure_columns(engine)

    driver = attach_driver()
    open_baidu_map(driver)

    df = get_pending_company_city(engine, limit=limit)
    if df.empty:
        print("✅ 沒有待查 (company_name, city) 組合。")
        try: driver.quit()
        except Exception: pass
        return

    print(f"🔎 本批需處理組數：{len(df)}")

    for i, row in df.iterrows():
        name = (row["company_name"] or "").strip()
        city = (row["city"] or "").strip()
        if not name:
            continue

        if company_city_has_phone(engine, name, city if city else None):
            print(f"[SKIP] 已有電話：{city} | {name}")
            continue

        attempt = 0
        while True:
            prefix = f"[{i+1}/{len(df)}] 搜尋：{city} | {name} … "
            status, phones, matched_name, matched_addr = search_and_pick(
                driver, name, city if city else None, try_list_top=TRY_LIST_TOP
            )

            if status == "found" and phones:
                joined = " / ".join(phones)
                update_bulk(engine, name, city if city else None, "found", joined)
                print(prefix + f" FOUND：{joined}")
                break
            elif status in ("mismatch", "not_found"):
                update_bulk(engine, name, city if city else None, status, None)
                print(prefix + (" 名稱不符（mismatch）" if status == "mismatch" else "❌ 未找到電話"))
                if PRINT_DEBUG_MISMATCH and status == "mismatch":
                    def core_dbg(s, c):
                        strict = _core_strict(s, c)
                        loose = _core_loose(s, c)
                        return strict, loose
                    s_strict, s_loose = core_dbg(name, city)
                    m_strict, m_loose = core_dbg(matched_name or "", city)
                    print("\n—— DEBUG ——")
                    print("DB :", name)
                    print("MAP:", matched_name or "")
                    print("ADDR:", matched_addr or "")
                    print("CORE_STRICT  DB :", s_strict)
                    print("CORE_STRICT  MAP:", m_strict)
                    print("CORE_LOOSE   DB :", s_loose)
                    print("CORE_LOOSE   MAP:", m_loose)
                    print("———————")
                break
            else:
                if attempt < RETRY_ON_ERROR:
                    print(prefix + "ERROR → 正在復原並重試一次…", end="")
                    recover_to_search(driver)
                    time.sleep(0.85)
                    attempt += 1
                    continue
                update_bulk(engine, name, city if city else None, "error", None)
                print(prefix + "ERROR")
                break

        time.sleep(random.uniform(min_delay, max_delay))

    try:
        driver.quit()
    except Exception:
        pass
    print("完成！")

if __name__ == "__main__":
    main(limit=6000, min_delay=1.0, max_delay=2.5)
