
import os, re, sys, time, json, random, logging, hashlib
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote_plus, urlencode, urlparse, urlunparse, parse_qs

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from playwright.sync_api import (
    sync_playwright, Browser, BrowserContext, Page,
    TimeoutError as PWTimeoutError
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
QUERY = os.getenv("QUERY", "室内设计")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"
CITIES_WHITELIST = [
    "上海","北京","天津","厦门","大连","北海","中山","徐州","武汉","宁波",
    "佛山","西安","岳阳","东莞","合肥","苏州","成都","昆明","沈阳","莆田",
    "杭州","南宁","长沙","郑州","深圳","惠州","汕头","重庆","广州","肇庆",
    "南京","潮州","镇江","嘉兴","襄阳","太原","宜昌","珠海","烟台","威海"
]
ALIASES = {
    "上海市": "上海", "北京市": "北京", "廣州": "广州", "廣州市": "广州",
    "深圳市": "深圳", "杭州市": "杭州", "南京市": "南京", "蘇州": "苏州",
    "天津市": "天津", "瀋陽": "沈阳", "廈門": "厦门", "寧波": "宁波", "嘉興": "嘉兴", "鎮江": "镇江"
}
MSSQL_URL = os.getenv("MSSQL_URL", "mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/clean_data?driver=ODBC+Driver+17+for+SQL+Server")
TABLE_NAME = os.getenv("TABLE_NAME", "clean_data.dbo.spyder_zhilian")
RESUME = os.getenv("RESUME", "true").lower() == "true"
RESET_RESUME = os.getenv("RESET_RESUME", "false").lower() == "true"
PROGRESS_PATH = os.getenv("PROGRESS_PATH", "./data/resume_zhilian.json")
WAIT_MIN_MS = int(os.getenv("WAIT_MIN_MS", "600"))
WAIT_MAX_MS = int(os.getenv("WAIT_MAX_MS", "1600"))
NETWORK_IDLE_MS = int(os.getenv("NETWORK_IDLE_MS", "1500"))
DETAILS_PER_CONTEXT = int(os.getenv("DETAILS_PER_CONTEXT", "14"))

DATA_DIR = os.getenv("DATA_DIR", "./data")
LOG_DIR = os.getenv("LOG_DIR", "./logs")
COOKIES_TXT = os.getenv("COOKIES_TXT", "./data/cookies_zhaopin.txt")
COOKIES_JSON = os.getenv("COOKIES_JSON", "./data/cookies_zhaopin.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("zhilian_v2")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt)
fh = logging.FileHandler(os.path.join(LOG_DIR, "zhilian_v2.log"), encoding="utf-8"); fh.setFormatter(fmt)
if not logger.handlers:
    logger.addHandler(ch); logger.addHandler(fh)

def jitter_sleep(a=WAIT_MIN_MS, b=WAIT_MAX_MS):
    time.sleep(random.randint(a, b) / 1000.0)

def normalize_text(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()

def normalize_city_name(city: str) -> str:
    city = normalize_text(city); return ALIASES.get(city, city)

def judge_city(card_city: str, toolbar_city: str, address_text: str) -> Optional[str]:
    for cand in [card_city, toolbar_city]:
        c = normalize_city_name(cand or "")
        if c in CITIES_WHITELIST: return c
    for c in CITIES_WHITELIST:
        if c in (address_text or ""): return c
    return None

def hash_id(company_url: str, address_norm: str) -> str:
    m = hashlib.md5(); m.update(f"{company_url}|{address_norm}".encode("utf-8","ignore")); return m.hexdigest()
def _split_three(full: str) -> Tuple[Optional[str], str, str]:
    parts = [p for p in full.split(".") if p]
    if len(parts) == 3: return parts[0], parts[1], parts[2]
    if len(parts) == 2: return None, parts[0], parts[1]
    if len(parts) == 1: return None, "dbo", parts[0]
    return None, "dbo", "spyder_zhilian"

def ensure_table_and_columns(engine: Engine, full_name: str):
    db, schema, table = _split_three(full_name)
    qp = (f'[{db}].' if db else '')
    with engine.begin() as conn:
        exists = conn.execute(text(f"""
            SELECT 1
            FROM {qp}sys.tables t
            JOIN {qp}sys.schemas s ON s.schema_id=t.schema_id
            WHERE t.name=:t AND s.name=:s;
        """), {"t": table, "s": schema}).fetchone() is not None
        if not exists:
            conn.execute(text(f"""
            CREATE TABLE {(f'[{db}].' if db else '')}[{schema}].[{table}] (
              [id]               VARCHAR(64)  NOT NULL,
              [city]             NVARCHAR(32) NULL,
              [company_name]     NVARCHAR(256) NULL,
              [industry]         NVARCHAR(128) NULL,
              [company_size]     NVARCHAR(64)  NULL,
              [contact]          NVARCHAR(64)  NULL,
              [address_raw]      NVARCHAR(512) NULL,
              [address_norm]     NVARCHAR(512) NULL,
              [lng]              DECIMAL(11,6) NULL,
              [lat]              DECIMAL(11,6) NULL,
              [company_url]      NVARCHAR(512) NULL,
              [job_url]          NVARCHAR(512) NULL,
              [source]           NVARCHAR(64)  NOT NULL DEFAULT N'zhilian',
              [out_of_city]      BIT           NOT NULL DEFAULT 0,
              [first_seen_at]    DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
              [last_seen_at]     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
              [seen_count]       INT           NOT NULL DEFAULT 1,
              CONSTRAINT [PK_spyder_zhilian] PRIMARY KEY CLUSTERED ([id])
            );
            """))
        for col, spec in [
            ("industry","NVARCHAR(128) NULL"),
            ("company_size","NVARCHAR(64) NULL"),
            ("contact","NVARCHAR(64) NULL"),
        ]:
            conn.execute(text(f"""
            IF COL_LENGTH(N'{(f'[{db}].' if db else '')}[{schema}].[{table}]', '{col}') IS NULL
            BEGIN
              ALTER TABLE {(f'[{db}].' if db else '')}[{schema}].[{table}] ADD [{col}] {spec};
            END
            """))
        conn.execute(text(f"""
        IF NOT EXISTS (
          SELECT 1 FROM {qp}sys.indexes
          WHERE name = N'UX_spyder_zhilian_url_addr'
            AND object_id = OBJECT_ID(N'{(f'[{db}].' if db else '')}[{schema}].[{table}]')
        )
        BEGIN
          CREATE UNIQUE INDEX UX_spyder_zhilian_url_addr
            ON {(f'[{db}].' if db else '')}[{schema}].[{table}] ([company_url], [address_norm]);
        END
        """))

MERGE_SQL = """
MERGE {full} AS T
USING (VALUES
 (:id,:city,:company_name,:industry,:company_size,:contact,:address_raw,:address_norm,
  :lng,:lat,:company_url,:job_url,:source,:out_of_city)
) AS S([id],[city],[company_name],[industry],[company_size],[contact],[address_raw],[address_norm],
       [lng],[lat],[company_url],[job_url],[source],[out_of_city])
ON T.[id]=S.[id]
WHEN MATCHED THEN UPDATE SET
  [city]=S.[city],
  [company_name]=S.[company_name],
  [industry]=S.[industry],
  [company_size]=S.[company_size],
  [contact]=S.[contact],
  [address_raw]=S.[address_raw],
  [address_norm]=S.[address_norm],
  [lng]=S.[lng],
  [lat]=S.[lat],
  [company_url]=S.[company_url],
  [job_url]=S.[job_url],
  [out_of_city]=S.[out_of_city],
  [last_seen_at]=SYSUTCDATETIME(),
  [seen_count]=T.[seen_count]+1
WHEN NOT MATCHED THEN INSERT (
  [id],[city],[company_name],[industry],[company_size],[contact],[address_raw],[address_norm],
  [lng],[lat],[company_url],[job_url],[source],[out_of_city],[first_seen_at],[last_seen_at],[seen_count]
) VALUES (
  S.[id],S.[city],S.[company_name],S.[industry],S.[company_size],S.[contact],S.[address_raw],S.[address_norm],
  S.[lng],S.[lat],S.[company_url],S.[job_url],S.[source],S.[out_of_city],SYSUTCDATETIME(),SYSUTCDATETIME(),1
);
"""

def upsert(engine: Engine, rec: Dict, full_name: str):
    sql = MERGE_SQL.format(full=f"[{full_name.replace('.', '].[')}]")
    with engine.begin() as conn:
        conn.execute(text(sql), rec)
def load_cookies_from_txt(path: str) -> List[Dict]:
    cookies = []
    if not os.path.isfile(path): return cookies
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            parts = line.split("\t")
            if len(parts) < 7: continue
            domain, flag, pathv, secure, exp, name, value = parts[-7:]
            try:
                cookies.append({
                    "name": name, "value": value, "domain": domain,
                    "path": pathv or "/", "secure": True if str(secure).lower()=="true" else False,
                    "httpOnly": False, "sameSite": "Lax",
                    "expires": int(exp) if str(exp).isdigit() else -1
                })
            except Exception: pass
    return cookies

def load_cookies_from_json(path: str) -> List[Dict]:
    cookies = []
    if not os.path.isfile(path): return cookies
    try:
        data = json.load(open(path, "r", encoding="utf-8"))
        if isinstance(data, dict) and "cookies" in data: data = data["cookies"]
        if isinstance(data, list):
            for c in data:
                try:
                    cookies.append({
                        "name": c.get("name"), "value": c.get("value"),
                        "domain": c.get("domain"), "path": c.get("path") or "/",
                        "secure": bool(c.get("secure", False)),
                        "httpOnly": bool(c.get("httpOnly", False)),
                        "sameSite": "Lax",
                        "expires": int(c.get("expirationDate", -1)) if c.get("expirationDate") else -1
                    })
                except Exception: pass
    except Exception: pass
    return cookies

def add_cookies(ctx: BrowserContext, cookies: List[Dict]):
    filtered = [c for c in cookies if c.get("domain") and "zhaopin.com" in c.get("domain")]
    if not filtered: return
    ctx.add_cookies([{
        "name": c["name"], "value": c["value"], "domain": c.get("domain"),
        "path": c.get("path") or "/", "secure": c.get("secure", False),
        "httpOnly": c.get("httpOnly", False), "sameSite": "Lax",
        **({"expires": c["expires"]} if isinstance(c.get("expires"), int) and c["expires"]>0 else {})
    } for c in filtered])
def wait_idle(page: Page, ms: int = NETWORK_IDLE_MS):
    try: page.wait_for_load_state("networkidle", timeout=ms)
    except PWTimeoutError: pass

def get_toolbar_city(page: Page) -> str:
    try:
        t = page.locator(".content-s .content-s__item a.content-s__item__text").first.inner_text().strip()
        return normalize_text(t)
    except Exception:
        return ""

def ensure_city(page: Page, city: str) -> bool:
    city = normalize_text(city)
    try:
        current = get_toolbar_city(page)
        if normalize_city_name(current) == normalize_city_name(city):
            return True
    except Exception:
        pass
    try:
        page.locator(".content-s .content-s__item").first.click(timeout=5000)
        time.sleep(0.4)
        target = page.locator(f"a[title='{city}']")
        if target.count() == 0:
            target = page.locator(f"text={city}").first
        target.click(timeout=4000)
        wait_idle(page, 2500)
        current2 = get_toolbar_city(page)
        return normalize_city_name(current2) == normalize_city_name(city)
    except Exception as e:
        logger.warning(f"[城市切換] 失敗 {city}：{e}")
        return False

def build_page_url_like(cur_url: str, page_no: int) -> str:
    try:
        if "/p" in cur_url:
            return re.sub(r"/p\d+", f"/p{page_no}", cur_url)
        u = urlparse(cur_url)
        qs = parse_qs(u.query)
        qs["p"] = [str(page_no)]
        new_q = urlencode({k: v[0] if isinstance(v, list) else v for k, v in qs.items()})
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
    except Exception:
        sep = "&" if "?" in cur_url else "?"
        return f"{cur_url}{sep}p={page_no}"

def parse_card_list(page: Page) -> List[Dict]:
    out = []
    cards = page.locator(".joblist-box__item.clearfix")
    n = cards.count()
    if n == 0:
        cards = page.locator(".contentpile__content__wrapper__item")
        n = cards.count()
    for i in range(n):
        c = cards.nth(i)
        try:
            job_a = c.locator("a.jobinfo__name")
            job_url = job_a.get_attribute("href") if job_a.count() > 0 else ""
            comp_a = c.locator("a.companyinfo__name")
            company_url = comp_a.get_attribute("href") if comp_a.count() > 0 else ""
            company_name = normalize_text(comp_a.get_attribute("title") or (comp_a.inner_text() if comp_a.count() else ""))

            address = ""
            info_items = c.locator(".jobinfo__other-info .jobinfo__other-info-item")
            m = info_items.count()
            for j in range(m):
                item = info_items.nth(j)
                try:
                    if item.locator("img[src*='location']").count() > 0:
                        address = normalize_text(item.locator("span").first.inner_text())
                        break
                except Exception:
                    pass
            if not address and info_items.count() > 0:
                address = normalize_text(info_items.first.inner_text())

            industry = None; company_size = None
            tags = [normalize_text(t) for t in c.locator(".companyinfo__tag .joblist-box__item-tag").all_inner_texts()]
            for t in tags:
                if re.search(r"(人|人以上|以下|少于)", t): company_size = t
                else:
                    industry = t if not industry else (industry + " | " + t)

            contact = None
            try:
                t = normalize_text(c.locator(".companyinfo__staff-name").first.inner_text())
                if t:
                    contact = t.split("·")[0].strip()
            except Exception:
                pass

            card_city = ""
            try:
                if "·" in address:
                    card_city = address.split("·",1)[0].strip()
            except Exception:
                pass

            out.append({
                "job_url": job_url or "",
                "company_url": company_url or job_url or "",
                "company_name": company_name or "",
                "address_raw": address or "",
                "card_city": card_city or "",
                "industry": industry,
                "company_size": company_size,
                "contact": contact
            })
        except Exception:
            pass
    return out
def write_progress(path: str, city_name: str, next_page: int):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"city": city_name, "next_page": next_page, "ts": int(time.time())}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[PROGRESS] 寫入失敗：{e}")

def read_progress(path: str) -> Tuple[Optional[str], int]:
    try:
        if os.path.isfile(path):
            d = json.load(open(path, "r", encoding="utf-8"))
            return d.get("city"), int(d.get("next_page", 1))
    except Exception as e:
        logger.warning(f"[PROGRESS] 讀取失敗：{e}")
    return None, 1
def main():
    logger.info("=== 啟動：智聯 v2（列表抽取｜城市下拉｜即時入庫） ===")
    if RESET_RESUME and os.path.isfile(PROGRESS_PATH):
        os.remove(PROGRESS_PATH); logger.info("[PROGRESS] 已清除進度檔")

    engine = create_engine(MSSQL_URL, fast_executemany=True)
    ensure_table_and_columns(engine, TABLE_NAME)

    resume_city, resume_page = (None, 1)
    if RESUME:
        resume_city, resume_page = read_progress(PROGRESS_PATH)
        if resume_city:
            logger.info(f"[PROGRESS] 自動續跑：{resume_city} 第 {resume_page} 頁")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--lang=zh-CN,zh;q=0.9"
        ])
        context = browser.new_context(
            viewport={"width": 1380, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            java_script_enabled=True, ignore_https_errors=True, locale="zh-CN",
        )
        ck_count = 0
        try:
            if os.path.isfile(COOKIES_TXT):
                cks = load_cookies_from_txt(COOKIES_TXT); add_cookies(context, cks); ck_count += len(cks)
            if os.path.isfile(COOKIES_JSON):
                cks = load_cookies_from_json(COOKIES_JSON); add_cookies(context, cks); ck_count += len(cks)
        except Exception as e:
            logger.warning(f"[Cookies] 載入失敗：{e}")
        logger.info(f"[Cookies] 已載入 {ck_count} 條（僅 zhaopin.com 域名生效）")

        page = context.new_page()
        started = False if resume_city else True
        for city in CITIES_WHITELIST:
            if not started:
                if normalize_city_name(city) == normalize_city_name(resume_city):
                    started = True
                else:
                    logger.info(f"[跳過] 尚未到起點城市：{city}")
                    continue

            base_url = f"https://sou.zhaopin.com/?kw={quote_plus(QUERY)}&p=1"
            logger.info(f"== 城市：{city} ==")
            page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
            wait_idle(page, 2500)

            if not ensure_city(page, city):
                logger.warning(f"[城市] 切換失敗，嘗試直接解析當前城市列表（可能混入其他城市）")

            toolbar_city = get_toolbar_city(page)

            cur1 = page.url
            start_page = resume_page if (resume_city and normalize_city_name(resume_city)==normalize_city_name(city)) else 1

            for pn in range(start_page, MAX_PAGES+1):
                list_url = build_page_url_like(cur1, pn)
                logger.info(f"[列表] {city} 第 {pn}/{MAX_PAGES} 頁 → {list_url}")
                try:
                    page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
                except PWTimeoutError:
                    logger.warning("[列表] 打開超時 → 刷新重試一次")
                    try:
                        page.goto(list_url, wait_until="domcontentloaded", timeout=30000)
                    except PWTimeoutError:
                        logger.error("[列表] 重試仍超時，跳下一頁")
                        if RESUME: write_progress(PROGRESS_PATH, city, pn+1)
                        continue

                for _ in range(3):
                    page.mouse.wheel(0, random.randint(600, 1000)); time.sleep(random.uniform(0.4, 0.8))
                wait_idle(page, 2000)

                cards = parse_card_list(page)
                logger.info(f"[列表] {city} 本頁卡片數：{len(cards)}")
                if len(cards) == 0:
                    logger.warning("[列表] 未取到卡片，可能觸發驗證或需更強 cookies")
                    if RESUME: write_progress(PROGRESS_PATH, city, pn+1)
                    continue

                for it in cards:
                    addr_norm = normalize_text(it["address_raw"])
                    final_city = judge_city(it.get("card_city",""), toolbar_city, addr_norm) or city
                    out_of_city = 1 if final_city != city else 0
                    rec = {
                        "id": hash_id(it.get("company_url","") or it.get("job_url",""), addr_norm or (it.get("company_url","") or it.get("job_url",""))),
                        "city": final_city,
                        "company_name": it.get("company_name") or None,
                        "industry": it.get("industry"),
                        "company_size": it.get("company_size"),
                        "contact": it.get("contact"),
                        "address_raw": it.get("address_raw"),
                        "address_norm": addr_norm,
                        "lng": None, "lat": None,
                        "company_url": it.get("company_url"),
                        "job_url": it.get("job_url"),
                        "source": "zhilian",
                        "out_of_city": out_of_city
                    }
                    try:
                        upsert(create_engine(MSSQL_URL, fast_executemany=True), rec, TABLE_NAME)
                        logger.info(f"[入庫] {city} 成功：{rec['company_name'] or '未知公司'} | {rec['address_norm'] or '無地址'} | {rec['industry'] or '-'} | {rec['company_size'] or '-'} | {rec['contact'] or '-'}")
                    except Exception as e:
                        logger.error(f"[入庫] 失敗：{e}")

                if RESUME: write_progress(PROGRESS_PATH, city, pn+1)
                jitter_sleep()

        logger.info("=== 完成所有城市 ===")

        try: page.close()
        except Exception: pass
        try: context.close(); browser.close()
        except Exception: pass

if __name__ == "__main__":
    main()
