"""
Boss直聘『室內設計』公司資訊抓取（全城市巡檢 v3）
新增：
- 可「續跑」三種方式：
  1) 簡單指定起點：START_CITY、START_PAGE
  2) 自動續跑：RESUME=true + PROGRESS_PATH（每頁落檔，下次自動接續）
  3) 預填已處理集合：PREFILL_SEEN_FROM_DB=true（從資料庫載入已抓過的 company_url，減少重訪詳情）
- 延續 v2 修正：僅在成功入庫後才標記 seen；列表/詳情分離 Context。
"""
import os, re, sys, time, json, random, hashlib, logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlencode

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page, TimeoutError as PWTimeoutError

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass
MSSQL_URL = os.getenv("MSSQL_URL", "mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/clean_data?driver=ODBC+Driver+17+for+SQL+Server")
TABLE_NAME = os.getenv("TABLE_NAME", "clean_data.dbo.spyder_boss")
MAX_PAGES = int(os.getenv("MAX_PAGES", "10"))
QUERY = os.getenv("QUERY", "室内设计")
CITIES_ENV = [c.strip() for c in os.getenv("CITIES", "").split(",") if c.strip()]
PROXY_LIST_ENV = os.getenv("PROXY_LIST", "").strip()
PROXIES: List[str] = [p.strip() for p in PROXY_LIST_ENV.split(",") if p.strip()]
DETAILS_PER_CONTEXT = int(os.getenv("DETAILS_PER_CONTEXT", "12"))
WAIT_MIN_MS = int(os.getenv("WAIT_MIN_MS", "700"))
WAIT_MAX_MS = int(os.getenv("WAIT_MAX_MS", "2100"))
SCROLL_PAUSE_MS = int(os.getenv("SCROLL_PAUSE_MS", "900"))
NETWORK_IDLE_MS = int(os.getenv("NETWORK_IDLE_MS", "1200"))
RETRY_TIMES_SOFT = int(os.getenv("RETRY_TIMES_SOFT", "2"))
RETRY_TIMES_HARD = int(os.getenv("RETRY_TIMES_HARD", "5"))
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
ALWAYS_VISIT_DETAIL = os.getenv("ALWAYS_VISIT_DETAIL", "false").lower() == "true"
START_CITY = os.getenv("START_CITY", "").strip()
START_PAGE = int(os.getenv("START_PAGE", "1").strip() or "1")
RESUME = os.getenv("RESUME", "false").lower() == "true"
PROGRESS_PATH = os.getenv("PROGRESS_PATH", "./data/resume.json")
RESET_RESUME = os.getenv("RESET_RESUME", "false").lower() == "true"
PREFILL_SEEN_FROM_DB = os.getenv("PREFILL_SEEN_FROM_DB", "false").lower() == "true"

DATA_DIR = os.getenv("DATA_DIR", "./data")
LOG_DIR = os.getenv("LOG_DIR", "./logs")
SESSION_DIR = os.getenv("SESSION_DIR", "./.sessions")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(SESSION_DIR, exist_ok=True)

CITY_CODE = {
    "北京":"101010100","上海":"101020100","天津":"101030100","重庆":"101040100",
    "武汉":"101200100","西安":"101110100","郑州":"101180100","南京":"101190100",
    "杭州":"101210100","合肥":"101220100","长沙":"101250100","成都":"101270100",
    "昆明":"101290100","沈阳":"101070100","南宁":"101300100",
    "广州":"101280100","深圳":"101280600","佛山":"101280800","东莞":"101281600",
    "中山":"101281700","珠海":"101280700","惠州":"101281100","汕头":"101280500",
    "肇庆":"101280901",
    "苏州":"101190400","无锡":"101190200","常州":"101191100","宁波":"101210400",
    "嘉兴":"101210300","镇江":"101190300","徐州":"101190800",
    "厦门":"101230200","莆田":"101230300",
    "大连":"101070200","烟台":"101120500","威海":"101121100",
    "北海":"101301300",
    "岳阳":"101251400","宜昌":"101200900","襄阳":"101200200",
    "太原":"101100100",
    "深圳市":"101280600","蘇州":"101190400","廣州":"101280100","廈門":"101230200",
    "寧波":"101210400","嘉興":"101210300","鎮江":"101190300","瀋陽":"101070100",
}
logger = logging.getLogger("bosszhipin_interior_all_v3")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt)
fh = logging.FileHandler(os.path.join(LOG_DIR, "spider_all_v3.log"), encoding="utf-8"); fh.setFormatter(fmt)
if not logger.handlers:
    logger.addHandler(ch); logger.addHandler(fh)

def jitter_sleep(min_ms=WAIT_MIN_MS, max_ms=WAIT_MAX_MS):
    import time
    t = random.randint(min_ms, max_ms) / 1000.0
    time.sleep(t)
def to_abs_url(href: str) -> str:
    if not href: return ""
    if href.startswith("http://") or href.startswith("https://"): return href
    return "https://www.zhipin.com" + href

def normalize_text(s: str) -> str:
    if not s: return ""
    import re
    return re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()

def hash_id(company_url: str, addr_norm: str) -> str:
    m = hashlib.md5(); m.update(f"{company_url}|{addr_norm}".encode("utf-8", "ignore")); return m.hexdigest()

ALIASES = {
    "上海市": "上海", "北京市": "北京", "廣州": "广州", "廣州市": "广州",
    "深圳市": "深圳", "杭州市": "杭州", "南京市": "南京", "蘇州": "苏州",
    "天津市": "天津", "瀋陽": "沈阳", "廈門": "厦门", "寧波": "宁波", "嘉興": "嘉兴", "鎮江": "镇江"
}
def normalize_city_name(city: str) -> str:
    city = normalize_text(city)
    return ALIASES.get(city, city)

def judge_city_from_sources(card_city: str, biz_region: str, address_text: str, whitelist: set) -> Optional[str]:
    for candidate in [card_city, biz_region]:
        c = normalize_city_name(candidate or "")
        if c in whitelist:
            return c
    for c in whitelist:
        if c in (address_text or ""):
            return c
    return None
from typing import Optional as _Optional
def _split_three_part(name: str) -> Tuple[_Optional[str], str, str]:
    parts = [p for p in name.split(".") if p]
    if len(parts) == 3: return parts[0], parts[1], parts[2]
    if len(parts) == 2: return None, parts[0], parts[1]
    if len(parts) == 1: return None, "dbo", parts[0]
    return None, "dbo", "spyder_boss"

def ensure_table(engine: Engine, full_name: str):
    db, schema, table = _split_three_part(full_name)
    check_sql = f"""
    SELECT 1
    FROM {(f'[{db}].' if db else '')}sys.tables t
    JOIN {(f'[{db}].' if db else '')}sys.schemas s ON s.schema_id = t.schema_id
    WHERE t.name = :tname AND s.name = :sname;
    """
    with engine.begin() as conn:
        exists = conn.execute(text(check_sql), {"tname": table, "sname": schema}).fetchone() is not None
    if not exists:
        ddl = f"""
        CREATE TABLE {(f'[{db}].' if db else '')}[{schema}].[{table}] (
          [id]               VARCHAR(64)  NOT NULL,
          [city]             NVARCHAR(32) NULL,
          [company_name]     NVARCHAR(256) NULL,
          [legal_person]     NVARCHAR(128) NULL,
          [address_raw]      NVARCHAR(512) NULL,
          [address_norm]     NVARCHAR(512) NULL,
          [lng]              DECIMAL(11,6) NULL,
          [lat]              DECIMAL(11,6) NULL,
          [company_url]      NVARCHAR(512) NULL,
          [job_url]          NVARCHAR(512) NULL,
          [source]           NVARCHAR(64)  NOT NULL DEFAULT N'boss_zhipin',
          [out_of_city]      BIT           NOT NULL DEFAULT 0,
          [first_seen_at]    DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
          [last_seen_at]     DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME(),
          [seen_count]       INT           NOT NULL DEFAULT 1,
          [proxy_meta]       NVARCHAR(256) NULL,
          CONSTRAINT [PK_spyder_boss] PRIMARY KEY CLUSTERED ([id])
        );
        """
        with engine.begin() as conn:
            conn.execute(text(ddl))
    idx_sql = f"""
    IF NOT EXISTS (
      SELECT 1 FROM {(f'[{db}].' if db else '')}sys.indexes
      WHERE name = N'UX_spyder_boss_url_addr'
        AND object_id = OBJECT_ID(N'{(f'[{db}].' if db else '')}[{schema}].[{table}]')
    )
    BEGIN
      CREATE UNIQUE INDEX UX_spyder_boss_url_addr
        ON {(f'[{db}].' if db else '')}[{schema}].[{table}] ([company_url], [address_norm]);
    END
    """
    with engine.begin() as conn:
        conn.execute(text(idx_sql))

def prefill_seen_from_db(engine: Engine, full_table_name: str) -> set:
    sql = f"SELECT DISTINCT [company_url] FROM [{full_table_name.replace('.', '].[')}]"
    seen = set()
    try:
        with engine.begin() as conn:
            for (url,) in conn.execute(text(sql)):
                if url: seen.add(url)
    except Exception as e:
        logger.warning(f"[DB] 載入已處理 company_url 失敗：{e}")
    return seen

MERGE_SQL_TPL = """
MERGE {full} AS T
USING (VALUES
  (:id, :city, :company_name, :legal_person, :address_raw, :address_norm,
   :lng, :lat, :company_url, :job_url, :source, :out_of_city, :proxy_meta)
) AS S([id],[city],[company_name],[legal_person],[address_raw],[address_norm],
       [lng],[lat],[company_url],[job_url],[source],[out_of_city],[proxy_meta])
ON T.[id] = S.[id]
WHEN MATCHED THEN UPDATE SET
  [city] = S.[city],
  [company_name] = S.[company_name],
  [legal_person] = S.[legal_person],
  [address_raw] = S.[address_raw],
  [address_norm] = S.[address_norm],
  [lng] = S.[lng],
  [lat] = S.[lat],
  [company_url] = S.[company_url],
  [job_url] = S.[job_url],
  [out_of_city] = S.[out_of_city],
  [proxy_meta] = S.[proxy_meta],
  [last_seen_at] = SYSUTCDATETIME(),
  [seen_count] = T.[seen_count] + 1
WHEN NOT MATCHED THEN INSERT (
  [id],[city],[company_name],[legal_person],[address_raw],[address_norm],
  [lng],[lat],[company_url],[job_url],[source],[out_of_city],
  [first_seen_at],[last_seen_at],[seen_count],[proxy_meta]
) VALUES (
  S.[id],S.[city],S.[company_name],S.[legal_person],S.[address_raw],S.[address_norm],
  S.[lng],S.[lat],S.[company_url],S.[job_url],S.[source],S.[out_of_city],
  SYSUTCDATETIME(),SYSUTCDATETIME(),1,S.[proxy_meta]
);
"""

def upsert_record(engine: Engine, rec: Dict, full_table_name: str):
    sql = MERGE_SQL_TPL.format(full=f"[{full_table_name.replace('.', '].[')}]")
    with engine.begin() as conn:
        conn.execute(text(sql), rec)
def load_proxies() -> List[str]:
    proxies = list(PROXIES)
    if not proxies:
        pfile = os.path.join(os.getcwd(), "proxies.txt")
        if os.path.isfile(pfile):
            with open(pfile, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxies.append(line)
    return proxies

class ProxyManager:
    def __init__(self, proxies: List[str]):
        self.proxies = proxies or []
        self.idx = 0
        random.shuffle(self.proxies)

    def pick(self) -> Optional[Dict]:
        if not self.proxies:
            return None
        p = self.proxies[self.idx % len(self.proxies)]
        self.idx += 1
        m = re.match(r"^(?P<scheme>https?|socks5)://(?:(?P<u>[^:]+):(?P<p>[^@]+)@)?(?P<h>[^:]+):(?P<port>\d+)$", p)
        if not m:
            return None
        d = m.groupdict()
        conf = {"server": f"{d['scheme']}://{d['h']}:{d['port']}"}
        if d.get("u") and d.get("p"):
            conf["username"] = d["u"]; conf["password"] = d["p"]
        return conf
def new_context(browser: Browser, proxy_conf: Optional[Dict]) -> BrowserContext:
    w = random.randint(1280, 1600); h = random.randint(800, 1000)
    args = dict(
        viewport={"width": w, "height": h},
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"),
        java_script_enabled=True,
        ignore_https_errors=True,
        locale="zh-CN"
    )
    if proxy_conf:
        args["proxy"] = proxy_conf
    return browser.new_context(**args)

def random_human_scroll(page: Page, steps: int = 5):
    for _ in range(steps):
        page.mouse.wheel(0, random.randint(400, 1000))
        jitter_sleep(SCROLL_PAUSE_MS, SCROLL_PAUSE_MS + 600)

def wait_for_cards_stable(page: Page, timeout_ms: int = 8000) -> bool:
    t0 = time.time(); last_cnt = -1; stable_times = 0
    while (time.time() - t0) * 1000 < timeout_ms:
        try:
            cnt = page.locator(".card-area .job-card-wrap .job-card-box").count()
            if cnt == last_cnt and cnt > 0: stable_times += 1
            else: stable_times = 0
            last_cnt = cnt
            if stable_times >= 2: return True
        except Exception: pass
        jitter_sleep(500, 900)
    return False

def wait_for_network_idle(page: Page, timeout_ms: int = NETWORK_IDLE_MS):
    try: page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PWTimeoutError: pass
def parse_list_cards(page: Page) -> List[Dict]:
    cards = page.locator(".card-area .job-card-wrap .job-card-box")
    n = cards.count(); out = []
    for i in range(n):
        c = cards.nth(i)
        try:
            ch = c.locator(".job-card-footer a.boss-info").get_attribute("href")
            company_url = to_abs_url(ch or "")
            loc_text = normalize_text(c.locator(".job-card-footer .company-location").inner_text())
            card_city = normalize_text(loc_text.split("·")[0] if "·" in loc_text else loc_text)
            jh = c.locator(".job-info .job-title a.job-name").get_attribute("href")
            job_url = to_abs_url(jh or "")
            if company_url: out.append({"company_url": company_url, "job_url": job_url, "card_city": card_city})
        except Exception as e:
            logger.warning(f"[列表解析] 卡片第 {i} 解析失敗：{e}")
    return out

def expand_company_business(page: Page) -> None:
    try:
        lab = page.locator("label[ka='company_full_info']")
        if lab.count() > 0 and lab.first.is_visible(): lab.first.click(); jitter_sleep(); return
        btns = page.locator("label:has-text('查看更多信息')")
        if btns.count() > 0 and btns.first.is_visible(): btns.first.click(); jitter_sleep()
    except Exception: pass

def extract_business_info(page: Page) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    name = legal = region = None
    try:
        sec = page.locator(".job-sec.company-business")
        if sec.count() == 0: return name, legal, region
        ul = sec.locator("ul"); lis = ul.locator("li")
        for i in range(lis.count()):
            text = normalize_text(lis.nth(i).inner_text())
            if text.startswith("企业名称：") or text.startswith("企業名稱："): name = text.split("：", 1)[1].strip()
            elif text.startswith("法定代表人："): legal = text.split("：", 1)[1].strip()
            elif text.startswith("所属地区：") or text.startswith("所屬地區："): region = text.split("：", 1)[1].strip()
    except Exception as e:
        logger.warning(f"[工商信息] 解析失敗：{e}")
    return name, legal, region

def extract_addresses(page: Page) -> List[Dict]:
    out = []
    try:
        sec_candidates = page.locator(".job-sec").filter(has_text="公司地址")
        if sec_candidates.count() == 0: return out
        sec = sec_candidates.first
        more = sec.locator(".location-item .more-view")
        for i in range(more.count()):
            try:
                el = more.nth(i)
                if el.is_visible(): el.click(); jitter_sleep(400, 900)
            except Exception: pass
        items = sec.locator(".job-location .location-item")
        for i in range(items.count()):
            it = items.nth(i)
            addr = normalize_text(it.locator(".location-address").inner_text())
            lng = lat = None
            try:
                m = it.locator(".map-container").first
                data_lat = m.get_attribute("data-lat") or ""
                if "," in data_lat:
                    parts = data_lat.split(",")
                    if len(parts) == 2: lng = float(parts[0].strip()); lat = float(parts[1].strip())
            except Exception: pass
            out.append({"address_raw": addr, "lng": lng, "lat": lat})
    except Exception as e:
        logger.warning(f"[公司地址] 解析失敗：{e}")
    return out
def write_progress(city_name: str, next_page: int):
    try:
        with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump({"city": city_name, "next_page": next_page, "ts": int(time.time())}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[PROGRESS] 寫入進度失敗：{e}")

def read_progress() -> Tuple[Optional[str], int]:
    try:
        if os.path.isfile(PROGRESS_PATH):
            with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
                d = json.load(f)
                return d.get("city"), int(d.get("next_page", 1))
    except Exception as e:
        logger.warning(f"[PROGRESS] 讀取進度失敗：{e}")
    return None, 1
def build_city_start_url(city_code: str, page_num: int = 1) -> str:
    base = "https://www.zhipin.com/web/geek/job"
    qs = {"query": QUERY, "city": city_code, "page": str(page_num)}
    return base + "?" + urlencode(qs, doseq=True)

def main():
    logger.info("=== 啟動：全城市巡檢 v3（續跑支援）===")
    if RESET_RESUME and os.path.isfile(PROGRESS_PATH):
        os.remove(PROGRESS_PATH)
        logger.info("[PROGRESS] 已清除進度檔")

    engine = create_engine(MSSQL_URL, fast_executemany=True)
    ensure_table(engine, TABLE_NAME)

    proxies = load_proxies(); proxy_mgr = ProxyManager(proxies)
    logger.info(f"代理總數：{len(proxies)}（可為0=直連）")
    if CITIES_ENV:
        city_list = CITIES_ENV
    else:
        tmp = set()
        for k in CITY_CODE.keys():
            tmp.add(normalize_city_name(k))
        city_list = sorted(tmp)
    CITIES_WHITELIST = set(city_list)
    resume_city, resume_page = (None, 1)
    if RESUME:
        resume_city, resume_page = read_progress()
        if resume_city:
            logger.info(f"[PROGRESS] 自動續跑：{resume_city} 第 {resume_page} 頁")
    start_city = START_CITY or resume_city or ""
    start_page = START_PAGE if START_CITY else (resume_page if resume_city else 1)
    started = False if start_city else True
    seen_company_urls_global = set()
    if PREFILL_SEEN_FROM_DB:
        logger.info("[DB] 正在從資料庫預填已處理 company_url …")
        seen_company_urls_global = prefill_seen_from_db(engine, TABLE_NAME)
        logger.info(f"[DB] 預填完成：{len(seen_company_urls_global)} 條")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-gpu",
                "--lang=zh-CN,zh;q=0.9",
            ]
        )

        try:
            for city_name in city_list:
                if not started:
                    if normalize_city_name(city_name) == normalize_city_name(start_city):
                        started = True
                    else:
                        logger.info(f"[跳過] 尚未到起點城市：{city_name}")
                        continue
                list_ctx = new_context(browser, proxy_mgr.pick())
                page_list = list_ctx.new_page()
                details_in_ctx = 0
                detail_ctx = new_context(browser, proxy_mgr.pick())
                city_code = CITY_CODE.get(city_name)
                if not city_code:
                    for k, v in CITY_CODE.items():
                        if normalize_city_name(k) == city_name:
                            city_code = v; break
                if not city_code:
                    logger.warning(f"[城市] 找不到城市碼：{city_name}，略過。")
                    try: list_ctx.close()
                    except Exception: pass
                    continue

                logger.info(f"== 城市：{city_name}（{city_code}） ==")
                current_page_num = start_page if city_name == start_city and start_page > 1 else 1
                start_page = 1

                while current_page_num <= MAX_PAGES:
                    start_url = build_city_start_url(city_code, current_page_num)
                    logger.info(f"[列表] {city_name} 第 {current_page_num}/{MAX_PAGES} 頁 → {start_url}")

                    try:
                        page_list.goto(start_url, wait_until="domcontentloaded", timeout=30000)
                    except PWTimeoutError:
                        logger.warning("[列表] 打開超時 → 重新建立列表Context並重試一次")
                        try: list_ctx.close()
                        except Exception: pass
                        list_ctx = new_context(browser, proxy_mgr.pick())
                        page_list = list_ctx.new_page()
                        try:
                            page_list.goto(start_url, wait_until="domcontentloaded", timeout=30000)
                        except PWTimeoutError:
                            logger.error("[列表] 重試仍超時，跳到下一頁")
                            current_page_num += 1
                            if RESUME: write_progress(city_name, current_page_num)
                            continue

                    random_human_scroll(page_list, steps=3)
                    wait_for_network_idle(page_list)
                    wait_for_cards_stable(page_list, timeout_ms=10000)

                    results = parse_list_cards(page_list)
                    if ALWAYS_VISIT_DETAIL:
                        to_process = results; skipped = 0
                    else:
                        to_process = [r for r in results if r["company_url"] not in seen_company_urls_global]
                        skipped = len(results) - len(to_process)

                    logger.info(f"[列表] {city_name} 本頁卡片數：{len(results)} | 準備處理：{len(to_process)} | 已看過跳過：{skipped}")

                    for item in to_process:
                        company_url = item["company_url"]
                        job_url = item["job_url"]
                        card_city = item["card_city"]

                        if details_in_ctx >= DETAILS_PER_CONTEXT:
                            try: detail_ctx.close()
                            except Exception: pass
                            detail_ctx = new_context(browser, proxy_mgr.pick())
                            details_in_ctx = 0
                        details_in_ctx += 1

                        page_detail = detail_ctx.new_page()
                        proxy_meta = json.dumps(detail_ctx._options.get("proxy", {}) if hasattr(detail_ctx, "_options") else {}, ensure_ascii=False)

                        logger.info(f"[詳情] 打開公司頁：{company_url}")
                        try:
                            page_detail.goto(company_url, wait_until="domcontentloaded", timeout=30000)
                        except PWTimeoutError:
                            logger.warning("[詳情] 打開超時，略過（之後仍可於後續頁重試）")
                            page_detail.close()
                            continue

                        random_human_scroll(page_detail, steps=random.randint(2, 4))
                        wait_for_network_idle(page_detail)

                        for _ in range(RETRY_TIMES_SOFT + 1):
                            expand_company_business(page_detail)
                            company_name, legal_person, biz_region = extract_business_info(page_detail)
                            if company_name or legal_person or biz_region: break
                            jitter_sleep()

                        addresses = []
                        for _ in range(RETRY_TIMES_HARD):
                            addresses = extract_addresses(page_detail)
                            if addresses: break
                            random_human_scroll(page_detail, steps=1)
                            jitter_sleep(500, 1200)

                        if not addresses:
                            addresses = [{"address_raw": "", "lng": None, "lat": None}]

                        any_success = False
                        for addr_obj in addresses:
                            address_raw = addr_obj.get("address_raw") or ""
                            addr_norm = normalize_text(address_raw)
                            lng = addr_obj.get("lng"); lat = addr_obj.get("lat")

                            city_guess = judge_city_from_sources(card_city, biz_region, address_raw, set(city_list))
                            final_city = city_guess or city_name
                            out_of_city = 1 if final_city != city_name else 0

                            rec = {
                                "id": hash_id(company_url, addr_norm or company_url),
                                "city": final_city or None,
                                "company_name": company_name or None,
                                "legal_person": legal_person or None,
                                "address_raw": address_raw or None,
                                "address_norm": addr_norm or None,
                                "lng": lng, "lat": lat,
                                "company_url": company_url,
                                "job_url": job_url or None,
                                "source": "boss_zhipin",
                                "out_of_city": out_of_city,
                                "proxy_meta": proxy_meta
                            }
                            try:
                                upsert_record(engine, rec, TABLE_NAME)
                                any_success = True
                                logger.info(f"[入庫] {city_name} 成功：{rec['company_name'] or '未知公司'} | {rec['address_norm'] or '無地址'}")
                            except Exception as e:
                                logger.error(f"[入庫] 失敗：{e}")

                        page_detail.close()
                        jitter_sleep()

                        if any_success and not ALWAYS_VISIT_DETAIL:
                            seen_company_urls_global.add(company_url)

                    current_page_num += 1
                    if RESUME: write_progress(city_name, current_page_num)
                if RESUME:
                    write_progress(city_name, MAX_PAGES + 1)
                try: detail_ctx.close()
                except Exception: pass
                try: list_ctx.close()
                except Exception: pass

        except KeyboardInterrupt:
            logger.warning("手動中止")
            try:
                if RESUME:
                    write_progress(locals().get("city_name", ""), locals().get("current_page_num", 1))
            except Exception:
                pass
        finally:
            try: browser.close()
            except Exception: pass

    logger.info("=== 結束：全城市巡檢 v3（可續跑）===")

if __name__ == "__main__":
    main()
