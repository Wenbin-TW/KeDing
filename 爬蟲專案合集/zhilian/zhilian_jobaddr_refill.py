
import os, re, sys, time, random, logging
from typing import List, Dict, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError, Page

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

MSSQL_URL = os.getenv("MSSQL_URL", "mssql+pyodbc://Tw0002:Ywb081688@192.168.1.119/clean_data?driver=ODBC+Driver+17+for+SQL+Server")
TABLE_NAME = os.getenv("TABLE_NAME", "clean_data.dbo.spyder_zhilian")
MODE = os.getenv("MODE", "MISSING").upper() 
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "1000"))
COOKIES_TXT = os.getenv("COOKIES_TXT", "./data/cookies_zhaopin.txt")
COOKIES_JSON = os.getenv("COOKIES_JSON", "./data/cookies_zhaopin.json")
HEADLESS = os.getenv("HEADLESS", "false").lower() == "true"

LOG_DIR = os.getenv("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("jobaddr_refill")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
ch = logging.StreamHandler(sys.stdout); ch.setFormatter(fmt)
fh = logging.FileHandler(os.path.join(LOG_DIR, "jobaddr_refill.log"), encoding="utf-8"); fh.setFormatter(fmt)
if not logger.handlers:
    logger.addHandler(ch); logger.addHandler(fh)

def normalize_text(s: str) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()

def ensure_columns(engine: Engine, full: str):
    """表不存在時不建；只『補欄位』 job_address_raw / job_address_norm"""
    def split_three(name: str):
        parts = [p for p in name.split(".") if p]
        if len(parts)==3: return parts[0], parts[1], parts[2]
        if len(parts)==2: return None, parts[0], parts[1]
        return None, "dbo", parts[0]
    db, schema, table = split_three(full)
    qp = (f'[{db}].' if db else '')
    with engine.begin() as conn:
        for col, spec in [("job_address_raw","NVARCHAR(512) NULL"),
                          ("job_address_norm","NVARCHAR(512) NULL")]:
            conn.execute(text(f"""
            IF COL_LENGTH(N'{(f'[{db}].' if db else '')}[{schema}].[{table}]', '{col}') IS NULL
            BEGIN
              ALTER TABLE {(f'[{db}].' if db else '')}[{schema}].[{table}] ADD [{col}] {spec};
            END
            """))

def load_cookies_from_txt(path: str):
    ck=[]
    try:
        if not os.path.isfile(path): return ck
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#"): continue
                parts=line.split("\t")
                if len(parts)<7: continue
                domain, flag, pth, secure, exp, name, value = parts[-7:]
                if "zhaopin.com" not in domain: continue
                ck.append({"name":name,"value":value,"domain":domain,"path":pth or "/","secure":str(secure).lower()=="true",
                           "httpOnly":False,"sameSite":"Lax"})
    except Exception: pass
    return ck

def load_cookies_from_json(path: str):
    ck=[]
    try:
        if not os.path.isfile(path): return ck
        import json
        data = json.load(open(path,"r",encoding="utf-8"))
        if isinstance(data, dict) and "cookies" in data: data = data["cookies"]
        if isinstance(data, list):
            for c in data:
                if "zhaopin.com" not in (c.get("domain") or ""): continue
                ck.append({"name":c.get("name"),"value":c.get("value"),"domain":c.get("domain"),
                           "path":c.get("path") or "/","secure":bool(c.get("secure",False)),
                           "httpOnly":bool(c.get("httpOnly",False)),"sameSite":"Lax"})
    except Exception: pass
    return ck

def fetch_targets(engine: Engine, full: str, mode: str, limit: int):
    cond = "WHERE job_url IS NOT NULL AND job_url <> ''"
    if mode == "MISSING":
        cond += " AND (job_address_norm IS NULL OR LTRIM(RTRIM(job_address_norm))='')"
    sql = f"""
    SELECT TOP ({limit})
           id, job_url
    FROM {full}
    {cond}
    ORDER BY last_seen_at ASC
    """
    with engine.begin() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [{"id": r.id, "job_url": r.job_url} for r in rows]

def update_address(engine: Engine, full: str, _id: str, raw: Optional[str], norm: Optional[str]):
    sql = f"""
    UPDATE {full}
    SET job_address_raw = :raw,
        job_address_norm = :norm,
        last_seen_at = SYSUTCDATETIME()
    WHERE id = :id
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"raw": raw, "norm": norm, "id": _id})

def extract_job_address(page: Page) -> Optional[str]:
    """主選擇器：.job-address__content .job-address__content-text
       後備：含「上班地址」「工作地址」的區塊"""
    try:
        loc = page.locator(".job-address__content .job-address__content-text")
        if loc.count() > 0:
            txt = normalize_text(loc.first.inner_text())
            txt = re.sub(r"^\s*[\uf0a0-\uf8ff]*", "", txt)
            return txt
    except Exception:
        pass
    try:
        blk = page.locator("section:has-text('上班地址'), section:has-text('工作地址'), div:has-text('上班地址'), div:has-text('工作地址')")
        if blk.count() > 0:
            t = normalize_text(blk.first.inner_text())
            m = re.search(r"(上班地址|工作地址)[：:]\s*([^\n]+)", t)
            if m: return normalize_text(m.group(2))
    except Exception:
        pass
    try:
        t = normalize_text(page.inner_text())
        m = re.search(r"(上班地址|工作地址)[：:]\s*([^\n]+)", t)
        if m: return normalize_text(m.group(2))
    except Exception:
        pass
    return None

def main():
    logger.info(f" 啟動：job_url 補抓工作地址（MODE={MODE}, LIMIT={BATCH_LIMIT}）")
    engine = create_engine(MSSQL_URL, fast_executemany=True)
    full = "[" + TABLE_NAME.replace(".", "].[") + "]"
    ensure_columns(engine, TABLE_NAME)

    targets = fetch_targets(engine, full, MODE, BATCH_LIMIT)
    if not targets:
        logger.info("沒有待處理的記錄（可能已全部補齊）。")
        return

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=[
            "--disable-blink-features=AutomationControlled","--no-sandbox","--lang=zh-CN,zh;q=0.9"
        ])
        context = browser.new_context(
            viewport={"width": 1366,"height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"),
            locale="zh-CN", ignore_https_errors=True, java_script_enabled=True
        )
        ck = 0
        try:
            c1 = load_cookies_from_txt(COOKIES_TXT); 
            c2 = load_cookies_from_json(COOKIES_JSON)
            if c1: context.add_cookies(c1); ck += len(c1)
            if c2: context.add_cookies(c2); ck += len(c2)
        except Exception as e:
            logger.warning(f"[Cookies] 載入失敗：{e}")
        if ck: logger.info(f"[Cookies] 已加載 {ck} 條（僅 zhaopin.com 生效）")

        page = context.new_page()
        done = 0

        for row in targets:
            _id = row["id"]; url = row["job_url"]
            if not url: continue
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except PWTimeoutError:
                logger.warning(f"[超時] 打不開：{url}")
                continue
            try:
                page.wait_for_selector(".job-address__content, .job-address__content-text", timeout=5000)
            except PWTimeoutError:
                pass
            for _ in range(2):
                page.mouse.wheel(0, random.randint(400, 900)); time.sleep(random.uniform(0.3,0.7))

            addr = extract_job_address(page)
            if addr:
                update_address(engine, full, _id, addr, normalize_text(addr))
                done += 1
                logger.info(f"[OK] 已回寫地址：{addr}")
            else:
                logger.info(f"[SKIP] 無地址：{url}")
            time.sleep(random.uniform(0.6, 1.4))

        try:
            page.close(); context.close(); browser.close()
        except Exception:
            pass

    logger.info(f" 完成：處理 {len(targets)} 條，成功 {done} 條 ")

if __name__ == "__main__":
    main()
