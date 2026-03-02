"""
update_proxies.py
- 下載免費 HTTP 代理清單（以 ProxyScrape 為主，附 GitHub 備援）
- 併合去重 → 併發驗證 → 只保留可用代理到 proxies.txt
建議：定期先跑一次本腳本，再跑主程式。
"""
import asyncio, random, re, sys
from typing import List, Set
import httpx
SOURCES = [

    "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=8000&country=all&format=text",

    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
]

OUT_FILE = "proxies.txt"
TEST_URL = "http://httpbin.org/ip"
CONCURRENCY = 80
TIMEOUT = 6.0

def parse_proxies(text: str) -> List[str]:
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        m = re.match(r"^(?:http://)?(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$", line)
        if m:
            out.append(f"http://{m.group(1)}:{m.group(2)}")
    return out

async def fetch_source(client: httpx.AsyncClient, url: str) -> List[str]:
    try:
        r = await client.get(url, timeout=15.0, follow_redirects=True)
        r.raise_for_status()
        return parse_proxies(r.text)
    except Exception:
        return []

async def check_one(proxy: str, client: httpx.AsyncClient) -> str | None:
    try:
        r = await client.get(TEST_URL, proxies={"http://": proxy, "https://": proxy}, timeout=TIMEOUT)
        if r.status_code == 200:
            return proxy
    except Exception:
        return None

async def main():
    print("[*] 下載免費代理清單…")
    all_proxies: Set[str] = set()
    async with httpx.AsyncClient() as client:
        lists = await asyncio.gather(*(fetch_source(client, u) for u in SOURCES))
    for lst in lists:
        all_proxies.update(lst)
    print(f"[*] 總計抓到 {len(all_proxies)} 條（未驗證）")

    if not all_proxies:
        print("[!] 沒抓到任何代理，請稍後再試或更換來源。")
        sys.exit(1)

    print("[*] 併發驗證中…")
    ok: List[str] = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        sem = asyncio.Semaphore(CONCURRENCY)
        async def run(p):
            async with sem:
                res = await check_one(p, client)
                if res:
                    ok.append(res)
        await asyncio.gather(*(run(p) for p in list(all_proxies)))


    random.shuffle(ok)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        for p in ok:
            f.write(p + "\n")

    print(f"[*] 可用代理：{len(ok)} → 已輸出 {OUT_FILE}")
    if len(ok) < 10:
        print("[!] 可用數量偏少，建議：降低抓取速率、增加來源或使用少量高品質節點作保底。")

if __name__ == "__main__":
    asyncio.run(main())
