import time
from datetime import datetime, timedelta
from dateutil import parser as dp
from urllib.parse import urlparse
from .utils import http_session, http_get, soup, join, get_logger
from .config import SUBPAGES, REQUEST_SLEEP, REPORT_KEYWORDS

log = get_logger("discover_reports")
RECENT_WINDOW = timedelta(days=500)

def _estimate_date(text: str):
    try: return dp.parse(text, fuzzy=True, default=datetime.utcnow())
    except Exception: return None

def _rank(ir_url: str, session) -> list[dict]:
    seen, results = set(), []
    def crawl(url):
        if url in seen: return
        seen.add(url)
        r = http_get(session, url)
        if not r: return
        doc = soup(r.text)
        for a in doc.select("a[href]"):
            href = join(url, a["href"])
            txt = (a.get_text(" ", strip=True) or "")
            ltxt = (txt + " " + href).lower()
            if any(k in ltxt for k in REPORT_KEYWORDS):
                neighborhood = " ".join(a.parent.stripped_strings)[:400].lower()
                dt = _estimate_date(neighborhood) or _estimate_date(txt)
                results.append({"title": txt, "href": href, "date": dt})
        for a in doc.select("a[href]"):
            href = join(url, a["href"])
            if urlparse(href).netloc != urlparse(ir_url).netloc: continue
            if any(sp in href.lower() for sp in SUBPAGES): crawl(href)
    crawl(ir_url)

    now = datetime.utcnow()
    scored = []
    for it in results:
        ext = (it["href"].split("?")[0].split("#")[0].split(".")[-1] or "").lower()
        typ = 90 if ext in ("pdf","ppt","pptx") else 60
        recency = 0
        if it["date"]:
            age = (now - it["date"]).days
            if age <= RECENT_WINDOW.days:
                recency = max(0, 100 - age//5)
        scored.append((typ + recency, it))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [i for _, i in scored]

def pick_latest(ir_url: str, max_items=3):
    s = http_session()
    ranked = _rank(ir_url, s)
    chosen = []
    by_day = {}
    for item in ranked:
        day = item["date"].date().isoformat() if item["date"] else "unknown"
        by_day.setdefault(day, []).append(item)
    # choose top date group (most recent with at least one artifact)
    for day in sorted(by_day.keys(), reverse=True):
        chosen = by_day[day][:max_items]
        break
    return {"candidates": ranked[:10], "chosen": chosen}