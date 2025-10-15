# (Full file — replace your current app.py with this)
import json
import time
import argparse
import re
from datetime import datetime
from dateutil import parser as dateparser
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd

USE_JS_RENDERING = False  # set True & install playwright if you need JS rendering

def fetch_html(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        if USE_JS_RENDERING:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, timeout=30000)
                page.wait_for_load_state("networkidle", timeout=15000)
                html = page.content()
                browser.close()
                return html
        else:
            resp = requests.get(url, timeout=20, headers=headers)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return ""

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    pages = cfg.get("pages") or cfg.get("websites") or []
    valid_pages = []
    for i, p in enumerate(pages):
        if not isinstance(p, dict):
            print(f"Config: skipping non-object entry #{i}")
            continue
        website = p.get("website"); url = p.get("url"); schema = p.get("schema")
        if not website or not url or not isinstance(schema, list):
            print(f"Config: skipping invalid page #{i}: website={website!r}, url={url!r}, schema_type={type(schema).__name__}")
            continue
        valid_pages.append({
            "website": website, "url": url, "schema": schema, "timeframe": p.get("timeframe", "7d")
        })
    cfg["websites"] = valid_pages
    return cfg

def parse_timeframe_to_seconds(tf: str) -> int:
    tf = str(tf).strip().lower()
    if tf.endswith("m"): return int(tf[:-1]) * 60
    if tf.endswith("h"): return int(tf[:-1]) * 3600
    if tf.endswith("d"): return int(tf[:-1]) * 86400
    try: return int(tf) * 86400
    except: return 7 * 86400

def to_mmddyyyy(value):
    if value is None: return "N/A"
    if isinstance(value, datetime): return value.strftime("%m/%d/%Y")
    try:
        dt = dateparser.parse(str(value))
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return "N/A"

def ensure_field(val):
    if val is None: return "N/A"
    s = str(val).strip()
    return s if s else "N/A"

# ----- NEW: heuristics for AgeGroup / Days / Times -----
WEEKDAY_WORDS = r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|thur|fri|sat|sun|weekend|weekends)"
AGE_PATTERNS = [
    r"ages?\s*\d+\s*[-–to]+\s*\d+\s*(?:yrs?|years|yo)?",   # ages 3-5
    r"ages?\s*\d+\+?",                                      # ages 3+
    r"age\s*[:\-]?\s*\d+\s*(?:months|month|yrs|years|yo)?",
    r"\b(infant|toddler|preschool|pre-school|baby|kids?|children|teen)\b",  # keywords
    r"\d+\s*(?:months|month)\b",
    r"\d+\s*(?:yrs|yrs\.|years|yo)\b"
]
TIME_PATTERNS = [
    r"\d{1,2}[:.]\d{2}\s*(?:am|pm|AM|PM)?(?:\s*-\s*\d{1,2}[:.]\d{2}\s*(?:am|pm)?)?",  # 9:30 AM - 10:15 AM
    r"\d{1,2}\s*(?:am|pm|AM|PM)\b",  # 9 AM
    r"\d{1,2}[:.]\d{2}\b",           # 09:30
]
AGE_REGEX = re.compile("|".join(AGE_PATTERNS), re.I)
TIME_REGEX = re.compile("|".join(TIME_PATTERNS))
DAY_REGEX = re.compile(WEEKDAY_WORDS, re.I)

def extract_age_day_time_from_text(text):
    """Search text for likely age-group, days (weekdays), and times. Returns (age, days, times)."""
    if not text:
        return "N/A", "N/A", "N/A"
    txt = re.sub(r'\s+', ' ', text)  # normalize whitespace
    # Ages
    age_match = AGE_REGEX.search(txt)
    age = age_match.group(0).strip() if age_match else "N/A"

    # Days: collect unique weekday mentions (preserve order)
    days_found = []
    for m in DAY_REGEX.finditer(txt):
        w = m.group(0).strip()
        if w.lower() not in [d.lower() for d in days_found]:
            days_found.append(w)
    days = ", ".join(days_found) if days_found else "N/A"

    # Times: find first reasonable time-like match (could be multiple)
    times_found = TIME_REGEX.findall(txt)
    # TIME_REGEX.findall returns tuples if pattern groups exist; normalize
    def normalize_match(m):
        if isinstance(m, tuple):
            for g in m:
                if g:
                    return g
            return ""
        return m
    times = []
    for m in times_found:
        nm = normalize_match(m)
        if nm and nm not in times:
            times.append(nm.strip())
    times_str = ", ".join(times) if times else "N/A"

    return age, days, times_str

def scrape_alphaminds(url, schema, website_name):
    """
    Improved AlphaMinds scraper:
    - Finds class headings (h3/h4)
    - Scans sibling nodes up to the next heading for AgeGroup, Days, Times, Description
    - Uses regex heuristics to extract times like '9:30 AM', '9:30-10:15', '9am', '14:00', etc.
    """
    html = fetch_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    # Time regex: matches 9:30, 9:30 AM, 9am, 09:30, 9:30-10:15, 9:30–10:15, 9:30 AM - 10:15 AM
    time_re = re.compile(
        r'((?:\d{1,2}[:.]\d{2}|\d{1,2})\s*(?:am|pm|AM|PM)?(?:\s*[–\-]\s*(?:\d{1,2}[:.]\d{2}|\d{1,2})\s*(?:am|pm|AM|PM)?)?)',
        re.IGNORECASE
    )
    # Age and days regex (already in your heuristics, but keep local quick checks)
    age_re = re.compile(r'(ages?\s*\d+\s*(?:[-–to]\s*\d+)?|\d+\s*(?:months|month|yrs|years)\b|infant|toddler|preschool|kids|children)', re.I)
    days_re = re.compile(r'(Mon|Tue|Wed|Thu|Fri|Sat|Sun|Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|weekend|weekends)', re.I)

    for heading in soup.find_all(["h3", "h4"]):
        class_type = ensure_field(heading.get_text(" ", strip=True))
        age = "N/A"
        days = "N/A"
        times = "N/A"
        description_parts = []

        # Scan siblings until next heading
        for sib in heading.find_next_siblings():
            if sib.name in ["h3", "h4"]:
                break
            txt = sib.get_text(" ", strip=True)
            if not txt:
                continue

            # Prefer explicit labeled fields if present
            # e.g., "Age Group: 6-8", "Days: Mon/Wed", "Times: 4:00-5:00pm"
            if "age group" in txt.lower() or txt.lower().startswith("age:") or "ages" in txt.lower():
                m = age_re.search(txt)
                if m:
                    age = m.group(0).strip()
            if "day" in txt.lower() or "days" in txt.lower() or re.search(r'\b(mon(day)?|tue|wed|thu|fri|sat|sun|weekend)\b', txt, re.I):
                # Aggregate distinct weekday mentions
                found_days = list(dict.fromkeys(days_re.findall(txt)))
                if found_days:
                    days = ", ".join(found_days)
            # Times: either labeled or pattern matches
            if "time" in txt.lower() or "when" in txt.lower() or time_re.search(txt):
                t_matches = [m.group(0).strip() for m in time_re.finditer(txt)]
                if t_matches:
                    times = ", ".join(dict.fromkeys(t_matches))  # unique preserve order

            # Collect description paragraphs / lists
            if sib.name in ["p", "ul", "ol", "div", "li"]:
                description_parts.append(txt)

        description = (" ".join(x for x in description_parts if x)).strip() or "N/A"

        # If we still don't have age/days/times, try to infer from the small local block (heading + following text)
        if age == "N/A" or days == "N/A" or times == "N/A":
            nearby_text = heading.get_text(" ", strip=True) + " " + " ".join(description_parts[:3])
            a_guess, d_guess, t_guess = extract_age_day_time_from_text(nearby_text)
            if age == "N/A" and a_guess != "N/A":
                age = a_guess
            if days == "N/A" and d_guess != "N/A":
                days = d_guess
            if times == "N/A" and t_guess != "N/A":
                times = t_guess

        # Build Title: prefer a short descriptive subtitle rather than duplicate ClassType
        first_sentence = (description.split(".")[0].strip() if description != "N/A" else "")
        title = f"{class_type} — {first_sentence}" if first_sentence and first_sentence.lower() not in class_type.lower() else class_type

        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassType": class_type,
            "AgeGroup": ensure_field(age),
            "Days": ensure_field(days),
            "Times": ensure_field(times),
            "Title": ensure_field(title),
            "Description": ensure_field(description),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})

    # Post-process: normalize and dedupe by ClassType
    if not items:
        return []

    df = pd.DataFrame(items)
    for col in df.columns:
        df[col] = df[col].map(ensure_field)
    df = df.drop_duplicates(subset=["ClassType"])
    return df.to_dict(orient="records")
def scrape_generic(url, schema, website_name):
    html = fetch_html(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    try:
        title = soup.title.string.strip() if soup.title else "N/A"
        # Use nearest <p> or a longer text snippet for description
        p = soup.find("p")
        snippet = p.get_text(" ", strip=True) if p else soup.get_text(" ", strip=True)[:300]
        # Heuristics from whole page to try find age/days/times
        age, days, times = extract_age_day_time_from_text(soup.get_text(" ", strip=True))
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassType": ensure_field(title),
            "AgeGroup": ensure_field(age),
            "Days": ensure_field(days),
            "Times": ensure_field(times),
            "Title": ensure_field(title),
            "Description": ensure_field(snippet),
            "ScrapeDate": to_mmddyyyy(datetime.now()),
        }
        items.append({k: row.get(k, "N/A") for k in schema})
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
    return items

def _extract_jsonld_objects(soup):
    objs = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            text = script.string or script.get_text()
            parsed = json.loads(text)
            if isinstance(parsed, list):
                objs.extend(parsed)
            else:
                objs.append(parsed)
        except Exception:
            continue
    return objs

def scrape_soccershots(url, schema, website_name):
    html = fetch_html(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Try JSON-LD first
    json_objs = _extract_jsonld_objects(soup)
    for obj in json_objs:
        graph = obj.get("itemListElement") or obj.get("@graph") or obj.get("events") or obj.get("offers")
        candidates = graph if isinstance(graph, list) else []
        for c in candidates:
            title = c.get("name") or c.get("title") or c.get("headline") or "N/A"
            desc = c.get("description") or ""
            date = c.get("startDate") or ""
            times = c.get("time") or ""
            age = "N/A"
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassType": ensure_field(title),
                "AgeGroup": ensure_field(age),
                "Days": ensure_field(date),
                "Times": ensure_field(times),
                "Title": ensure_field(title),
                "Description": ensure_field(desc),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
    if items:
        return items

    # Try DOM heuristics: locate candidate blocks
    selectors = ["div.result", "div.search-result", "div.search-listing", "li.result", "div.card", "div.listing", "div.item"]
    for sel in selectors:
        nodes = soup.select(sel)
        if not nodes:
            continue
        for node in nodes:
            title = node.find(["h2", "h3", "h4"])
            title_text = title.get_text(" ", strip=True) if title else (node.find("a").get_text(" ", strip=True) if node.find("a") else "N/A")
            snippet = node.get_text(" ", strip=True)
            age, days, times = extract_age_day_time_from_text(snippet)
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassType": ensure_field(title_text),
                "AgeGroup": ensure_field(age),
                "Days": ensure_field(days),
                "Times": ensure_field(times),
                "Title": ensure_field(title_text),
                "Description": ensure_field(snippet),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
        if items:
            return items

    # Anchor fallback
    anchors = soup.find_all("a", href=True)
    for a in anchors:
        txt = a.get_text(" ", strip=True)
        if not txt or len(txt) < 8 or re.search(r'login|register|contact|facebook|instagram', txt, re.I):
            continue
        age, days, times = extract_age_day_time_from_text(a.get("title") or a.get_text(" ", strip=True))
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassType": ensure_field(txt),
            "AgeGroup": ensure_field(age),
            "Days": ensure_field(days),
            "Times": ensure_field(times),
            "Title": ensure_field(txt),
            "Description": ensure_field(a.get("title") or ""),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})
        if len(items) >= 8:
            break
    if items:
        return items

    # Nothing found — diagnostics + generic fallback
    print(f"  [SoccerShots] no structured listings detected at {url}")
    snippet = soup.get_text(" ", strip=True)[:400]
    print(f"  [SoccerShots] page snippet: {snippet!r}")
    return scrape_generic(url, schema, website_name)

def scrape_mygym(url, schema, website_name):
    html = fetch_html(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    # Look for card/listing nodes
    nodes = soup.select("div.schedule-card, div.class-card, div.card, li.class-item, div.schedule")
    if not nodes:
        # fallback: search for headings and their following text
        headings = soup.find_all(["h2", "h3", "h4"])
        for h in headings:
            title = h.get_text(" ", strip=True)
            snippet = ""
            # look at next siblings within small range
            for sib in h.find_next_siblings(limit=6):
                snippet += " " + sib.get_text(" ", strip=True)
            age, days, times = extract_age_day_time_from_text(snippet or soup.get_text(" ", strip=True))
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassType": ensure_field(title),
                "AgeGroup": ensure_field(age),
                "Days": ensure_field(days),
                "Times": ensure_field(times),
                "Title": ensure_field(title),
                "Description": ensure_field(snippet.strip() or title),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
        return items if items else scrape_generic(url, schema, website_name)

    for node in nodes:
        title = node.find(["h2", "h3", "h4"])
        title_text = title.get_text(" ", strip=True) if title else (node.find("a").get_text(" ", strip=True) if node.find("a") else "N/A")
        snippet = node.get_text(" ", strip=True)
        age, days, times = extract_age_day_time_from_text(snippet)
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassType": ensure_field(title_text),
            "AgeGroup": ensure_field(age),
            "Days": ensure_field(days),
            "Times": ensure_field(times),
            "Title": ensure_field(title_text),
            "Description": ensure_field(snippet),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})
    return items if items else scrape_generic(url, schema, website_name)

def scrape_hisawyer(url, schema, website_name):
    html = fetch_html(url)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    nodes = soup.select("div.schedule-item, div.schedules, div.event, li.event")
    if not nodes:
        # try headings
        headings = soup.find_all(["h2","h3","h4"])
        for h in headings:
            title = h.get_text(" ", strip=True)
            snippet = ""
            for sib in h.find_next_siblings(limit=6):
                snippet += " " + sib.get_text(" ", strip=True)
            age, days, times = extract_age_day_time_from_text(snippet)
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassType": ensure_field(title),
                "AgeGroup": ensure_field(age),
                "Days": ensure_field(days),
                "Times": ensure_field(times),
                "Title": ensure_field(title),
                "Description": ensure_field(snippet.strip() or title),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
        return items if items else scrape_generic(url, schema, website_name)

    for node in nodes:
        title = node.find(["h2","h3","h4"]) or node.find("a")
        title_text = title.get_text(" ", strip=True) if title else "N/A"
        snippet = node.get_text(" ", strip=True)
        age, days, times = extract_age_day_time_from_text(snippet)
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassType": ensure_field(title_text),
            "AgeGroup": ensure_field(age),
            "Days": ensure_field(days),
            "Times": ensure_field(times),
            "Title": ensure_field(title_text),
            "Description": ensure_field(snippet),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})
    return items

SCRAPER_REGISTRY = {
    "alphamindsacademy": scrape_alphaminds,
    "soccershots": scrape_soccershots,
    "mygym": scrape_mygym,
    "hisawyer": scrape_hisawyer,
    "generic": scrape_generic,
}

def run_scrape_job(page_config, loader):
    website_name = page_config.get("website", "N/A")
    url = page_config.get("url", "N/A")
    print(f"[{datetime.now().isoformat()}] Running scrape for: {website_name} - {url}")
    schema = page_config.get("schema")
    if not isinstance(schema, list):
        print("  Invalid schema — skipping page.")
        return

    wl = (website_name or "").lower()
    try:
        if "alphamindsacademy" in wl or "alphaminds" in wl:
            items = SCRAPER_REGISTRY["alphamindsacademy"](url, schema, website_name)
        elif "soccershots" in wl or "hudsoncounty" in wl:
            items = SCRAPER_REGISTRY["soccershots"](url, schema, website_name)
        elif "mygym" in wl:
            items = SCRAPER_REGISTRY["mygym"](url, schema, website_name)
        elif "hisawyer" in wl:
            items = SCRAPER_REGISTRY["hisawyer"](url, schema, website_name)
        else:
            items = SCRAPER_REGISTRY.get(wl, SCRAPER_REGISTRY["generic"])(url, schema, website_name)
    except Exception as e:
        print(f"  Exception while scraping {website_name}: {e}")
        items = []

    if not items:
        print("  No items found.")
        return
    loader.append_rows(items, header=schema)
    print(f"  ✅ Inserted {len(items)} clean rows for {website_name}.")

class GoogleSheetLoader:
    def __init__(self, creds_json_path, sheet_name):
        self.creds_json_path = creds_json_path
        self.sheet_name = sheet_name
        self.client = None
        self.sheet = None
        self._connect()
    def _connect(self):
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_json_path, scope)
        self.client = gspread.authorize(creds)
        try:
            self.sheet = self.client.open(self.sheet_name).sheet1
        except gspread.SpreadsheetNotFound:
            ss = self.client.create(self.sheet_name)
            self.sheet = ss.sheet1
    def append_rows(self, rows, header=None):
        if not rows: return
        if header:
            try:
                existing = self.sheet.row_values(1)
            except Exception:
                existing = []
            if not existing or [c.strip() for c in existing] != header:
                try:
                    self.sheet.insert_row(header, index=1)
                except Exception:
                    pass
        batch_values = [[r.get(h, "N/A") for h in header] for r in rows]
        try:
            self.sheet.append_rows(batch_values)
        except Exception as e:
            print(f"  Failed to append rows to Google Sheet: {e}")

def schedule_all_jobs(config, loader, run_once=False):
    pages = config.get("pages", []) if "pages" in config else config.get("websites", [])
    if not pages:
        print("No pages found in config to schedule.")
        return
    scheduler = BackgroundScheduler()
    for page in pages:
        website = page.get("website", "N/A")
        url = page.get("url", "N/A")
        schema = page.get("schema")
        if not url or not isinstance(schema, list):
            print(f"Skipping invalid page entry: website={website!r}, url={url!r}")
            continue
        tf_seconds = parse_timeframe_to_seconds(page.get("timeframe", "7d"))
        if run_once:
            run_scrape_job(page, loader)
            continue
        job_id = (website + "_" + url)[:100]
        try:
            scheduler.add_job(run_scrape_job, 'interval', seconds=tf_seconds, args=[page, loader], id=job_id)
            print(f"Scheduled {website} ({page.get('timeframe','7d')}) every {tf_seconds}s.")
        except Exception as e:
            print(f"Failed to schedule job for {website} - {url}: {e}")
    if not run_once:
        scheduler.start()
        print("Scheduler started. Press Ctrl+C to exit.")
        try:
            while True:
                time.sleep(2)
        except (KeyboardInterrupt, SystemExit):
            print("Stopping scheduler...")
            scheduler.shutdown()

def main():
    parser = argparse.ArgumentParser(description="Web Scraper + Google Sheets Loader")
    parser.add_argument("--config", default="config.json", help="Path to config.json")
    parser.add_argument("--run-once", action="store_true", help="Run all scrapers once and exit")
    args = parser.parse_args()

    config = load_config(args.config)
    gs_conf = config.get("google_sheet", {})
    creds = gs_conf.get("credentials_json")
    sheet_name = gs_conf.get("sheet_name")
    if not creds or not sheet_name:
        print("Missing Google Sheets credentials or sheet name in config.json.")
        return
    loader = GoogleSheetLoader(creds, sheet_name)
    schedule_all_jobs(config, loader, run_once=args.run_once)

if __name__ == "__main__":
    main()
