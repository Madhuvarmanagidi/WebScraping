import json
import time
import argparse
from datetime import datetime
from dateutil import parser as dateparser
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd

USE_JS_RENDERING = False  

def fetch_html(url):
    """Fetch page HTML; optionally use JS rendering."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }

    try:
        if USE_JS_RENDERING:
            # Placeholder for future JS rendering support
            # Example (future use):
            # from playwright.sync_api import sync_playwright
            # with sync_playwright() as p:
            #     browser = p.chromium.launch(headless=True)
            #     page = browser.new_page()
            #     page.goto(url)
            #     html = page.content()
            #     browser.close()
            #     return html
            raise NotImplementedError("JS rendering not yet enabled.")
        else:
            resp = requests.get(url, timeout=15, headers=headers)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return ""

def load_config(path="config.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def parse_timeframe_to_seconds(tf: str) -> int:
    tf = tf.strip().lower()
    if tf.endswith("m"):
        return int(tf[:-1]) * 60
    if tf.endswith("h"):
        return int(tf[:-1]) * 3600
    if tf.endswith("d"):
        return int(tf[:-1]) * 86400
    return int(tf) * 86400

def to_mmddyyyy(value):
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    try:
        dt = dateparser.parse(str(value))
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return "N/A"

def ensure_field(val):
    if val is None:
        return "N/A"
    s = str(val).strip()
    return s if s else "N/A"


def scrape_alphaminds(url, schema, website_name):
    html = fetch_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    try:
        for heading in soup.find_all(["h3", "h4"]):
            class_type = ensure_field(heading.get_text(strip=True))
            age, days = "N/A", "N/A"
            description_parts = []

            siblings = heading.find_next_siblings()
            for sib in siblings:
                if sib.name in ["h3", "h4"]:
                    break
                txt = sib.get_text(" ", strip=True)
                if "Age Group:" in txt:
                    age = txt.replace("Age Group:", "").strip()
                elif "Days:" in txt:
                    days = txt.replace("Days:", "").strip()
                elif sib.name in ["p", "ul", "ol"]:
                    description_parts.append(txt)

            description = (" ".join(description_parts)).strip()
            if not description:
                description = "N/A"

            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassType": class_type,
                "AgeGroup": age,
                "Days": days,
                "Title": class_type,
                "Description": description,
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})


        df = pd.DataFrame(items)

        if df.empty:
            return []

        main_desc = "N/A"
        header_rows = df[(df["AgeGroup"] == "N/A") & (df["Days"] == "N/A")]
        if not header_rows.empty:
            main_desc = header_rows["Description"].iloc[0]

        clean_df = df[df["AgeGroup"] != "N/A"].copy()

        clean_df["Description"] = clean_df["Description"].apply(
            lambda x: main_desc if x == "N/A" else x
        )

        clean_df = clean_df.drop_duplicates(subset=["ClassType"])

        clean_df = clean_df.applymap(ensure_field)

        items = clean_df.to_dict(orient="records")

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        items = []

    return items


def scrape_generic(url, schema, website_name):
    html = fetch_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    items = []

    try:
        title = soup.title.string.strip() if soup.title else "N/A"
        snippet = soup.find("p").get_text(strip=True) if soup.find("p") else "N/A"

        row = {
            "Website": website_name,
            "PageURL": url,
            "Title": ensure_field(title),
            "ContentSnippet": ensure_field(snippet),
            "ScrapeDate": to_mmddyyyy(datetime.now()),
        }
        items.append({k: row.get(k, "N/A") for k in schema})
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
    return items


SCRAPER_REGISTRY = {
    "alphamindsacademy": scrape_alphaminds,
    "generic": scrape_generic,
}


class GoogleSheetLoader:
    def __init__(self, creds_json_path, sheet_name):
        self.creds_json_path = creds_json_path
        self.sheet_name = sheet_name
        self.client = None
        self.sheet = None
        self._connect()

    def _connect(self):
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_json_path, scope)
        self.client = gspread.authorize(creds)
        try:
            self.sheet = self.client.open(self.sheet_name).sheet1
        except gspread.SpreadsheetNotFound:
            ss = self.client.create(self.sheet_name)
            self.sheet = ss.sheet1

    def append_rows(self, rows, header=None):
        if not rows:
            return
        if header:
            try:
                existing = self.sheet.row_values(1)
            except Exception:
                existing = []
            if not existing or [c.strip() for c in existing] != header:
                self.sheet.insert_row(header, index=1)
        batch_values = [[r.get(h, "N/A") for h in header] for r in rows]
        self.sheet.append_rows(batch_values)


def run_scrape_job(page_config, loader):
    print(f"[{datetime.now().isoformat()}] Running scrape for: {page_config.get('website','N/A')} - {page_config.get('url')}")
    website_lower = page_config.get("website", "").lower()

    if "alphamindsacademy" in website_lower:
        items = SCRAPER_REGISTRY["alphamindsacademy"](page_config["url"], page_config["schema"], page_config["website"])
    else:
        items = SCRAPER_REGISTRY["generic"](page_config["url"], page_config["schema"], page_config["website"])

    if not items:
        print("  No items found.")
        return

    header = page_config["schema"]
    loader.append_rows(items, header=header)
    print(f"  ✅ Inserted {len(items)} clean rows for {page_config['website']}.")

def schedule_all_jobs(config, loader, run_once=False):
    pages = config.get("pages", []) if "pages" in config else config.get("websites", [])
    scheduler = BackgroundScheduler()

    for page in pages:
        tf_seconds = parse_timeframe_to_seconds(page.get("timeframe", "7d"))
        if run_once:
            run_scrape_job(page, loader)
            continue
        job_id = (page.get("website", "site") + "_" + page.get("url", "url"))[:100]
        scheduler.add_job(run_scrape_job, 'interval', seconds=tf_seconds, args=[page, loader], id=job_id)
        print(f"Scheduled {page.get('website','site')} ({page['timeframe']}) every {tf_seconds}s.")

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
