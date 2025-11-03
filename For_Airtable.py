#!/usr/bin/env python3
"""
Web Scraper App (Multi-Site + Scheduler)
----------------------------------------
✅ AlphaMinds, AquaTots, SoccerShots, MyGym, Generic sites
✅ Google Sheets integration (via config.json)
✅ Optional Airtable upload (uses REST API)
✅ Playwright JS rendering support
✅ Scheduler (runs sites based on 'timeframe' in config.json)
"""

import json
import time
import re
import os
import argparse
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from apscheduler.schedulers.background import BackgroundScheduler
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
USE_JS_RENDERING = True  # Enable Playwright rendering globally

# ---------------------------------------------------------------------
# PLAYWRIGHT FETCH (OPTIONAL JS RENDER)
# ---------------------------------------------------------------------
def fetch_html(url):
    """Fetches HTML with optional Playwright JS rendering."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/110.0.0.0 Safari/537.36"
        )
    }
    try:
        if USE_JS_RENDERING:
            from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.goto(url, wait_until="networkidle")
                html = page.content()
                browser.close()
                return html
        else:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return ""


def clean_text(text):
    return re.sub(r"\s+", " ", text or "").strip()


# ---------------------------------------------------------------------
# SCRAPER: AlphaMinds
# ---------------------------------------------------------------------
def scrape_alphaminds(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")
    records = []

    blocks = soup.find_all("div", class_="elementor-element")
    for block in blocks:
        if not block.get_text(strip=True):
            continue
        text = clean_text(block.get_text())
        if len(text) < 30:
            continue

        record = {
            "Program": "",
            "Age": "",
            "Days": "",
            "Times": "",
            "Description": text,
        }

        age_match = re.search(r"Ages?\s*[:\-]?\s*([\d\-–+ ]+)", text, re.I)
        if age_match:
            record["Age"] = age_match.group(1).strip()

        days_match = re.search(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*[^\n]*", text, re.I)
        if days_match:
            record["Days"] = days_match.group(0).strip()

        times_match = re.search(
            r"\d{1,2}[:.]\d{2}\s*(?:am|pm)?\s*[-–]\s*\d{1,2}[:.]\d{2}\s*(?:am|pm)?", text, re.I
        )
        if times_match:
            record["Times"] = times_match.group(0).strip()

        records.append(record)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------
# SCRAPER: AquaTots
# ---------------------------------------------------------------------
def scrape_aquatots(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    records = []
    rows = soup.select("table tr")
    if rows:
        headers = [clean_text(th.text) for th in rows[0].find_all("th")]
        for row in rows[1:]:
            cols = [clean_text(td.text) for td in row.find_all("td")]
            if len(cols) == len(headers):
                records.append(dict(zip(headers, cols)))
    else:
        blocks = soup.find_all(["p", "div"], string=re.compile("Age", re.I))
        for b in blocks:
            text = clean_text(b.text)
            record = {
                "Program": "",
                "Age": "",
                "Days": "",
                "Times": "",
                "Description": text,
            }
            records.append(record)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------
# SCRAPER: SoccerShots
# ---------------------------------------------------------------------
def scrape_soccershots(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    data = []
    for card in soup.select(".program-card"):
        title = clean_text(card.select_one(".program-title").text if card.select_one(".program-title") else "")
        desc = clean_text(card.select_one(".program-description").text if card.select_one(".program-description") else "")
        ages = re.search(r"Ages?\s*[:\-]?\s*([\d\-\+ ]+)", desc, re.I)
        record = {
            "Program": title,
            "Age": ages.group(1).strip() if ages else "",
            "Days": "",
            "Times": "",
            "Description": desc,
        }
        data.append(record)
    return pd.DataFrame(data)


# ---------------------------------------------------------------------
# SCRAPER: MyGym
# ---------------------------------------------------------------------
def scrape_mygym(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    data = []
    for block in soup.select(".class-item, .program-block, .view-content div"):
        text = clean_text(block.get_text())
        if not text or len(text) < 30:
            continue
        age = re.search(r"Ages?\s*[:\-]?\s*([\d\-\+ ]+)", text, re.I)
        data.append({
            "Program": "",
            "Age": age.group(1).strip() if age else "",
            "Days": "",
            "Times": "",
            "Description": text,
        })
    return pd.DataFrame(data)


# ---------------------------------------------------------------------
# SCRAPER: Generic (Fallback)
# ---------------------------------------------------------------------
def scrape_generic(url):
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    text_blocks = soup.find_all(["p", "div"], string=re.compile(r"Age|Class|Schedule", re.I))
    records = []
    for b in text_blocks:
        txt = clean_text(b.text)
        if len(txt) < 30:
            continue
        record = {
            "Program": "",
            "Age": "",
            "Days": "",
            "Times": "",
            "Description": txt,
        }
        records.append(record)
    return pd.DataFrame(records)


# ---------------------------------------------------------------------
# GOOGLE SHEETS UPLOAD
# ---------------------------------------------------------------------
def push_to_gsheet(df, sheet_name):
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).sheet1

        sheet.clear()
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"[INFO] ✅ Pushed {len(df)} rows to Google Sheet: {sheet_name}")
    except Exception as e:
        print(f"[ERROR] Google Sheets upload failed: {e}")


# ---------------------------------------------------------------------
# OPTIONAL AIRTABLE UPLOAD
# ---------------------------------------------------------------------
def upload_to_airtable(records):
    airtable_api_key = os.getenv("AIRTABLE_API_KEY") or "YOUR_AIRTABLE_API_KEY"
    airtable_base_id = os.getenv("AIRTABLE_BASE_ID") or "YOUR_BASE_ID"
    airtable_table_name = os.getenv("AIRTABLE_TABLE_NAME") or "YOUR_TABLE_NAME"

    if "YOUR_" in airtable_api_key:
        print("[INFO] Airtable credentials not set. Skipping upload.")
        return

    url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}"
    headers = {"Authorization": f"Bearer {airtable_api_key}", "Content-Type": "application/json"}

    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        payload = {"records": [{"fields": r} for r in batch]}
        try:
            resp = requests.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                print(f"[INFO] ✅ Uploaded batch {i // 10 + 1} to Airtable.")
            else:
                print(f"[WARN] Airtable upload failed: {resp.status_code} → {resp.text}")
        except Exception as e:
            print(f"[ERROR] Airtable upload error: {e}")


# ---------------------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------------------
SCRAPER_MAP = {
    "alphaminds": scrape_alphaminds,
    "aquatots": scrape_aquatots,
    "soccershots": scrape_soccershots,
    "mygym": scrape_mygym,
    "generic": scrape_generic,
}


def run_scraper(source, url, sheet_name):
    start = time.time()
    print(f"\n[INFO] Starting scrape → {source} | {url}")

    func = SCRAPER_MAP.get(source.lower(), scrape_generic)
    df = func(url)
    print(f"[INFO] Scraped {len(df)} records from {source} in {time.time() - start:.2f}s")

    if not df.empty:
        push_to_gsheet(df, sheet_name)
        upload_to_airtable(df.to_dict(orient="records"))

    print(f"[INFO] ✅ Completed {source} | ⏱ {time.time() - start:.2f}s total\n")


def main():
    with open("config.json") as f:
        config = json.load(f)

    for site in config.get("websites", []):
        run_scraper(site["name"], site["url"], site["sheet_name"])


# ---------------------------------------------------------------------
# SCHEDULER
# ---------------------------------------------------------------------
def schedule_all():
    with open("config.json") as f:
        config = json.load(f)

    scheduler = BackgroundScheduler()
    for site in config.get("websites", []):
        hours = float(site.get("timeframe", 6))
        scheduler.add_job(run_scraper, "interval", hours=hours,
                          kwargs={"source": site["name"], "url": site["url"], "sheet_name": site["sheet_name"]})
        print(f"[INFO] Scheduled {site['name']} every {hours}h")

    scheduler.start()
    print("[INFO] Scheduler started. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        scheduler.shutdown()
        print("[INFO] Scheduler stopped.")


# ---------------------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["once", "schedule"], default="once")
    args = parser.parse_args()

    if args.mode == "once":
        main()
    else:
        schedule_all()


