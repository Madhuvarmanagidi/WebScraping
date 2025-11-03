# Updated app.py — extended extraction for AgeRange, ClassSize, ClassLength, DayOfWeek, ClassDescription, ClassTitle, ClassLocation
import json
import time
import argparse
import re
from datetime import datetime, timedelta
from dateutil import parser as dateparser
import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd

# Toggle global JS rendering (Playwright). You can set per-page hints in config as well.
USE_JS_RENDERING = True  # set True & install playwright if you need JS rendering

def fetch_html(url, use_js=None):
    """
    Fetch HTML. Uses Playwright if JS rendering is enabled.
    Now improved to wait for post-load DOM rendering on modern frameworks.
    Returns the final HTML string or empty string on failure.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            " AppleWebKit/537.36 (KHTML, like Gecko)"
            " Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        js = USE_JS_RENDERING if use_js is None else bool(use_js)
        if js:
            try:
                from playwright.sync_api import sync_playwright
            except Exception:
                print(
                    "⚠️ Playwright not available. Install it with: "
                    "'pip install playwright && playwright install chromium'"
                )
                raise

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                page.set_default_timeout(90000)

                print(f"[Playwright] Rendering JS page: {url}")
                page.goto(url, timeout=90000)
                page.wait_for_load_state("networkidle", timeout=60000)

                # ---- Smart extra waits by domain ----
                if "alphamindsacademy.com" in url:
                    # Wait for Elementor containers and headings to load
                    try:
                        page.wait_for_selector("h2, h3, .elementor-widget-container", timeout=30000)
                    except Exception:
                        pass

                elif "mainstreetsites.com" in url:
                    # Wait for dynamic class list widget
                    try:
                        page.wait_for_selector(".classlist, .schedule, #classlistWidget", timeout=45000)
                    except Exception:
                        pass

                elif "soccershots" in url or "hisawyer" in url:
                    try:
                        page.wait_for_selector("div, li, section", timeout=20000)
                    except Exception:
                        pass

                # Small delay: sometimes data arrives asynchronously
                time.sleep(3)

                html = page.content()
                browser.close()
                return html

        # --- Non-JS fallback ---
        resp = requests.get(url, timeout=25, headers=headers)
        resp.raise_for_status()
        return resp.text

    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
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
        # allow per-page JS hint: "use_js": true
        valid_pages.append({
            "website": website,
            "url": url,
            "schema": schema,
            "timeframe": p.get("timeframe", "7d"),
            "use_js": p.get("use_js", False)
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

# ----- Heuristics & regexes for new fields -----
WEEKDAY_WORDS = r"(monday|tuesday|wednesday|thursday|friday|saturday|sunday|mon|tue|wed|thu|thur|fri|sat|sun|weekend|weekends)"
AGE_PATTERNS = [
    # Prioritizing Grade levels (e.g., 'Grade 1 - 5', 'PreK 3') as seen on AlphaMinds
    r"\b(?:Grade|Grades|PreK|Pre-K)\s*[0-9]+(?:\s*[-\–—]\s*[0-9]+)?\s*(?:\-\s*[0-9]+)?\b",
    r"\b(?:ages?|age[:\s-]*)\s*\d{1,2}\s*(?:[-–to]+\s*\d{1,2})?\s*(?:yrs?|years|yo|mos?|months)?\b",
    r"\b\d{1,2}\s*(?:years|yrs|yr|yo|months|mos)\b",
    r"\b(infant|toddler|preschool|pre-school|baby|kids?|children|teen|adult)\b"
    r"\b[Kk]\s*[-–]?\s*\d*(?:st|nd|rd|th)?\b",
    r"\bGrade\s*\d+\b"
]
TIME_PATTERNS = [
    r"\d{1,2}[:.]\d{2}\s*(?:am|pm|AM|PM)?(?:\s*[–\-]\s*\d{1,2}[:.]\d{2}\s*(?:am|pm|AM|PM)?)?",  # 9:30 AM - 10:15 AM
    r"\d{1,2}\s*(?:am|pm|AM|PM)\b",  # 9 AM
    r"\d{1,2}[:.]\d{2}\b",           # 09:30
]
AGE_REGEX = re.compile("|".join(AGE_PATTERNS), re.I)
TIME_REGEX = re.compile("|".join(TIME_PATTERNS), re.I)
DAY_REGEX = re.compile(WEEKDAY_WORDS, re.I)

# class size & duration & address
CLASS_SIZE_REGEX = re.compile(r'\b(?:class size|capacity|max(?:imum)?(?: seats?)?|limit(?:ed)?|spots(?: available)?)[:\s]*\D*?(\d{1,3})\b', re.I)
DURATION_REGEX = re.compile(r'(\d{1,3}\s*(?:min(?:ute)?s?|mins?|minutes?|hr|hrs|hour|hours))', re.I)
ADDRESS_SELECTORS = ['address', 'addr', 'location', 'studio', 'venue', 'directions', 'location-name', 'map']

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
        lw = w.lower()
        if lw not in [d.lower() for d in days_found]:
            days_found.append(w.capitalize() if len(w) > 3 else w.capitalize())
    days = ", ".join(days_found) if days_found else "N/A"

    # Times: find matches
    times_found = [m.group(0).strip() for m in TIME_REGEX.finditer(txt)]
    times = ", ".join(dict.fromkeys(times_found)) if times_found else "N/A"

    return age, days, times

def extract_size_length_address(text, node=None, soup=None):
    """
    Best-effort extraction of class size, duration (string) and address.
    node: optional BeautifulSoup node to scope search; soup: full page.
    """
    size = "N/A"
    length = "N/A"
    address = "N/A"
    txt = (text or "")
    # size
    m = CLASS_SIZE_REGEX.search(txt)
    if m:
        size = m.group(1)
    # duration
    m2 = DURATION_REGEX.search(txt)
    if m2:
        length = m2.group(1).strip()
    # scoped node address
    if node is not None:
        addr_tag = node.find('address')
        if addr_tag:
            address = addr_tag.get_text(" ", strip=True)
        else:
            for sel in ADDRESS_SELECTORS:
                found = node.select(f'[class*="{sel}"], [id*="{sel}"]')
                if found:
                    address = " ".join(x.get_text(" ", strip=True) for x in found).strip()
                    if address:
                        break
    # global JSON-LD or page address
    if (not address or address == "N/A") and soup is not None:
        # JSON-LD
        for obj in _extract_jsonld_objects(soup):
            loc = None
            if isinstance(obj, dict):
                loc = obj.get('location') or obj.get('address') or obj.get('location') or obj.get('venue')
            if isinstance(loc, dict):
                parts = []
                for k in ('streetAddress','addressLocality','postalCode','addressRegion','addressCountry','name'):
                    if loc.get(k):
                        parts.append(str(loc.get(k)))
                if parts:
                    address = ", ".join(parts)
                    break
            elif isinstance(loc, str):
                address = loc
                break
        if (not address or address == "N/A"):
            addr_tag = soup.find('address')
            if addr_tag:
                address = addr_tag.get_text(" ", strip=True)
            else:
                # search common classes globally
                for sel in ADDRESS_SELECTORS:
                    found = soup.select(f'[class*="{sel}"], [id*="{sel}"]')
                    if found:
                        address = " ".join(x.get_text(" ", strip=True) for x in found)[:300].strip()
                        if address:
                            break
    return ensure_field(size), ensure_field(length), ensure_field(address)

def parse_time_range_to_duration(timestr):
    """
    Given a timestring like '9:30 AM - 10:15 AM', try to compute duration.
    Strictly checks for time components to prevent interpreting grades/numbers as time.
    """
    if not timestr or timestr == "N/A":
        return "N/A"
    
    # Must contain a time component (colon or AM/PM) AND a separator
    if not re.search(r'(:|\s(?:am|pm))\s*[–—\-]\s*(\d{1,2})', timestr, re.I):
        return "N/A"
    
    # find two time tokens separated by a dash or 'to'
    parts = re.split(r'\s*[–—\-]\s*|\s+to\s+', timestr)
    
    # We require exactly two parts for a range calculation
    if len(parts) < 2:
        return "N/A"
        
    try:
        t0 = dateparser.parse(parts[0], default=datetime(2000,1,1))
        t1 = dateparser.parse(parts[1], default=datetime(2000,1,1))
        
        # handle midnight-wrap
        if t1 < t0:
            t1 = t1 + timedelta(days=1)
            
        diff = t1 - t0
        
        # Guard against illogical or massive durations (Max 6 hours duration limit)
        if diff.total_seconds() <= 0 or diff.total_seconds() > (6 * 3600): 
             return "N/A"

        mins = int(diff.total_seconds() // 60)
        if mins < 60:
            return f"{mins}m"
        else:
            h = mins // 60
            m = mins % 60
            return f"{h}h{m}m" if m else f"{h}h"
    except Exception:
        return "N/A"

# --- New Aquatots Scraper ---
def scrape_aquatots(url, schema, website_name, use_js=False):
    html = fetch_html(url, use_js=use_js)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    
    # 1. Class Title/Type (Often in H1 or Title)
    title = soup.title.string.replace(" | Aqua-Tots Swim Schools", "").strip() if soup.title else "N/A"
    
    # 2. Extract details from the prominent level description area
    level_tags = soup.select('.swim-level-detail')
    
    if not level_tags:
        # Fallback to generic page scrape if structure not found
        return scrape_generic(url, schema, website_name, use_js=use_js)

    for level_tag in level_tags:
        class_type_tag = level_tag.select_one('h2, h3')
        class_type = class_type_tag.get_text(" ", strip=True) if class_type_tag else title
        
        description_tag = level_tag.select_one('p')
        description = description_tag.get_text(" ", strip=True) if description_tag else "N/A"

        full_text = level_tag.get_text(" ", strip=True)
        
        # Aggressive Age Extraction using global regex
        age = "N/A"
        age_match = AGE_REGEX.search(full_text)
        if age_match:
            age = age_match.group(0).strip()
            
        # AquaTots pages typically don't list specific times/days/locations/size on these level pages
        
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassTitle": ensure_field(title),
            "ClassType": ensure_field(class_type),
            "AgeRange": ensure_field(age),
            "ClassSize": "N/A", 
            "ClassLength": "N/A", 
            "DayOfWeek": "N/A",
            "Times": "N/A",
            "ClassDescription": ensure_field(description),
            "ClassLocation": "General/Online (Check local branch for address)",
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})
        
    return items


def scrape_alphaminds(url, schema, website_name, use_js=False):
    html = fetch_html(url, use_js=use_js)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    
    # --- ENHANCED FILTERING LIST (Targeting Irrelevant Headers) ---
    SKIP_KEYWORDS = [
        "testimonial", "teacher", "contact us", "quick link", "registration", 
        "why choose", "core strengths", "your message has been sent", 
        "programs:", "jeremiah hosea", "rilwan ameen", "anthony kozikowsky", 
        "george gawargi", "ivette garcia", "matthew rydell", "svetlana margoulis", 
        "vera vitali", "stay in touch", "classes", "read more", "enroll",
        "private lessons", "individual instruction", "small group instruction"
    ]

    time_re = re.compile(r'((?:\d{1,2}[:.]\d{2}|\d{1,2})\s*(?:am|pm|AM|PM)?(?:\s*[–\-]\s*(?:\d{1,2}[:.]\d{2}|\d{1,2})\s*(?:am|pm|AM|PM)?)?)', re.I)

    for heading in soup.find_all(["h2", "h3", "h4"]):
        class_type = ensure_field(heading.get_text(" ", strip=True))
        lower_class_type = class_type.lower()
        
        # 1. Skip based on explicit keywords
        if any(kw in lower_class_type for kw in SKIP_KEYWORDS):
            continue
            
        # 2. Skip if the title is too short and non-descriptive
        # if len(class_type.split()) < 2 and class_type not in ["Classes Offered", "All Age Groups"]:
            # continue

        age = "N/A"; days = "N/A"; times = "N/A"; description_parts = []
        class_size = "N/A"; class_length = "N/A"; class_location = "N/A"
        
        # Start collecting text from siblings
        for sib in heading.find_next_siblings():
            if sib.name in ["h2", "h3", "h4"]:
                break
            
            # Stop if we hit a registration button or call-to-action divider (common on this site)
            if sib.name == "div" and sib.find(["a", "button"], string=re.compile("register", re.I)):
                break
            
            # Skip WordPress interactivity scripts if they appear in the raw text
            if sib.name == "script" or (sib.get("id") and "wp-interactivity-store" in sib.get("id")):
                 continue
            
            txt = sib.get_text(" ", strip=True)
            if not txt:
                continue
                
            low = txt.lower()
            
            # --- AlphaMinds Specific Extractions (Prioritized) ---
            if age == "N/A":
                age_line_match = re.search(r'(Age Group:.*?)\s*(Days:|$)', txt, re.I | re.DOTALL)
                if age_line_match:
                    age_text = age_line_match.group(1)
                    m = AGE_REGEX.search(age_text)
                    if m:
                        age = m.group(0).strip()
                        
            if days == "N/A":
                # Look for "Days: <Day, Day, Day>"
                days_line_match = re.search(r'Days:\s*(.*?)(?:\s*Our|\s*Register)', txt, re.I | re.DOTALL)
                if days_line_match:
                    day_list = days_line_match.group(1).split(',')
                    cleaned_days = [d.strip().capitalize() for d in day_list if d.strip()]
                    if cleaned_days:
                         days = ", ".join(cleaned_days)
            # --- End AlphaMinds Specific Extractions ---

            
            # Age detection (using global AGE_REGEX) - secondary search
            m = AGE_REGEX.search(txt)
            if m and age == "N/A": 
                age = m.group(0).strip()
            
            # Time detection
            if re.search(r'\b(time|when)\b', low) or time_re.search(txt):
                tms = [m.group(0).strip() for m in time_re.finditer(txt)]
                if tms:
                    current_times = times.split(', ') if times != 'N/A' else []
                    new_times = list(dict.fromkeys(current_times + tms))
                    times = ", ".join(new_times)

                    # Compute length if a time range is present AND length is not already set
                    cl = parse_time_range_to_duration(times)
                    if cl != "N/A" and class_length == "N/A":
                        class_length = cl

            # Duration/Size
            if re.search(r'\b(max|capacity|class size|limit|spots)\b', low) or re.search(r'\b(duration|length|minutes|hours)\b', low):
                m = CLASS_SIZE_REGEX.search(txt)
                if m:
                    class_size = m.group(1)
                m2 = DURATION_REGEX.search(txt)
                if m2 and class_length == "N/A":
                    class_length = m2.group(1).strip()
                    
            # Collect description parts
            if sib.name in ["p", "div", "li", "ul", "ol"]:
                # Exclude known location/API junk from the description text
                if "schoolMapsSettings" not in txt and "imports" not in txt and "var schoolMapsSettings" not in txt:
                    description_parts.append(txt)

            # Location extraction (use helper function which tries to find address tags/classes)
            if class_location == "N/A" and re.search(r'\b(address|location|studio|venue)\b', low):
                _, _, s_addr = extract_size_length_address(txt, node=sib, soup=soup)
                if s_addr and s_addr != "N/A":
                    class_location = s_addr
        
        # --- Post-processing and Heuristics ---

        # 3. Clean up the final description
        description = (" ".join(x for x in description_parts if x)).strip() or "N/A"
        
        # 4. Aggressive Fallback: Final sweep of all text
        full_text_for_search = class_type + " " + description
        
        if age == "N/A" or days == "N/A" or times == "N/A" or class_length == "N/A":
            a_guess, d_guess, t_guess = extract_age_day_time_from_text(full_text_for_search)
            
            if age == "N/A" and a_guess != "N/A": age = a_guess
            if days == "N/A" and d_guess != "N/A": days = d_guess
            
            if times == "N/A" and t_guess != "N/A":
                times = t_guess
                if class_length == "N/A":
                    cl = parse_time_range_to_duration(times)
                    if cl != "N/A": class_length = cl

            # Try extracting duration string again if duration is missing
            m2 = DURATION_REGEX.search(full_text_for_search)
            if m2 and class_length == "N/A":
                class_length = m2.group(1).strip()
        
        # 5. Final cleanup for location pollution risk 
        if class_location and ('schoolMapsSettings' in class_location or 'wordpress/interactivity' in class_location or 'AIzaSyDn9tufwujzyp22Go' in class_location):
            class_location = "N/A"
        
        # --- Final Row Filtering ---

        # Build the final structured row
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassTitle": ensure_field(class_type),
            "ClassType": ensure_field(class_type),
            "AgeRange": ensure_field(age),
            "ClassSize": ensure_field(class_size),
            "ClassLength": ensure_field(class_length),
            "DayOfWeek": ensure_field(days),
            "Times": ensure_field(times),
            "ClassDescription": ensure_field(description),
            "ClassLocation": ensure_field(class_location),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        
        # Only output rows that contain some meaningful data (Title, Age, Length, or Day)
        if (row["AgeRange"] != "N/A" or row["ClassLength"] != "N/A" or row["DayOfWeek"] != "N/A" or row["ClassTitle"] not in ["Classes Offered", "All Age Groups"]):
             items.append({k: row.get(k, "N/A") for k in schema})
             
    return items

def scrape_generic(url, schema, website_name, use_js=False):
    html = fetch_html(url, use_js=use_js)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    try:
        title = soup.title.string.strip() if soup.title else "N/A"
        p = soup.find("p")
        snippet = p.get_text(" ", strip=True) if p else soup.get_text(" ", strip=True)[:300]
        age, days, times = extract_age_day_time_from_text(soup.get_text(" ", strip=True))
        class_size, class_length, class_location = extract_size_length_address(soup.get_text(" ", strip=True), node=None, soup=soup)
        # if times present and length unknown, try compute
        if class_length == "N/A" and times and times != "N/A":
            class_length = parse_time_range_to_duration(times)
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassTitle": ensure_field(title),
            "ClassType": ensure_field(title),
            "AgeRange": ensure_field(age),
            "ClassSize": ensure_field(class_size),
            "ClassLength": ensure_field(class_length),
            "DayOfWeek": ensure_field(days),
            "Times": ensure_field(times),
            "ClassDescription": ensure_field(snippet),
            "ClassLocation": ensure_field(class_location),
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

def scrape_soccershots(url, schema, website_name, use_js=True):
    # SoccerShots search pages generally render results client-side — enable JS by default
    html = fetch_html(url, use_js=use_js)
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
            class_size = "N/A"
            class_length = "N/A"
            class_location = "N/A"
            # try location object
            loc = c.get("location") or c.get("venue") or c.get("address")
            if isinstance(loc, dict):
                parts = []
                for k in ('streetAddress','addressLocality','postalCode','addressRegion','addressCountry','name'):
                    if loc.get(k):
                        parts.append(str(loc.get(k)))
                if parts:
                    class_location = ", ".join(parts)
            elif isinstance(loc, str):
                class_location = loc
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassTitle": ensure_field(title),
                "ClassType": ensure_field(title),
                "AgeRange": ensure_field(age),
                "ClassSize": ensure_field(class_size),
                "ClassLength": ensure_field(class_length),
                "DayOfWeek": ensure_field(date),
                "Times": ensure_field(times),
                "ClassDescription": ensure_field(desc),
                "ClassLocation": ensure_field(class_location),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
    if items:
        return items
    # Fallback to DOM heuristics (best-effort)
    # Added common SoccerShots specific selectors like search-result-card
    selectors = ["div.search-result-card", "div.result", "div.search-result", "div.search-listing", "li.result", "div.card", "div.listing", "div.item", ".product"]
    for sel in selectors:
        nodes = soup.select(sel)
        if not nodes:
            continue
        for node in nodes:
            title = node.find(["h2","h3","h4"]) or node.find("a")
            title_text = title.get_text(" ", strip=True) if title else node.get_text(" ", strip=True)[:100]
            snippet = node.get_text(" ", strip=True)
            age, days, times = extract_age_day_time_from_text(snippet)
            class_size, class_length, class_location = extract_size_length_address(snippet, node=node, soup=soup)
            if class_length == "N/A" and times and times != "N/A":
                class_length = parse_time_range_to_duration(times)
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassTitle": ensure_field(title_text),
                "ClassType": ensure_field(title_text),
                "AgeRange": ensure_field(age),
                "ClassSize": ensure_field(class_size),
                "ClassLength": ensure_field(class_length),
                "DayOfWeek": ensure_field(days),
                "Times": ensure_field(times),
                "ClassDescription": ensure_field(snippet),
                "ClassLocation": ensure_field(class_location),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
        if items:
            return items
    # final fallback
    return scrape_generic(url, schema, website_name, use_js=use_js)

def scrape_mygym(url, schema, website_name, use_js=False):
    html = fetch_html(url, use_js=use_js)
    if not html: return []
    soup = BeautifulSoup(html, "html.parser")
    items = []
    nodes = soup.select("div.schedule-item, div.class-box, div.schedule-card, div.class-card, div.card, li.class-item, div.schedule, .class-listing, .class")
    if not nodes:
        headings = soup.find_all(["h2","h3","h4"])
        for h in headings:
            title = h.get_text(" ", strip=True)
            snippet = ""
            for sib in h.find_next_siblings(limit=6):
                snippet += " " + sib.get_text(" ", strip=True)
            age, days, times = extract_age_day_time_from_text(snippet)
            class_size, class_length, class_location = extract_size_length_address(snippet, node=h, soup=soup)
            if class_length == "N/A" and times and times != "N/A":
                class_length = parse_time_range_to_duration(times)
            row = {
                "Website": website_name,
                "PageURL": url,
                "ClassTitle": ensure_field(title),
                "ClassType": ensure_field(title),
                "AgeRange": ensure_field(age),
                "ClassSize": ensure_field(class_size),
                "ClassLength": ensure_field(class_length),
                "DayOfWeek": ensure_field(days),
                "Times": ensure_field(times),
                "ClassDescription": ensure_field(snippet.strip() or title),
                "ClassLocation": ensure_field(class_location),
                "ScrapeDate": to_mmddyyyy(datetime.now())
            }
            items.append({k: row.get(k, "N/A") for k in schema})
        return items if items else scrape_generic(url, schema, website_name, use_js=use_js)
    for node in nodes:
        title = node.find(["h2","h3","h4"]) or node.find("a")
        title_text = title.get_text(" ", strip=True) if title else node.get_text(" ", strip=True)[:80]
        snippet = node.get_text(" ", strip=True)
        age, days, times = extract_age_day_time_from_text(snippet)
        class_size, class_length, class_location = extract_size_length_address(snippet, node=node, soup=soup)
        if class_length == "N/A" and times and times != "N/A":
            class_length = parse_time_range_to_duration(times)
        row = {
            "Website": website_name,
            "PageURL": url,
            "ClassTitle": ensure_field(title_text),
            "ClassType": ensure_field(title_text),
            "AgeRange": ensure_field(age),
            "ClassSize": ensure_field(class_size),
            "ClassLength": ensure_field(class_length),
            "DayOfWeek": ensure_field(days),
            "Times": ensure_field(times),
            "ClassDescription": ensure_field(snippet),
            "ClassLocation": ensure_field(class_location),
            "ScrapeDate": to_mmddyyyy(datetime.now())
        }
        items.append({k: row.get(k, "N/A") for k in schema})
    return items

def scrape_hisawyer(url, schema, website_name, use_js=False):
    # similar to mygym/generic heuristics
    return scrape_generic(url, schema, website_name, use_js=use_js)

def scrape_babybandstand(url, schema, website_name, use_js=False):
    # babybandstand often uses the mainstreetsites widget which requires JS
    # The dispatcher handles force-enabling JS if configured.
    return scrape_generic(url, schema, website_name, use_js=use_js)

SCRAPER_REGISTRY = {
    "alphamindsacademy": scrape_alphaminds,
    "aquatots": scrape_aquatots, # Added new scraper to registry
    "soccershots": scrape_soccershots,
    "mygym": scrape_mygym,
    "hisawyer": scrape_hisawyer,
    "babybandstand": scrape_babybandstand,
    "generic": scrape_generic,
}

def run_scrape_job(page_config, loader):
    website_name = page_config.get("website", "N/A")
    url = page_config.get("url", "N/A")
    # Determine JS usage: global setting, or page specific setting, or specific site override
    use_js = page_config.get("use_js", False) or USE_JS_RENDERING 
    
    print(f"[{datetime.now().isoformat()}] Running scrape for: {website_name} - {url} (use_js={use_js})")
    schema = page_config.get("schema")
    if not isinstance(schema, list):
        print("  Invalid schema — skipping page.")
        return
    wl = (website_name or "").lower()
    
    scraper_func = SCRAPER_REGISTRY.get(wl, SCRAPER_REGISTRY["generic"])
    
    # Handle specific overrides where JS is mandatory (e.g., SoccerShots)
    if "soccershots" in wl or "hudsoncounty" in wl:
        scraper_func = SCRAPER_REGISTRY["soccershots"]
        use_js = True

    try:
        items = scraper_func(url, schema, website_name, use_js=use_js)
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
        scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
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
            # normalize header strings
            hdr_norm = [c.strip() for c in header]
            if not existing or [c.strip() for c in existing] != hdr_norm:
                try:
                    self.sheet.insert_row(hdr_norm, index=1)
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

"""
