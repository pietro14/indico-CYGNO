"""
CYGNO Indico Scraper — adapted from indico2csv.py
Scrapes CYGNO collaboration meetings from agenda.infn.it,
stores data in SQLite with incremental update support.
"""

import os
import re
import sqlite3
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://agenda.infn.it"
DEFAULT_START_URL = "https://agenda.infn.it/event/44949/"
DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "cygno_meetings.db")


# --- Helpers (from original indico2csv.py) ---

def remove_parentheses_content(text):
    return re.sub(r"\(.*?\)", "", text).strip()


def format_date(date_str):
    date_str_cleaned = " ".join(date_str.split())
    date_formats = [
        "%A %b %d, %Y, %I:%M %p",
        "%b %d, %Y, %I:%M %p",
    ]
    for fmt in date_formats:
        try:
            dt = datetime.strptime(date_str_cleaned, fmt)
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
    return date_str_cleaned


# --- Database ---

def init_db(db_path=DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_url TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            date TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            meeting_id INTEGER NOT NULL REFERENCES meetings(id),
            title TEXT,
            speaker TEXT,
            institution TEXT,
            pdf_url TEXT,
            UNIQUE(meeting_id, title, speaker)
        );
        CREATE TABLE IF NOT EXISTS scrape_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    return conn


def event_exists(conn, event_url):
    row = conn.execute(
        "SELECT 1 FROM meetings WHERE event_url = ?", (event_url,)
    ).fetchone()
    return row is not None


def insert_event(conn, event_url, title, date, contributions):
    cur = conn.execute(
        "INSERT OR IGNORE INTO meetings (event_url, title, date) VALUES (?, ?, ?)",
        (event_url, title, date),
    )
    if cur.rowcount == 0:
        return False
    meeting_id = cur.lastrowid
    for c in contributions:
        conn.execute(
            "INSERT OR IGNORE INTO contributions (meeting_id, title, speaker, institution, pdf_url) "
            "VALUES (?, ?, ?, ?, ?)",
            (meeting_id, c["title"], c["speaker"], c["institution"], c["pdf_url"]),
        )
    conn.commit()
    return True


def get_meta(conn, key):
    row = conn.execute("SELECT value FROM scrape_meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else None


def set_meta(conn, key, value):
    conn.execute(
        "INSERT OR REPLACE INTO scrape_meta (key, value) VALUES (?, ?)", (key, value)
    )
    conn.commit()


# --- Selenium ---

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


# --- Parsing ---

def parse_single_event(soup, event_url):
    title_el = soup.find("h1", itemprop="name")
    event_title = title_el.get_text(strip=True).replace("¶", "") if title_el else "Untitled"

    date_el = soup.find("time", itemprop="startDate")
    event_date = format_date(date_el.get_text(strip=True)) if date_el else ""

    contributions = []
    entries = soup.find_all("li", class_="timetable-item timetable-contrib")

    if not entries:
        contributions.append({
            "title": "",
            "speaker": "",
            "institution": "",
            "pdf_url": "",
        })
    else:
        for entry in entries:
            ctitle_el = entry.find("span", class_="timetable-title")
            ctitle = ctitle_el.get_text(strip=True).replace("¶", "") if ctitle_el else ""

            speaker = "N/A"
            institution = "N/A"
            speaker_el = entry.find("div", class_="speaker-list")
            if speaker_el:
                spans = speaker_el.find_all("span")
                if len(spans) > 1:
                    speaker = remove_parentheses_content(spans[1].get_text(strip=True))
                inst_el = speaker_el.find("span", class_="affiliation")
                if inst_el:
                    institution = inst_el.get_text(strip=True).replace("(", "").replace(")", "")

            pdf_url = "no PDF"
            pdf_el = entry.find("div", class_="js-attachment-container")
            if pdf_el:
                anchor = pdf_el.find("a", href=True)
                if anchor and anchor["href"].endswith(".pdf"):
                    pdf_url = BASE_URL + anchor["href"]

            contributions.append({
                "title": ctitle,
                "speaker": speaker,
                "institution": institution,
                "pdf_url": pdf_url,
            })

    return {
        "title": event_title,
        "date": event_date,
        "event_url": event_url,
        "contributions": contributions,
    }


def find_older_event_link(soup):
    link = soup.find("a", class_="icon-prev", href=True)
    if link:
        return BASE_URL + link["href"]
    return None


# --- Main scrape loop ---

def scrape_events(db_path=DB_PATH, start_url=DEFAULT_START_URL, progress_callback=None):
    conn = init_db(db_path)
    driver = create_driver()
    new_count = 0

    try:
        current_url = start_url
        while current_url:
            if progress_callback:
                progress_callback(f"Loading {current_url}", new_count)

            driver.get(current_url)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            actual_url = driver.current_url

            # Incremental: stop if we already have this event
            if event_exists(conn, actual_url):
                if progress_callback:
                    progress_callback("Reached already-scraped events, stopping.", new_count)
                break

            soup = BeautifulSoup(driver.page_source, "html.parser")
            event_data = parse_single_event(soup, actual_url)

            inserted = insert_event(
                conn,
                event_data["event_url"],
                event_data["title"],
                event_data["date"],
                event_data["contributions"],
            )
            if inserted:
                new_count += 1

            current_url = find_older_event_link(soup)

        # Update metadata
        set_meta(conn, "last_scrape_timestamp", datetime.now().isoformat())
        set_meta(conn, "start_url", start_url)

    except Exception as e:
        print(f"Scrape error: {e}")
        raise
    finally:
        driver.quit()
        conn.close()

    return new_count


if __name__ == "__main__":
    def _progress(msg, count):
        print(f"  [{count} new] {msg}")

    print("Starting CYGNO Indico scraper...")
    n = scrape_events(progress_callback=_progress)
    print(f"Done. {n} new events scraped.")
