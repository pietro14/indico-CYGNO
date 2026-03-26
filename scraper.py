"""
CYGNO Indico Scraper — uses the Indico JSON API
Fetches all CYGNO meetings from agenda.infn.it category 1149
(including all subcategories), stores data in SQLite.
Supports incremental updates.
"""

import os
import sqlite3
import time
from datetime import datetime

import requests

BASE_URL = "https://agenda.infn.it"
CATEGORY_ID = 1149
CATEGORY_API = f"{BASE_URL}/export/categ/{CATEGORY_ID}.json"
EVENT_API = f"{BASE_URL}/export/event/{{event_id}}.json"

DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DB_DIR, "cygno_meetings.db")

# Rate limiting: pause between API calls to be polite
REQUEST_DELAY = 0.3  # seconds


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
            date TEXT NOT NULL,
            category TEXT DEFAULT ''
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


def insert_event(conn, event_url, title, date, category, contributions):
    cur = conn.execute(
        "INSERT OR IGNORE INTO meetings (event_url, title, date, category) VALUES (?, ?, ?, ?)",
        (event_url, title, date, category),
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


# --- API Fetching ---

def fetch_category_events():
    """Fetch all events in the CYGNO category (including subcategories)."""
    params = {
        "from": "2018-01-01",
        "to": "2027-12-31",
        "limit": "1000",
    }
    resp = requests.get(CATEGORY_API, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    return data.get("results", [])


def fetch_event_contributions(event_id):
    """Fetch detailed contribution data for a single event."""
    url = EVENT_API.format(event_id=event_id)
    params = {"detail": "contributions"}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("results", [])
    if not results:
        return []
    event = results[0]
    return event.get("contributions", [])


def parse_contributions(raw_contributions):
    """Parse contribution JSON into our format."""
    contributions = []
    for c in raw_contributions:
        title = c.get("title", "")

        # Speakers
        speakers = c.get("speakers", [])
        if speakers:
            speaker_name = speakers[0].get("first_name", "") + " " + speakers[0].get("last_name", "")
            speaker_name = speaker_name.strip()
            institution = speakers[0].get("affiliation", "N/A")
        else:
            speaker_name = "N/A"
            institution = "N/A"

        # PDF: look through folders -> attachments
        pdf_url = "no PDF"
        for folder in c.get("folders", []):
            for att in folder.get("attachments", []):
                if att.get("filename", "").lower().endswith(".pdf"):
                    pdf_url = att.get("download_url", "no PDF")
                    break
            if pdf_url != "no PDF":
                break

        contributions.append({
            "title": title,
            "speaker": speaker_name,
            "institution": institution,
            "pdf_url": pdf_url,
        })

    return contributions


def get_event_category(event_data):
    """Extract the most specific category name from categoryPath."""
    path = event_data.get("categoryPath", [])
    if path:
        return path[-1].get("name", "CYGNO")
    return event_data.get("category", "CYGNO")


# --- Main scrape ---

def scrape_events(db_path=DB_PATH, progress_callback=None):
    conn = init_db(db_path)
    new_count = 0

    try:
        if progress_callback:
            progress_callback("Fetching event list from Indico API...", 0, 0)

        all_events = fetch_category_events()
        total = len(all_events)

        if progress_callback:
            progress_callback(f"Found {total} events. Checking for new ones...", 0, total)

        # Filter to only new events
        new_events = []
        for ev in all_events:
            url = ev.get("url", "")
            if url and not event_exists(conn, url):
                new_events.append(ev)

        if progress_callback:
            progress_callback(f"{len(new_events)} new events to fetch.", 0, len(new_events))

        for i, ev in enumerate(new_events):
            event_id = ev.get("id", "")
            event_url = ev.get("url", "")
            event_title = ev.get("title", "Untitled")
            category = get_event_category(ev)

            # Format date
            start = ev.get("startDate", {})
            event_date = f"{start.get('date', '')} {start.get('time', '')[:5]}"

            if progress_callback:
                progress_callback(
                    f"Fetching [{i+1}/{len(new_events)}]: {event_title[:50]}",
                    i + 1,
                    len(new_events),
                )

            # Fetch contributions
            try:
                raw_contribs = fetch_event_contributions(event_id)
                contributions = parse_contributions(raw_contribs)
            except Exception as e:
                print(f"  Warning: failed to fetch contributions for event {event_id}: {e}")
                contributions = []

            # If no contributions, still store the event with an empty contribution
            if not contributions:
                contributions = [{
                    "title": "",
                    "speaker": "",
                    "institution": "",
                    "pdf_url": "",
                }]

            inserted = insert_event(conn, event_url, event_title, event_date, category, contributions)
            if inserted:
                new_count += 1

            time.sleep(REQUEST_DELAY)

        # Update metadata
        set_meta(conn, "last_scrape_timestamp", datetime.now().isoformat())

    except Exception as e:
        print(f"Scrape error: {e}")
        raise
    finally:
        conn.close()

    return new_count


if __name__ == "__main__":
    def _progress(msg, current, total):
        print(f"  [{current}/{total}] {msg}")

    print("Starting CYGNO Indico scraper (JSON API)...")
    n = scrape_events(progress_callback=_progress)
    print(f"Done. {n} new events scraped.")
