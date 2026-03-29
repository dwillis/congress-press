#!/usr/bin/env python3
"""Phase 1: Collect press release metadata from all congressional scrapers.

Scrapes page 1 of each scraper in SCRAPER_CONFIG, writes new records to the
current month's JSONL file, and updates existing records if titles have changed.
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from python_statement import Scraper
from python_statement.config import SCRAPER_CONFIG

from utils import (
    current_month_path,
    load_jsonl,
    load_member_map,
    now_iso,
    records_by_url,
    save_jsonl,
)

WORKERS = 10


def scrape_one(name):
    """Run a single scraper and return (name, results, error)."""
    try:
        results = Scraper.run_scraper(name, 1)
        return name, results or [], None
    except Exception as e:
        return name, [], str(e)


def main():
    member_map = load_member_map()
    path = current_month_path()
    existing = load_jsonl(path)
    by_url = records_by_url(existing)

    scraper_names = list(SCRAPER_CONFIG.keys())
    print(f"Running {len(scraper_names)} scrapers with {WORKERS} workers...")

    new_count = 0
    updated_count = 0
    failed_scrapers = []
    timestamp = now_iso()

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(scrape_one, name): name for name in scraper_names}

        for future in as_completed(futures):
            name, results, error = future.result()

            if error:
                failed_scrapers.append((name, error))
                continue

            member = member_map.get(name)

            for item in results:
                url = item.get("url")
                if not url:
                    continue

                date_val = item.get("date")
                date_str = str(date_val) if date_val else None

                if url in by_url:
                    # Existing record — check if title changed
                    record = by_url[url]
                    if record["title"] != item.get("title", ""):
                        record["title"] = item.get("title", "")
                        record["updated_at"] = timestamp
                        record["text"] = None  # trigger re-extraction
                        updated_count += 1
                else:
                    # New record
                    record = {
                        "url": url,
                        "title": item.get("title", ""),
                        "date": date_str,
                        "date_source": "scraper" if date_str else None,
                        "source": item.get("source", ""),
                        "domain": item.get("domain", ""),
                        "scraper": name,
                        "member": member,
                        "text": None,
                        "collected_at": timestamp,
                        "updated_at": timestamp,
                    }
                    by_url[url] = record
                    new_count += 1

    all_records = list(by_url.values())
    save_jsonl(path, all_records)

    print(f"\nResults saved to {path}")
    print(f"  Total records: {len(all_records)}")
    print(f"  New: {new_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Scrapers failed: {len(failed_scrapers)}")

    if failed_scrapers:
        print("\nFailed scrapers:")
        for name, error in sorted(failed_scrapers):
            print(f"  {name}: {error}")

    if failed_scrapers:
        sys.exit(1) if len(failed_scrapers) > len(scraper_names) // 2 else None


if __name__ == "__main__":
    main()
