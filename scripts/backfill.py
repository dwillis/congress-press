#!/usr/bin/env python3
"""One-time backfill: paginate through all scrapers to collect historical releases.

Usage:
    uv run python scripts/backfill.py                    # All scrapers, all pages
    uv run python scripts/backfill.py --scraper pelosi   # Single scraper
    uv run python scripts/backfill.py --max-pages 10     # Limit pagination depth
    uv run python scripts/backfill.py --resume            # Skip scrapers already in data
    uv run python scripts/backfill.py --workers 5        # Parallel scrapers (default: 3)
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from python_statement import Scraper
from python_statement.config import SCRAPER_CONFIG

from utils import (
    DATA_DIR,
    load_jsonl,
    load_member_map,
    now_iso,
    records_by_url,
    save_jsonl,
)

# Methods that don't support pagination
NO_PAGINATION = {"react", "joyce"}

DEFAULT_MAX_PAGES = 50
DEFAULT_WORKERS = 3


def scrape_all_pages(name, max_pages):
    """Paginate through a scraper until empty or max_pages. Returns all results."""
    all_results = []
    config = SCRAPER_CONFIG.get(name, {})
    method = config.get("method", "")

    # React/joyce sites don't paginate — just grab page 1
    if method in NO_PAGINATION:
        max_pages = 1

    for page in range(1, max_pages + 1):
        try:
            results = Scraper.run_scraper(name, page)
        except Exception as e:
            print(f"  {name} page {page}: error - {e}")
            break

        if not results:
            break

        # Check for duplicate results (some sites loop back to page 1)
        new_urls = {r["url"] for r in results if r.get("url")}
        existing_urls = {r["url"] for r in all_results if r.get("url")}
        if new_urls and new_urls.issubset(existing_urls):
            break

        all_results.extend(results)

    return all_results


def month_path_for_date(d):
    """Return the JSONL path for a given date."""
    year_dir = DATA_DIR / str(d.year)
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / f"{d.year}-{d.month:02d}.jsonl"


def main():
    parser = argparse.ArgumentParser(description="Backfill historical press releases")
    parser.add_argument("--scraper", type=str, help="Run a single scraper by name")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=f"Max pages to paginate per scraper (default: {DEFAULT_MAX_PAGES})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Parallel scrapers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip scrapers that already have records in data/",
    )
    args = parser.parse_args()

    member_map = load_member_map()

    # Determine which scrapers to run
    if args.scraper:
        if args.scraper not in SCRAPER_CONFIG:
            print(f"Unknown scraper: {args.scraper}")
            print(f"Available: {', '.join(sorted(SCRAPER_CONFIG.keys())[:20])}...")
            sys.exit(1)
        scraper_names = [args.scraper]
    else:
        scraper_names = sorted(SCRAPER_CONFIG.keys())

    # Load all existing records for resume/dedup
    existing_urls = set()
    scrapers_with_data = set()
    for jsonl_path in DATA_DIR.rglob("*.jsonl"):
        records = load_jsonl(jsonl_path)
        for r in records:
            existing_urls.add(r.get("url"))
            scrapers_with_data.add(r.get("scraper"))

    if args.resume:
        before = len(scraper_names)
        scraper_names = [n for n in scraper_names if n not in scrapers_with_data]
        print(f"Resume mode: skipping {before - len(scraper_names)} scrapers with existing data")

    print(f"Backfilling {len(scraper_names)} scrapers, max {args.max_pages} pages each")
    print(f"Existing records: {len(existing_urls)}")

    # Collect results grouped by month
    month_records = defaultdict(dict)  # {month_path: {url: record}}
    timestamp = now_iso()
    total_new = 0
    failed_scrapers = []

    # Load existing monthly files into month_records for dedup
    for jsonl_path in DATA_DIR.rglob("*.jsonl"):
        records = load_jsonl(jsonl_path)
        by_url = records_by_url(records)
        month_records[str(jsonl_path)] = by_url

    def process_scraper(name):
        results = scrape_all_pages(name, args.max_pages)
        return name, results

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_scraper, name): name for name in scraper_names}

        for future in as_completed(futures):
            name, results = future.result()

            if not results:
                failed_scrapers.append(name)
                continue

            member = member_map.get(name)
            new_for_scraper = 0

            for item in results:
                url = item.get("url")
                if not url or url in existing_urls:
                    continue

                date_val = item.get("date")
                date_str = str(date_val) if date_val else None

                # Determine which month file this belongs in
                if date_val and isinstance(date_val, date):
                    path = str(month_path_for_date(date_val))
                else:
                    # No date — put in current month
                    path = str(month_path_for_date(date.today()))

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

                if path not in month_records:
                    month_records[path] = {}

                month_records[path][url] = record
                existing_urls.add(url)
                new_for_scraper += 1

            total_new += new_for_scraper
            pages = len(results) // 10 + 1  # rough estimate
            print(f"  {name}: {len(results)} results, {new_for_scraper} new (~{pages} pages)")

    # Write all month files
    files_written = 0
    for path, url_dict in month_records.items():
        if url_dict:
            save_jsonl(path, list(url_dict.values()))
            files_written += 1

    print(f"\nBackfill complete")
    print(f"  New records: {total_new}")
    print(f"  Files written: {files_written}")
    print(f"  Scrapers with no results: {len(failed_scrapers)}")

    if failed_scrapers:
        print(f"\nScrapers with no results ({len(failed_scrapers)}):")
        for name in sorted(failed_scrapers):
            print(f"  {name}")


if __name__ == "__main__":
    main()
