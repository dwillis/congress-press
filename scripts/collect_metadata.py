#!/usr/bin/env python3
"""Phase 1: Collect press release metadata from all congressional scrapers.

Scrapes page 1 of each member scraper (SCRAPER_CONFIG) and each committee
scraper (COMMITTEE_CONFIG). Member releases go to the current month's file in
data/YYYY/, committee releases to data/committees/YYYY/. New records are
added and existing records are updated if their titles have changed.
"""

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from python_statement import Scraper
from python_statement.committee_config import COMMITTEE_CONFIG
from python_statement.config import SCRAPER_CONFIG

from utils import (
    COMMITTEE_DIR,
    DATA_DIR,
    committee_attribution,
    current_month_path,
    is_future_date,
    load_all_urls,
    load_committee_map,
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


def collect(label, scraper_names, root, owner_fields, all_urls, timestamp):
    """Scrape page 1 of each scraper and merge results into root's current month.

    owner_fields(name, date_str) returns the fields that attach a record to its
    owner: {"member": ...} or {"member": None, "committee": ...}.
    Returns (failed_scrapers, total_scrapers).
    """
    path = current_month_path(root)
    by_url = records_by_url(load_jsonl(path))

    print(f"[{label}] Running {len(scraper_names)} scrapers with {WORKERS} workers...")

    new_count = 0
    updated_count = 0
    failed_scrapers = []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(scrape_one, name): name for name in scraper_names}

        for future in as_completed(futures):
            name, results, error = future.result()

            if error:
                failed_scrapers.append((name, error))
                continue

            for item in results:
                url = item.get("url")
                if not url:
                    continue

                date_val = item.get("date")
                date_str = str(date_val) if date_val else None

                if is_future_date(date_str):
                    continue

                if url in by_url:
                    # Existing record in current month — check if title changed
                    record = by_url[url]
                    if record["title"] != item.get("title", ""):
                        record["title"] = item.get("title", "")
                        record["updated_at"] = timestamp
                        record["text"] = None  # trigger re-extraction
                        updated_count += 1
                elif url in all_urls:
                    # Exists in a different month's file — skip
                    continue
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
                        **owner_fields(name, date_str),
                        "text": None,
                        "collected_at": timestamp,
                        "updated_at": timestamp,
                    }
                    by_url[url] = record
                    all_urls.add(url)
                    new_count += 1

    all_records = list(by_url.values())
    save_jsonl(path, all_records)

    print(f"\n[{label}] Results saved to {path}")
    print(f"  Total records: {len(all_records)}")
    print(f"  New: {new_count}")
    print(f"  Updated: {updated_count}")
    print(f"  Scrapers failed: {len(failed_scrapers)}")

    if failed_scrapers:
        print(f"\n[{label}] Failed scrapers:")
        for name, error in sorted(failed_scrapers):
            print(f"  {name}: {error}")
    print()

    return failed_scrapers, len(scraper_names)


def main():
    all_urls = load_all_urls()  # cross-file dedup, both collections
    timestamp = now_iso()

    member_map = load_member_map()
    member_failed, member_total = collect(
        "members",
        list(SCRAPER_CONFIG.keys()),
        DATA_DIR,
        lambda name, date_str: {"member": member_map.get(name)},
        all_urls,
        timestamp,
    )

    committee_map = load_committee_map()
    committee_failed, committee_total = collect(
        "committees",
        list(COMMITTEE_CONFIG.keys()),
        COMMITTEE_DIR,
        lambda name, date_str: {
            "member": None,
            "committee": committee_attribution(
                name, COMMITTEE_CONFIG[name], committee_map.get(name), date_str
            ),
        },
        all_urls,
        timestamp,
    )

    # Fail the run if more than half of either collection's scrapers failed.
    if len(member_failed) > member_total // 2 or len(committee_failed) > committee_total // 2:
        sys.exit(1)


if __name__ == "__main__":
    main()
