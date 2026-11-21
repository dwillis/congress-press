#!/usr/bin/env python3
"""One-time backfill: paginate through all scrapers to collect historical releases.

Usage:
    uv run python scripts/backfill.py                    # All member scrapers
    uv run python scripts/backfill.py --scraper pelosi   # Single scraper
    uv run python scripts/backfill.py --max-pages 10     # Limit pagination depth
    uv run python scripts/backfill.py --resume            # Skip scrapers already in data
    uv run python scripts/backfill.py --workers 5        # Parallel scrapers (default: 3)

    # Committee collection (written to data/committees/)
    uv run python scripts/backfill.py --collection committees --workers 4
    uv run python scripts/backfill.py --collection committees --committee HSAG --max-pages 5
"""

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from python_statement import Scraper
from python_statement.committee_config import COMMITTEE_CONFIG
from python_statement.config import SCRAPER_CONFIG

from utils import (
    COMMITTEE_DIR,
    DATA_DIR,
    committee_attribution,
    is_future_date,
    load_committee_map,
    load_jsonl,
    load_member_map,
    month_path,
    now_iso,
    records_by_url,
    save_jsonl,
)

# Methods that don't support pagination
NO_PAGINATION = {"react", "joyce"}
DEFAULT_MAX_PAGES = 50
# Committee sites are few and you want their whole history, so page deeper.
# The repeat-page check below stops sites that loop well before this.
DEFAULT_MAX_PAGES_COMMITTEES = 500
DEFAULT_WORKERS = 3


def scrape_all_pages(name, max_pages, config):
    """Paginate through a scraper until empty, repeating, or max_pages.

    Returns (results, pages_fetched, hit_cap).
    """
    all_results = []
    seen_urls = set()
    method = config.get("method", "")

    # React/joyce sites don't paginate — just grab page 1
    if method in NO_PAGINATION:
        max_pages = 1

    pages = 0
    for page in range(1, max_pages + 1):
        try:
            results = Scraper.run_scraper(name, page)
        except Exception as e:
            print(f"  {name} page {page}: error - {e}")
            return all_results, pages, False

        if not results:
            return all_results, pages, False

        # Check for duplicate results (some sites loop back to page 1 or
        # keep serving their last page)
        new_urls = {r["url"] for r in results if r.get("url")}
        if new_urls and new_urls.issubset(seen_urls):
            return all_results, pages, False

        pages += 1
        seen_urls |= new_urls
        all_results.extend(results)

    return all_results, pages, max_pages > 1


def main():
    parser = argparse.ArgumentParser(description="Backfill historical press releases")
    parser.add_argument(
        "--collection",
        choices=["members", "committees"],
        default="members",
        help="Which scraper collection to backfill (default: members)",
    )
    parser.add_argument("--scraper", type=str, help="Run a single scraper by name")
    parser.add_argument(
        "--committee",
        type=str,
        help="With --collection committees: every side of one committee (thomas_id, e.g. HSAG)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help=(
            f"Max pages to paginate per scraper (default: {DEFAULT_MAX_PAGES} for members, "
            f"{DEFAULT_MAX_PAGES_COMMITTEES} for committees)"
        ),
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
        help="Skip scrapers that already have records in this collection",
    )
    args = parser.parse_args()

    committees = args.collection == "committees"
    configs = COMMITTEE_CONFIG if committees else SCRAPER_CONFIG
    root = COMMITTEE_DIR if committees else DATA_DIR
    max_pages = args.max_pages or (DEFAULT_MAX_PAGES_COMMITTEES if committees else DEFAULT_MAX_PAGES)

    if committees:
        committee_map = load_committee_map()

        def owner_fields(name, date_str):
            return {
                "member": None,
                "committee": committee_attribution(
                    name, configs[name], committee_map.get(name), date_str
                ),
            }
    else:
        member_map = load_member_map()

        def owner_fields(name, date_str):
            return {"member": member_map.get(name)}

    # Determine which scrapers to run
    if args.committee:
        if not committees:
            print("--committee requires --collection committees")
            sys.exit(1)
        thomas_id = args.committee.upper()
        scraper_names = sorted(n for n, c in configs.items() if c["thomas_id"] == thomas_id)
        if not scraper_names:
            print(f"No committee scrapers for {thomas_id}")
            sys.exit(1)
    elif args.scraper:
        if args.scraper not in configs:
            print(f"Unknown {args.collection} scraper: {args.scraper}")
            print(f"Available: {', '.join(sorted(configs.keys())[:20])}...")
            sys.exit(1)
        scraper_names = [args.scraper]
    else:
        scraper_names = sorted(configs.keys())

    # URLs across both collections, for dedup. Month files from this
    # collection's tree only, since that's where new records go.
    existing_urls = set()
    scrapers_with_data = set()
    month_records = {}  # {month_path: {url: record}}
    committee_tree = COMMITTEE_DIR.resolve()
    for jsonl_path in DATA_DIR.rglob("*.jsonl"):
        records = load_jsonl(jsonl_path)
        in_root = jsonl_path.resolve().is_relative_to(committee_tree) == committees
        for r in records:
            existing_urls.add(r.get("url"))
            if in_root:
                scrapers_with_data.add(r.get("scraper"))
        if in_root:
            month_records[str(jsonl_path)] = records_by_url(records)

    if args.resume:
        before = len(scraper_names)
        scraper_names = [n for n in scraper_names if n not in scrapers_with_data]
        print(f"Resume mode: skipping {before - len(scraper_names)} scrapers with existing data")

    print(f"Backfilling {len(scraper_names)} {args.collection} scrapers, max {max_pages} pages each")
    print(f"Existing records: {len(existing_urls)}")

    timestamp = now_iso()
    total_new = 0
    failed_scrapers = []
    capped_scrapers = []
    dirty_paths = set()

    def process_scraper(name):
        results, pages, hit_cap = scrape_all_pages(name, max_pages, configs.get(name, {}))
        return name, results, pages, hit_cap

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_scraper, name): name for name in scraper_names}

        for future in as_completed(futures):
            name, results, pages, hit_cap = future.result()

            if not results:
                failed_scrapers.append(name)
                continue
            if hit_cap:
                capped_scrapers.append(name)

            new_for_scraper = 0

            for item in results:
                url = item.get("url")
                if not url or url in existing_urls:
                    continue

                date_val = item.get("date")
                date_str = str(date_val) if date_val else None

                if is_future_date(date_str):
                    continue

                # Month file by release date; undated records go in the current month
                path = str(month_path(date_str or str(date.today()), root=root))

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

                month_records.setdefault(path, {})[url] = record
                dirty_paths.add(path)
                existing_urls.add(url)
                new_for_scraper += 1

            total_new += new_for_scraper
            cap_note = " (hit --max-pages)" if hit_cap else ""
            print(f"  {name}: {len(results)} results, {new_for_scraper} new, {pages} pages{cap_note}")

    # Write only the month files that gained records
    for path in dirty_paths:
        save_jsonl(path, list(month_records[path].values()))

    print(f"\nBackfill complete")
    print(f"  New records: {total_new}")
    print(f"  Files written: {len(dirty_paths)}")
    print(f"  Scrapers with no results: {len(failed_scrapers)}")

    if failed_scrapers:
        print(f"\nScrapers with no results ({len(failed_scrapers)}):")
        for name in sorted(failed_scrapers):
            print(f"  {name}")

    if capped_scrapers:
        print(f"\nScrapers that hit --max-pages {max_pages}; rerun deeper for full history:")
        for name in sorted(capped_scrapers):
            print(f"  {name}")


if __name__ == "__main__":
    main()
