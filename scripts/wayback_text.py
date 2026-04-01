#!/usr/bin/env python3
"""Fetch text from Wayback Machine for press releases with missing text.

Checks archive.org for cached copies of pages that are no longer available
on the live web, extracts article text, and updates the JSONL records.

Usage:
    uv run python scripts/wayback_text.py --file data/2023/2023-06.jsonl --limit 10
    uv run python scripts/wayback_text.py --year 2023
    uv run python scripts/wayback_text.py --all-files --dry-run
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

from collect_text import extract_date_from_html
from utils import (
    DATA_DIR,
    current_month_path,
    is_future_date,
    load_jsonl,
    month_path,
    now_iso,
    records_by_url,
    save_jsonl,
)

USER_AGENT = "congress-press/1.0 (https://github.com/dwillis/congress-press)"
WORKERS = 2
DELAY = 1.0  # seconds between requests per worker (respectful rate limiting)
CDX_URL = "https://web.archive.org/cdx/search/cdx"


def find_snapshot(url, target_date=None):
    """Find the closest Wayback Machine snapshot for a URL using the CDX API.

    Uses the CDX Server API which is more reliable than the Availability API.
    When target_date is provided, finds the snapshot closest to that date.

    Returns snapshot_url (str) or None.
    """
    try:
        # Format timestamp for CDX API (YYYYMMDD)
        timestamp = target_date.replace("-", "") if target_date else None

        params = {
            "url": url,
            "output": "json",
            "limit": "1",
            "fl": "timestamp,statuscode",
            "filter": "statuscode:200",
        }

        if timestamp:
            # Find closest snapshot to the target date
            params["closest"] = timestamp
            params["sort"] = "closest"

        resp = requests.get(
            CDX_URL,
            params=params,
            timeout=30,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()

        data = resp.json()
        # CDX returns header row + data rows
        if len(data) < 2:
            return None

        snap_timestamp = data[1][0]  # e.g. "20230115103045"
        return f"https://web.archive.org/web/{snap_timestamp}/{url}"
    except Exception:
        return None


def fetch_wayback_text(snapshot_url, retries=2):
    """Fetch archived HTML and extract article text.

    Retries on timeout errors. Returns (text, pub_date, date_partial, error).
    """
    try:
        last_err = None
        for attempt in range(retries + 1):
            try:
                resp = requests.get(snapshot_url, timeout=60, headers={"User-Agent": USER_AGENT})
                resp.raise_for_status()
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_err = e
                if attempt < retries:
                    time.sleep(2 ** (attempt + 1))  # exponential backoff: 2s, 4s
                    continue
                return None, None, None, f"timeout after {retries + 1} attempts: {e}"
        html = resp.text

        # Use newspaper4k to parse the pre-fetched HTML
        from newspaper import Article
        from newspaper.article import ArticleDownloadState

        article = Article(snapshot_url)
        article.html = html
        article.download_state = ArticleDownloadState.SUCCESS
        article.parse()
        text = article.text.strip() if article.text else None

        # Extract date from the archived HTML
        pub_date = None
        date_partial = None
        extracted, is_partial = extract_date_from_html(html)
        if extracted and not is_partial:
            pub_date = extracted
        elif extracted and is_partial:
            date_partial = extracted

        return text, pub_date, date_partial, None
    except Exception as e:
        return None, None, None, str(e)


def needs_text(record):
    """Check if a record needs text extraction."""
    return record.get("text") is None


def process_file(path, dry_run=False, limit=0):
    """Process a single JSONL file: fetch text from Wayback Machine for null-text records.

    Returns (found_count, fetched_count, failed_count, not_archived_count,
             date_backfill_count, relocated_count, remaining_null).
    """
    path = Path(path)
    records = load_jsonl(path)

    if not records:
        return 0, 0, 0, 0, 0, 0, 0

    to_fetch = [r for r in records if needs_text(r)]

    if not to_fetch:
        return 0, 0, 0, 0, 0, 0, 0

    if limit > 0:
        to_fetch = to_fetch[:limit]

    found_count = 0
    fetched_count = 0
    failed_count = 0
    not_archived_count = 0
    date_backfill_count = 0
    timestamp = now_iso()

    url_to_idx = {r["url"]: i for i, r in enumerate(records)}

    def process_one(record):
        time.sleep(DELAY)
        url = record["url"]
        target_date = record.get("date")

        # Step 1: Check if Wayback has a snapshot
        snapshot_url = find_snapshot(url, target_date)
        if not snapshot_url:
            return url, None, None, None, "not_archived"

        if dry_run:
            return url, None, None, None, "available"

        # Step 2: Fetch and extract text from the snapshot
        time.sleep(DELAY)  # Additional delay before fetching
        text, pub_date, date_partial, error = fetch_wayback_text(snapshot_url)
        if error:
            return url, None, None, None, f"fetch_error: {error}"

        return url, text, pub_date, date_partial, None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(process_one, r): r for r in to_fetch}

        for future in as_completed(futures):
            url, text, pub_date, date_partial, status = future.result()
            idx = url_to_idx.get(url)
            if idx is None:
                continue

            record = records[idx]

            if status == "not_archived":
                not_archived_count += 1
                continue

            if status == "available":
                # Dry run: just count availability
                found_count += 1
                continue

            if status and status.startswith("fetch_error"):
                failed_count += 1
                continue

            found_count += 1

            if text:
                record["text"] = text
                record["text_source"] = "wayback"
                record["updated_at"] = timestamp
                fetched_count += 1

            # Date backfill
            if pub_date and not is_future_date(pub_date):
                if record.get("date") is None:
                    record["date"] = pub_date
                    record["date_source"] = "page_html"
                    date_backfill_count += 1
                elif is_future_date(record.get("date")):
                    record["date"] = pub_date
                    record["date_source"] = "page_html"
                    date_backfill_count += 1

            if record.get("date") is None and date_partial:
                record["date_partial"] = date_partial

    if dry_run:
        remaining_null = sum(1 for r in records if r.get("text") is None)
        return found_count, 0, 0, not_archived_count, 0, 0, remaining_null

    # Relocate records to correct monthly files if dates changed
    file_month = path.stem
    stay = []
    relocate = {}
    for r in records:
        record_month = r["date"][:7] if r.get("date") else None
        if record_month and record_month != file_month:
            target = str(month_path(r["date"]))
            relocate.setdefault(target, []).append(r)
        else:
            stay.append(r)

    save_jsonl(path, stay)

    relocated_count = 0
    for target_path, moved_records in relocate.items():
        existing = load_jsonl(target_path)
        by_url = records_by_url(existing)
        for r in moved_records:
            by_url[r["url"]] = r
            relocated_count += 1
        save_jsonl(target_path, list(by_url.values()))

    remaining_null = sum(1 for r in stay if r.get("text") is None)
    return found_count, fetched_count, failed_count, not_archived_count, date_backfill_count, relocated_count, remaining_null


def main():
    parser = argparse.ArgumentParser(
        description="Fetch text from Wayback Machine for press releases with missing text"
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Process a specific JSONL file",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Process all files for a specific year (e.g., 2023)",
    )
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Process all JSONL files that have records needing text",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of records to process per file (0 = no limit)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Check Wayback availability only; don't fetch text or write files",
    )
    parser.add_argument(
        "--min-year",
        type=str,
        help="Only process records dated this year or later (e.g., 2015)",
    )
    args = parser.parse_args()

    # Determine which files to process
    if args.file:
        files = [Path(args.file)]
    elif args.year:
        year_dir = DATA_DIR / args.year
        if not year_dir.exists():
            print(f"No data directory for {args.year}")
            sys.exit(1)
        files = sorted(year_dir.glob("*.jsonl"))
    elif args.all_files:
        files = sorted(DATA_DIR.rglob("*.jsonl"))
    else:
        files = [current_month_path()]

    # Filter by min-year if specified
    if args.min_year:
        files = [f for f in files if f.stem[:4] >= args.min_year]

    # Filter to files that have records needing text
    files_to_process = []
    for f in files:
        recs = load_jsonl(f)
        null_count = sum(1 for r in recs if r.get("text") is None)
        if null_count > 0:
            files_to_process.append((f, null_count, len(recs)))

    if not files_to_process:
        print("No records need text extraction.")
        sys.exit(0)

    total_needing = sum(n for _, n, _ in files_to_process)
    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"Mode: {mode}")
    print(f"Files to process: {len(files_to_process)}")
    print(f"Total records needing text: {total_needing:,}")
    print()

    grand_found = 0
    grand_fetched = 0
    grand_failed = 0
    grand_not_archived = 0
    grand_dates = 0
    grand_relocated = 0

    for i, (path, null_count, total_count) in enumerate(files_to_process, 1):
        print(f"[{i}/{len(files_to_process)}] {path.name}: {null_count:,} to check of {total_count:,}")
        found, fetched, failed, not_archived, dates, relocated, remaining = process_file(
            path, dry_run=args.dry_run, limit=args.limit,
        )

        parts = []
        if args.dry_run:
            parts.append(f"archived={found}")
            parts.append(f"not_archived={not_archived}")
        else:
            parts.append(f"fetched={fetched}")
            if failed:
                parts.append(f"failed={failed}")
            parts.append(f"not_archived={not_archived}")
            if dates:
                parts.append(f"dates_backfilled={dates}")
            if relocated:
                parts.append(f"relocated={relocated}")
        parts.append(f"remaining={remaining}")
        print(f"  {', '.join(parts)}")

        grand_found += found
        grand_fetched += fetched
        grand_failed += failed
        grand_not_archived += not_archived
        grand_dates += dates
        grand_relocated += relocated

    print(f"\nDone.")
    if args.dry_run:
        print(f"  Archived on Wayback: {grand_found:,}")
        print(f"  Not archived: {grand_not_archived:,}")
        pct = grand_found / (grand_found + grand_not_archived) * 100 if (grand_found + grand_not_archived) else 0
        print(f"  Availability rate: {pct:.1f}%")
    else:
        print(f"  Text fetched from Wayback: {grand_fetched:,}")
        print(f"  Fetch failures: {grand_failed:,}")
        print(f"  Not archived: {grand_not_archived:,}")
        print(f"  Dates backfilled: {grand_dates:,}")
        if grand_relocated:
            print(f"  Records relocated: {grand_relocated:,}")


if __name__ == "__main__":
    main()
