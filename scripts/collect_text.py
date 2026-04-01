#!/usr/bin/env python3
"""Phase 2: Fetch full text for press releases that need it.

Reads JSONL files, finds records with null text, fetches article text via
newspaper4k, and writes back. Optionally backfills dates from article metadata
when the scraper didn't provide one.

Usage:
    uv run python scripts/collect_text.py                    # Current month only
    uv run python scripts/collect_text.py --all-files        # All files with null text
    uv run python scripts/collect_text.py --file data/2023/2023-06.jsonl
    uv run python scripts/collect_text.py --retry-failures   # Re-attempt null-text records
"""

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

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

WORKERS = 5
DELAY = 0.5  # seconds between fetches per worker


def extract_date_from_html(html):
    """Extract a publish date from page HTML using meta tags and common selectors.

    Tries (in order):
    1. <meta property="article:published_time">
    2. <meta name="date">
    3. <time datetime="..."> attribute
    4. Elements with class "date" or "press-date" containing a parseable date

    Returns (date_str, is_partial) where date_str is "YYYY-MM-DD" or "MM-DD",
    and is_partial is True if only month/day were found.
    """
    from bs4 import BeautifulSoup
    from dateutil import parser as dateutil_parser

    soup = BeautifulSoup(html, "html.parser")

    # 1. Meta tags (most reliable)
    for attr, val in [
        ("property", "article:published_time"),
        ("name", "date"),
        ("name", "pubdate"),
        ("property", "og:article:published_time"),
    ]:
        meta = soup.find("meta", attrs={attr: val})
        if meta and meta.get("content"):
            content = meta["content"].strip()
            try:
                parsed = dateutil_parser.parse(content)
                return str(parsed.date()), False
            except (ValueError, OverflowError):
                continue

    # 2. <time datetime="..."> attribute
    time_el = soup.find("time", attrs={"datetime": True})
    if time_el:
        try:
            parsed = dateutil_parser.parse(time_el["datetime"])
            if parsed.year > 1900:
                return str(parsed.date()), False
        except (ValueError, OverflowError):
            pass

    # 3. Elements with date-like classes
    for selector in [".date", ".press-date", ".newsie_details_date", "span.date"]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            # Try full date (with year)
            full_match = re.search(
                r"((?:January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+\d{1,2},?\s+\d{4})",
                text,
            )
            if full_match:
                try:
                    parsed = dateutil_parser.parse(full_match.group(1))
                    return str(parsed.date()), False
                except (ValueError, OverflowError):
                    pass
            # Try partial date (month and day only)
            partial_match = re.search(
                r"((?:January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+\d{1,2})\b",
                text,
            )
            if partial_match:
                try:
                    parsed = dateutil_parser.parse(partial_match.group(1))
                    return f"{parsed.month:02d}-{parsed.day:02d}", True
                except (ValueError, OverflowError):
                    pass

    return None, False


def fetch_article(url):
    """Fetch and parse an article, returning (text, publish_date, date_partial, error).

    publish_date is a full "YYYY-MM-DD" string or None.
    date_partial is a "MM-DD" string when only month/day were found, or None.
    """
    try:
        from newspaper import Article

        article = Article(url)
        article.download()
        article.parse()
        text = article.text.strip() if article.text else None

        # Try extracting date from raw HTML first (more reliable)
        pub_date = None
        date_partial = None
        if article.html:
            extracted, is_partial = extract_date_from_html(article.html)
            if extracted and not is_partial:
                pub_date = extracted
            elif extracted and is_partial:
                date_partial = extracted

        # Fall back to newspaper4k's date extraction
        if not pub_date and article.publish_date:
            pd = article.publish_date
            if isinstance(pd, datetime):
                pub_date = str(pd.date())
            elif isinstance(pd, date):
                pub_date = str(pd)
            else:
                pub_date = str(pd)[:10]

        return text, pub_date, date_partial, None
    except Exception as e:
        return None, None, None, str(e)


def needs_text(record):
    """Check if a record needs text extraction."""
    return record.get("text") is None


def process_file(path, retry_failures=False, limit=0):
    """Process a single JSONL file: fetch text for records that need it.

    Returns (fetched_count, failed_count, date_backfill_count, relocated_count, remaining_null).
    """
    path = Path(path)
    records = load_jsonl(path)

    if not records:
        return 0, 0, 0, 0, 0

    # Find records needing text
    to_fetch = [r for r in records if needs_text(r)]

    if not to_fetch:
        return 0, 0, 0, 0, 0

    if limit > 0:
        to_fetch = to_fetch[:limit]

    fetched_count = 0
    failed_count = 0
    date_backfill_count = 0
    timestamp = now_iso()

    # Build URL -> record index for fast updates
    url_to_idx = {r["url"]: i for i, r in enumerate(records)}

    def fetch_with_delay(record):
        time.sleep(DELAY)
        url = record["url"]
        text, pub_date, date_partial, error = fetch_article(url)
        return url, text, pub_date, date_partial, error

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_with_delay, r): r for r in to_fetch}

        for future in as_completed(futures):
            url, text, pub_date, date_partial, error = future.result()
            idx = url_to_idx.get(url)
            if idx is None:
                continue

            record = records[idx]

            if error:
                failed_count += 1
                continue

            if text:
                record["text"] = text
                record["updated_at"] = timestamp
                fetched_count += 1

            # Date backfill: fill in missing or future (bad) dates
            if pub_date and not is_future_date(pub_date):
                if record.get("date") is None:
                    record["date"] = pub_date
                    record["date_source"] = "page_html"
                    date_backfill_count += 1
                elif is_future_date(record.get("date")):
                    record["date"] = pub_date
                    record["date_source"] = "page_html"
                    date_backfill_count += 1

            # Store partial date (month-day) when no full date is available
            if record.get("date") is None and date_partial:
                record["date_partial"] = date_partial

    # Separate records that need to move to a different monthly file
    file_month = path.stem  # e.g. "2026-03"
    stay = []
    relocate = {}  # {target_path: [records]}
    for r in records:
        record_month = r["date"][:7] if r.get("date") else None
        if record_month and record_month != file_month:
            target = str(month_path(r["date"]))
            relocate.setdefault(target, []).append(r)
        else:
            stay.append(r)

    save_jsonl(path, stay)

    # Merge relocated records into their target files
    relocated_count = 0
    for target_path, moved_records in relocate.items():
        existing = load_jsonl(target_path)
        by_url = records_by_url(existing)
        for r in moved_records:
            by_url[r["url"]] = r
            relocated_count += 1
        save_jsonl(target_path, list(by_url.values()))

    remaining_null = sum(1 for r in stay if r.get("text") is None)
    return fetched_count, failed_count, date_backfill_count, relocated_count, remaining_null


def main():
    parser = argparse.ArgumentParser(description="Fetch full text for press releases")
    parser.add_argument(
        "--retry-failures",
        action="store_true",
        help="Re-attempt all records with null text (including prior failures)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of articles to fetch per file (0 = no limit)",
    )
    parser.add_argument(
        "--file",
        type=str,
        help="Process a specific JSONL file",
    )
    parser.add_argument(
        "--all-files",
        action="store_true",
        help="Process all JSONL files that have records needing text",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Process all files for a specific year (e.g., 2023)",
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

    # Filter to files that actually have records needing text
    files_to_process = []
    for f in files:
        records = load_jsonl(f)
        null_count = sum(1 for r in records if r.get("text") is None)
        if null_count > 0:
            files_to_process.append((f, null_count, len(records)))

    if not files_to_process:
        print("No records need text extraction.")
        sys.exit(0)

    total_needing = sum(n for _, n, _ in files_to_process)
    print(f"Files to process: {len(files_to_process)}")
    print(f"Total records needing text: {total_needing:,}")
    print()

    grand_fetched = 0
    grand_failed = 0
    grand_dates = 0
    grand_relocated = 0

    for i, (path, null_count, total_count) in enumerate(files_to_process, 1):
        print(f"[{i}/{len(files_to_process)}] {path.name}: {null_count:,} to fetch of {total_count:,}")
        fetched, failed, dates, relocated, remaining = process_file(
            path, retry_failures=args.retry_failures, limit=args.limit,
        )
        print(f"  fetched={fetched}, failed={failed}, dates_backfilled={dates}", end="")
        if relocated:
            print(f", relocated={relocated}", end="")
        print(f", remaining={remaining}")

        grand_fetched += fetched
        grand_failed += failed
        grand_dates += dates
        grand_relocated += relocated

    print(f"\nDone.")
    print(f"  Total text fetched: {grand_fetched:,}")
    print(f"  Total fetch failures: {grand_failed:,}")
    print(f"  Total dates backfilled: {grand_dates:,}")
    if grand_relocated:
        print(f"  Total records relocated: {grand_relocated:,}")


if __name__ == "__main__":
    main()
