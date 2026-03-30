#!/usr/bin/env python3
"""Phase 2: Fetch full text for press releases that need it.

Reads the current month's JSONL file, finds records with null text or updated
titles, fetches article text via newspaper4k, and writes back. Optionally
backfills dates from article metadata when the scraper didn't provide one.
"""

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

from utils import (
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
    if record.get("text") is None:
        return True
    # Title was updated (text set to null by phase 1)
    if record.get("updated_at") != record.get("collected_at") and record.get("text") is None:
        return True
    return False


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
        help="Limit number of articles to fetch (0 = no limit)",
    )
    args = parser.parse_args()

    path = current_month_path()
    records = load_jsonl(path)

    if not records:
        print("No records found. Run collect_metadata.py first.")
        sys.exit(0)

    # Find records needing text
    to_fetch = [r for r in records if needs_text(r)]

    if args.retry_failures:
        # Include all null-text records, even if collected_at == updated_at
        to_fetch = [r for r in records if r.get("text") is None]

    if args.limit > 0:
        to_fetch = to_fetch[: args.limit]

    print(f"Records needing text: {len(to_fetch)} of {len(records)} total")

    if not to_fetch:
        print("Nothing to fetch.")
        sys.exit(0)

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
                    # Replace a bad future date with a good one
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

    print(f"\nResults saved to {path}")
    print(f"  Text fetched: {fetched_count}")
    print(f"  Fetch failures: {failed_count}")
    print(f"  Dates backfilled: {date_backfill_count}")
    if relocated_count:
        print(f"  Records relocated to correct month: {relocated_count} -> {len(relocate)} files")

    remaining_null = sum(1 for r in stay if r.get("text") is None)
    print(f"  Records still without text: {remaining_null}")


if __name__ == "__main__":
    main()
