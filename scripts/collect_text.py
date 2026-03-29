#!/usr/bin/env python3
"""Phase 2: Fetch full text for press releases that need it.

Reads the current month's JSONL file, finds records with null text or updated
titles, fetches article text via newspaper4k, and writes back. Optionally
backfills dates from article metadata when the scraper didn't provide one.
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

from utils import current_month_path, load_jsonl, now_iso, save_jsonl

WORKERS = 5
DELAY = 0.5  # seconds between fetches per worker


def fetch_article(url):
    """Fetch and parse an article, returning (text, publish_date, error)."""
    try:
        from newspaper import Article

        article = Article(url)
        article.download()
        article.parse()
        text = article.text.strip() if article.text else None
        pub_date = None
        if article.publish_date:
            pd = article.publish_date
            if isinstance(pd, datetime):
                pub_date = str(pd.date())
            elif isinstance(pd, date):
                pub_date = str(pd)
            else:
                pub_date = str(pd)[:10]  # fallback: take YYYY-MM-DD prefix
        return text, pub_date, None
    except Exception as e:
        return None, None, str(e)


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
        text, pub_date, error = fetch_article(url)
        return url, text, pub_date, error

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_with_delay, r): r for r in to_fetch}

        for future in as_completed(futures):
            url, text, pub_date, error = future.result()
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

            # Date backfill: only when scraper didn't provide a date
            if record.get("date") is None and pub_date:
                record["date"] = pub_date
                record["date_source"] = "newspaper4k"
                date_backfill_count += 1

    save_jsonl(path, records)

    print(f"\nResults saved to {path}")
    print(f"  Text fetched: {fetched_count}")
    print(f"  Fetch failures: {failed_count}")
    print(f"  Dates backfilled: {date_backfill_count}")

    remaining_null = sum(1 for r in records if r.get("text") is None)
    print(f"  Records still without text: {remaining_null}")


if __name__ == "__main__":
    main()
