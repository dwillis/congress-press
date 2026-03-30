#!/usr/bin/env python3
"""Import legacy press releases (2013-2020) from zipped JSON into monthly JSONL files.

Usage:
    uv run python scripts/import_legacy.py /path/to/statements_2013_2020.json.zip
    uv run python scripts/import_legacy.py /path/to/file.zip --dry-run
    uv run python scripts/import_legacy.py /path/to/file.zip --limit 100
"""

import argparse
import json
import sys
import zipfile
from collections import defaultdict
from urllib.parse import urlparse

from utils import DATA_DIR, load_jsonl, now_iso, records_by_url, save_jsonl


def normalize_url(url):
    """Normalize a URL for dedup: lowercase, strip trailing slash, http->https."""
    return url.lower().rstrip("/").replace("http://", "https://")

PARTY_MAP = {
    "D": "Democrat",
    "R": "Republican",
    "REP": "Republican",
    "I": "Independent",
    "ID": "Independent",
    "IND": "Independent",
}


def normalize_party(raw):
    """Normalize party abbreviation to full name."""
    if not raw:
        return None
    return PARTY_MAP.get(raw.strip().upper(), raw.strip())


def month_path_for_key(month_key):
    """Return the JSONL path for a YYYY-MM string."""
    year = month_key[:4]
    year_dir = DATA_DIR / year
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / f"{month_key}.jsonl"


def transform_record(old, timestamp):
    """Transform a legacy record into the congress-press format. Returns None if unusable."""
    url = (old.get("url") or "").strip()
    if not url:
        return None

    date_val = (old.get("date") or "").strip() or None

    # Build member object
    member_id = (old.get("member_id") or "").strip()
    if member_id:
        first = (old.get("first_name") or "").strip()
        last = (old.get("last_name") or "").strip()
        name = f"{first} {last}".strip() if (first or last) else None
        district = (old.get("district") or "").strip()
        member = {
            "bioguide_id": member_id,
            "name": name,
            "party": normalize_party(old.get("party")),
            "state": (old.get("state") or "").strip() or None,
            "chamber": "House" if district else "Senate",
        }
    else:
        member = None

    domain = ""
    try:
        domain = urlparse(url).netloc
    except Exception:
        pass

    body = old.get("body")
    text = body.strip() if body else None

    return {
        "url": url,
        "title": (old.get("title") or "").strip(),
        "date": date_val,
        "date_source": "legacy" if date_val else None,
        "source": None,
        "domain": domain,
        "scraper": None,
        "member": member,
        "text": text if text else None,
        "collected_at": timestamp,
        "updated_at": timestamp,
    }


def main():
    parser = argparse.ArgumentParser(description="Import legacy press releases from zip")
    parser.add_argument("zipfile", type=str, help="Path to the zip file")
    parser.add_argument("--dry-run", action="store_true", help="Transform and report but don't write")
    parser.add_argument("--limit", type=int, default=0, help="Process only first N records (0 = all)")
    args = parser.parse_args()

    timestamp = now_iso()

    # Load existing data for dedup
    print("Loading existing data for dedup...")
    month_records = defaultdict(dict)  # {path_str: {url: record}}
    existing_urls = set()

    for jsonl_path in DATA_DIR.rglob("*.jsonl"):
        records = load_jsonl(jsonl_path)
        by_url = records_by_url(records)
        month_records[str(jsonl_path)] = by_url
        for url in by_url:
            existing_urls.add(normalize_url(url))

    print(f"  Existing records: {len(existing_urls)}")

    # Read the zip
    print(f"Reading {args.zipfile}...")
    with zipfile.ZipFile(args.zipfile) as zf:
        names = zf.namelist()
        if not names:
            print("Error: zip file is empty")
            sys.exit(1)
        print(f"  Found: {names[0]}")
        with zf.open(names[0]) as f:
            data = json.load(f)

    legacy_records = data.get("RECORDS", data if isinstance(data, list) else [])
    total = len(legacy_records)
    print(f"  Total legacy records: {total}")

    if args.limit:
        legacy_records = legacy_records[: args.limit]
        print(f"  Limited to: {len(legacy_records)}")

    # Transform and bucket
    new_count = 0
    skip_no_url = 0
    skip_dup = 0
    skip_no_date = 0

    for old in legacy_records:
        record = transform_record(old, timestamp)

        if record is None:
            skip_no_url += 1
            continue

        if normalize_url(record["url"]) in existing_urls:
            skip_dup += 1
            continue

        if not record["date"]:
            skip_no_date += 1
            continue

        month_key = record["date"][:7]  # YYYY-MM
        path = str(month_path_for_key(month_key))

        month_records[path][record["url"]] = record
        existing_urls.add(normalize_url(record["url"]))
        new_count += 1

    # Write output
    files_written = 0
    if not args.dry_run:
        print("Writing files...")
        for path, url_dict in month_records.items():
            if url_dict:
                save_jsonl(path, list(url_dict.values()))
                files_written += 1
    else:
        files_written = sum(1 for d in month_records.values() if d)

    # Summary
    print(f"\n{'DRY RUN - ' if args.dry_run else ''}Import complete")
    print(f"  Legacy records processed: {len(legacy_records)}")
    print(f"  New records: {new_count}")
    print(f"  Skipped (no URL): {skip_no_url}")
    print(f"  Skipped (duplicate): {skip_dup}")
    print(f"  Skipped (no date): {skip_no_date}")
    print(f"  Files {'would be ' if args.dry_run else ''}written: {files_written}")


if __name__ == "__main__":
    main()
