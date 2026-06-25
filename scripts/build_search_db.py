#!/usr/bin/env python3
"""Build a SQLite + FTS5 search index from the JSONL press releases.

Produces a single `press.db` that Datasette can serve: full-text search over
title + body text, plus facetable columns (party, chamber, state, member, year).

The JSONL files under data/ are the source of truth; this script derives a
queryable index from them. It is incremental by default to keep daily CI cheap:
only month files modified since the last build are re-read, and only records
whose `updated_at` is newer than the last build are upserted. FTS triggers keep
the index current on each upsert, so unchanged rows are never re-indexed.

Usage:
    uv run python scripts/build_search_db.py            # incremental (default)
    uv run python scripts/build_search_db.py --full     # full rebuild + VACUUM
    uv run python scripts/build_search_db.py --out /tmp/press.db
    uv run python scripts/build_search_db.py --years 2025 2026   # subset (testing)
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import sqlite_utils

from utils import DATA_DIR

DEFAULT_OUT = Path(__file__).resolve().parent.parent / "press.db"

# Columns kept on the flat `releases` table (member.* is flattened in).
FTS_COLUMNS = ["title", "text"]


def transform(rec):
    """Flatten one JSONL record into a flat row dict for the releases table."""
    member = rec.get("member") or {}
    date = rec.get("date") or None
    text = rec.get("text") or None
    return {
        "url": rec.get("url"),
        "title": rec.get("title"),
        "date": date,
        "year": date[:4] if date else None,
        "month": date[:7] if date else None,
        "party": member.get("party"),
        "chamber": member.get("chamber"),
        "state": member.get("state"),
        "member_name": member.get("name"),
        "bioguide_id": member.get("bioguide_id"),
        "domain": rec.get("domain"),
        "scraper": rec.get("scraper"),
        "source": rec.get("source"),
        "date_source": rec.get("date_source"),
        "text": text,
        "has_text": 1 if text else 0,
        "collected_at": rec.get("collected_at"),
        "updated_at": rec.get("updated_at"),
    }


def iter_month_files(years=None):
    """Yield (year, jsonl_path) for every monthly file, optionally filtered."""
    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        if years and year_dir.name not in years:
            continue
        for jsonl_path in sorted(year_dir.glob("*.jsonl")):
            yield year_dir.name, jsonl_path


def iter_rows(paths, since=None):
    """Stream rows from the given JSONL paths.

    If `since` (ISO string) is given, only yield records whose updated_at is
    strictly newer, so incremental builds skip already-indexed rows.
    """
    for path in paths:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not rec.get("url"):
                    continue
                if since and (rec.get("updated_at") or "") <= since:
                    continue
                yield transform(rec)


def ensure_schema(db):
    """Create the releases table, indexes, and FTS once."""
    if db["releases"].exists():
        return
    db["releases"].create(
        {
            "url": str,
            "title": str,
            "date": str,
            "year": str,
            "month": str,
            "party": str,
            "chamber": str,
            "state": str,
            "member_name": str,
            "bioguide_id": str,
            "domain": str,
            "scraper": str,
            "source": str,
            "date_source": str,
            "text": str,
            "has_text": int,
            "collected_at": str,
            "updated_at": str,
        },
        pk="url",
    )
    for col in ["date", "year", "party", "chamber", "state", "bioguide_id", "has_text"]:
        db["releases"].create_index([col], if_not_exists=True)
    # External-content FTS5: indexes title+text without duplicating the body.
    db["releases"].enable_fts(
        FTS_COLUMNS, create_triggers=True, fts_version="FTS5", replace=True
    )


def get_last_build(db):
    if not db["_meta"].exists():
        return None
    row = db["_meta"].get("last_build") if db["_meta"].count else None
    return row["value"] if row else None


def set_last_build(db, value):
    db["_meta"].insert(
        {"key": "last_build", "value": value}, pk="key", replace=True
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output db path")
    parser.add_argument("--full", action="store_true", help="full rebuild + VACUUM")
    parser.add_argument(
        "--years", nargs="*", help="limit to these year dirs (testing/subset)"
    )
    args = parser.parse_args()

    out = args.out
    build_started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # A fresh DB or --full forces a full build.
    full = args.full or not out.exists()
    if full and out.exists():
        out.unlink()

    db = sqlite_utils.Database(out)
    ensure_schema(db)

    last_build = None if full else get_last_build(db)
    last_build_dt = None
    if last_build:
        last_build_dt = datetime.strptime(last_build, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )

    # Select candidate month files. Incremental: only files modified since the
    # last build (collect.py rewrites a whole month file when it changes, so its
    # mtime is a reliable "something changed here" signal).
    candidates = []
    for _year, path in iter_month_files(args.years):
        if last_build_dt is None:
            candidates.append(path)
        else:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if mtime > last_build_dt:
                candidates.append(path)

    mode = "full" if full else "incremental"
    print(f"Build mode: {mode}")
    print(f"  candidate files: {len(candidates)}")

    rows = iter_rows(candidates, since=last_build)
    db["releases"].upsert_all(rows, pk="url", batch_size=2000, analyze=True)

    set_last_build(db, build_started)

    if full:
        print("Optimizing FTS + VACUUM...")
        db["releases"].optimize()  # FTS optimize
        db.conn.commit()  # VACUUM cannot run inside a transaction
        db.vacuum()

    total = db["releases"].count
    with_text = db.execute("select count(*) from releases where has_text = 1").fetchone()[0]
    size_mb = out.stat().st_size / 1_048_576
    print(f"Done. {total:,} rows ({with_text:,} with text), {size_mb:,.1f} MB -> {out}")


if __name__ == "__main__":
    main()
