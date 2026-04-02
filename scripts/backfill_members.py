#!/usr/bin/env python3
"""Backfill null member fields in JSONL files using URL-based domain matching.

Usage:
    uv run python backfill_members.py                   # fix current month
    uv run python backfill_members.py data/2026/2026-04.jsonl
    uv run python backfill_members.py data/2026/*.jsonl
"""

import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_member_map():
    """Build scraper_name -> member info by matching SCRAPER_CONFIG URLs to legislators."""
    leg_path = _find_legislators_file()
    if not leg_path:
        print("ERROR: legislators_with_scrapers.json not found.")
        sys.exit(1)

    with open(leg_path) as f:
        legislators = json.load(f)

    domain_to_leg = {}
    for leg in legislators:
        url = leg.get("url", "")
        if url:
            domain = url.replace("https://", "").replace("http://", "").rstrip("/").lower()
            domain_to_leg[domain] = leg

    try:
        from python_statement.config import SCRAPER_CONFIG
    except ImportError:
        print("ERROR: python_statement not installed.")
        sys.exit(1)

    member_map = {}
    for scraper_name, config in SCRAPER_CONFIG.items():
        url_base = config.get("url_base", "") if isinstance(config, dict) else ""
        if not url_base:
            continue
        domain = url_base.replace("https://", "").replace("http://", "").split("/")[0].lower()
        leg = domain_to_leg.get(domain) or next(
            (v for k, v in domain_to_leg.items() if k.removeprefix("www.") == domain.removeprefix("www.")),
            None,
        )
        if leg:
            member_map[scraper_name] = {
                "bioguide_id": leg.get("bioguide"),
                "name": leg.get("official_full"),
                "party": leg.get("party"),
                "state": leg.get("state"),
                "chamber": "Senate" if leg.get("type") == "sen" else "House",
            }
    return member_map


def _find_legislators_file():
    try:
        import python_statement
        p = Path(python_statement.__file__).resolve().parent.parent / "legislators_with_scrapers.json"
        if p.exists():
            return p
    except ImportError:
        pass
    for candidate in [
        Path(__file__).resolve().parent.parent.parent / "python-statement" / "legislators_with_scrapers.json",
        Path(__file__).resolve().parent.parent / "legislators_with_scrapers.json",
    ]:
        if candidate.exists():
            return candidate
    return None


def fix_file(path, member_map):
    path = Path(path)
    if not path.exists():
        print(f"  SKIP (not found): {path}")
        return

    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    fixed = 0
    still_null = []
    for r in records:
        if r.get("member") is None:
            scraper = r.get("scraper")
            member = member_map.get(scraper)
            if member:
                r["member"] = member
                fixed += 1
            else:
                still_null.append(scraper)

    if fixed == 0 and not still_null:
        print(f"  OK (no nulls): {path}")
        return

    def sort_key(r):
        return (r.get("date") or "", r.get("url") or "")

    records.sort(key=sort_key)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"  Fixed {fixed} records in {path}")
    if still_null:
        print(f"    Still null (no match): {sorted(set(still_null))}")


def main():
    member_map = load_member_map()
    print(f"Loaded {len(member_map)} scrapers in member map.")

    if len(sys.argv) > 1:
        paths = sys.argv[1:]
    else:
        from datetime import date
        today = date.today()
        paths = [DATA_DIR / str(today.year) / f"{today.year}-{today.month:02d}.jsonl"]

    for path in paths:
        fix_file(path, member_map)


if __name__ == "__main__":
    main()
