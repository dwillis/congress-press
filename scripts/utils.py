"""Shared helpers for JSONL I/O, deduplication, and member lookup."""

import json
import os
from datetime import date, datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def current_month_path():
    """Return the JSONL path for the current month: data/YYYY/YYYY-MM.jsonl."""
    today = date.today()
    year_dir = DATA_DIR / str(today.year)
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / f"{today.year}-{today.month:02d}.jsonl"


def load_jsonl(path):
    """Load a JSONL file into a list of dicts. Returns [] if file doesn't exist."""
    if not os.path.exists(path):
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def save_jsonl(path, records):
    """Write records to a JSONL file, sorted by (date, url) for stable diffs."""
    def sort_key(r):
        d = r.get("date") or ""
        u = r.get("url") or ""
        return (d, u)

    sorted_records = sorted(records, key=sort_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for record in sorted_records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def records_by_url(records):
    """Index records by URL for fast dedup lookup."""
    return {r["url"]: r for r in records}


def load_member_map():
    """Load scraper_name -> member info mapping.

    Reads legislators_with_scrapers.json from the python-statement package.
    Falls back to checking common locations if the package path isn't found.
    """
    # Try to find it via the python-statement package location
    try:
        import python_statement
        pkg_dir = Path(python_statement.__file__).resolve().parent.parent
        leg_path = pkg_dir / "legislators_with_scrapers.json"
        if leg_path.exists():
            return _build_member_map(leg_path)
    except ImportError:
        pass

    # Fallback: check sibling directory
    sibling = Path(__file__).resolve().parent.parent.parent / "python-statement" / "legislators_with_scrapers.json"
    if sibling.exists():
        return _build_member_map(sibling)

    # Fallback: check local copy
    local = Path(__file__).resolve().parent.parent / "legislators_with_scrapers.json"
    if local.exists():
        return _build_member_map(local)

    print("Warning: legislators_with_scrapers.json not found. Member info will be null.")
    return {}


def _build_member_map(path):
    """Build scraper_name -> member info dict from legislators JSON."""
    with open(path) as f:
        legislators = json.load(f)

    member_map = {}
    for leg in legislators:
        scraper = leg.get("scraper_method")
        if not scraper:
            continue
        member_map[scraper] = {
            "bioguide_id": leg.get("bioguide"),
            "name": leg.get("official_full"),
            "party": leg.get("party"),
            "state": leg.get("state"),
            "chamber": "Senate" if leg.get("type") == "sen" else "House",
        }
    return member_map


def now_iso():
    """Return current UTC time as ISO string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
