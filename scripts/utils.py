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


def load_all_urls():
    """Load the set of all URLs across every monthly JSONL file.

    Used by collect_metadata.py to deduplicate against the entire dataset,
    not just the current month.
    """
    urls = set()
    for jsonl_path in DATA_DIR.rglob("*.jsonl"):
        with open(jsonl_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    url = record.get("url")
                    if url:
                        urls.add(url)
    return urls


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
    """Build scraper_name -> member info dict from legislators JSON.

    The legislators file stores a template method name in scraper_method, not the
    scraper's key name in SCRAPER_CONFIG. We match by URL/domain instead to build
    a reliable scraper_name -> member mapping.
    """
    with open(path) as f:
        legislators = json.load(f)

    # Build domain -> legislator lookup (strip scheme and trailing slash)
    domain_to_leg = {}
    for leg in legislators:
        url = leg.get("url", "")
        if url:
            domain = url.replace("https://", "").replace("http://", "").rstrip("/").lower()
            domain_to_leg[domain] = leg

    # Cross-reference SCRAPER_CONFIG scraper names with legislator URLs
    try:
        from python_statement.config import SCRAPER_CONFIG
    except ImportError:
        SCRAPER_CONFIG = {}

    member_map = {}
    for scraper_name, config in SCRAPER_CONFIG.items():
        url_base = config.get("url_base", "") if isinstance(config, dict) else ""
        if not url_base:
            continue
        domain = url_base.replace("https://", "").replace("http://", "").split("/")[0].lower()
        leg = domain_to_leg.get(domain)
        if not leg:
            # Try with www. prefix stripped
            domain_no_www = domain.removeprefix("www.")
            leg = next(
                (v for k, v in domain_to_leg.items() if k.removeprefix("www.") == domain_no_www),
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


def month_path(date_str):
    """Return the JSONL path for a YYYY-MM-DD date string: data/YYYY/YYYY-MM.jsonl."""
    year = date_str[:4]
    month_key = date_str[:7]
    year_dir = DATA_DIR / year
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / f"{month_key}.jsonl"


def is_future_date(date_str):
    """Return True if date_str is in the future (after today)."""
    if not date_str:
        return False
    try:
        return date_str > str(date.today())
    except (TypeError, ValueError):
        return False


def now_iso():
    """Return current UTC time as ISO string."""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
