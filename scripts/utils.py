"""Shared helpers for JSONL I/O, deduplication, and member lookup."""

import json
import os
import re
from datetime import date, datetime
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
# Committee releases are a separate collection with the same monthly layout:
# data/committees/YYYY/YYYY-MM.jsonl. Builders that only walk digit-named
# directories under data/ skip it unless they opt in.
COMMITTEE_DIR = DATA_DIR / "committees"


def collection_root(path):
    """Return COMMITTEE_DIR if path lives in the committee tree, else DATA_DIR."""
    try:
        Path(path).resolve().relative_to(COMMITTEE_DIR.resolve())
        return COMMITTEE_DIR
    except ValueError:
        return DATA_DIR


def current_month_path(root=DATA_DIR):
    """Return the JSONL path for the current month: <root>/YYYY/YYYY-MM.jsonl."""
    today = date.today()
    year_dir = Path(root) / str(today.year)
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


def _find_python_statement_file(filename):
    """Locate a JSON file published at the root of the python-statement repo.

    Checks the installed package's parent, a sibling checkout, then a local copy
    (CI downloads one into the repo root).
    """
    candidates = []
    try:
        import python_statement
        candidates.append(Path(python_statement.__file__).resolve().parent.parent / filename)
    except ImportError:
        pass
    root = Path(__file__).resolve().parent.parent
    candidates.append(root.parent / "python-statement" / filename)
    candidates.append(root / filename)
    return next((c for c in candidates if c.exists()), None)


COMMITTEE_CHAMBERS = {"house": "House", "senate": "Senate", "joint": "Joint"}


def load_committee_map():
    """Load scraper_name -> {thomas_id, name, chamber} for committee scrapers.

    Reads committees_with_scrapers.json from python-statement. Scrapers missing
    from that file still get thomas_id from COMMITTEE_CONFIG and a chamber
    guessed from the thomas_id prefix (H/S/J).
    """
    try:
        from python_statement.committee_config import COMMITTEE_CONFIG
    except ImportError:
        COMMITTEE_CONFIG = {}

    by_thomas = {}
    path = _find_python_statement_file("committees_with_scrapers.json")
    if path:
        with open(path) as f:
            for c in json.load(f):
                by_thomas[c["thomas_id"]] = c
    else:
        print("Warning: committees_with_scrapers.json not found. Committee names will be null.")

    prefix_chamber = {"H": "House", "S": "Senate", "J": "Joint"}
    committee_map = {}
    for name, config in COMMITTEE_CONFIG.items():
        thomas_id = config["thomas_id"]
        info = by_thomas.get(thomas_id, {})
        committee_map[name] = {
            "thomas_id": thomas_id,
            "name": info.get("name"),
            "chamber": COMMITTEE_CHAMBERS.get(info.get("type"), prefix_chamber.get(thomas_id[:1])),
        }
    return committee_map


# Party control of each chamber, as (start_date, party) transitions. A date
# belongs to the last transition on or before it.
# TODO(120th Congress): add ("2027-01-03", "<party>") to each chamber once the
# 2026 results are settled. Until then majority_party() raises for dates on or
# after 2027-01-03 rather than guessing.
CHAMBER_CONTROL = {
    "House": [
        ("2001-01-03", "Republican"),  # 107th-109th
        ("2007-01-04", "Democrat"),    # 110th-111th
        ("2011-01-05", "Republican"),  # 112th-115th
        ("2019-01-03", "Democrat"),    # 116th-117th
        ("2023-01-03", "Republican"),  # 118th-119th
    ],
    "Senate": [
        ("2001-01-03", "Democrat"),    # 107th: 50-50, VP Gore until Jan 20
        ("2001-01-20", "Republican"),  # 50-50, VP Cheney
        ("2001-06-06", "Democrat"),    # Jeffords switch
        ("2003-01-07", "Republican"),  # 108th-109th
        ("2007-01-04", "Democrat"),    # 110th-113th
        ("2015-01-06", "Republican"),  # 114th-116th
        ("2021-01-20", "Democrat"),    # 117th: 50-50, VP Harris
        ("2025-01-03", "Republican"),  # 119th
    ],
}
CONTROL_KNOWN_UNTIL = "2027-01-03"  # start of the 120th Congress


def majority_party(chamber, date_str):
    """Party holding `chamber` ('House'/'Senate') on a YYYY-MM-DD date.

    Returns None for dates before the table starts or for other chambers.
    Raises ValueError for dates past CONTROL_KNOWN_UNTIL.
    """
    transitions = CHAMBER_CONTROL.get(chamber)
    if not transitions or not date_str or date_str < transitions[0][0]:
        return None
    if date_str >= CONTROL_KNOWN_UNTIL:
        raise ValueError(
            f"Chamber control after {CONTROL_KNOWN_UNTIL} is unknown: add the 120th "
            f"Congress to CHAMBER_CONTROL in scripts/utils.py"
        )
    party = None
    for start, p in transitions:
        if date_str >= start:
            party = p
    return party


def other_party(party):
    return {"Democrat": "Republican", "Republican": "Democrat"}.get(party)


# url_base words that tie a committee press page to a party.
_PARTY_WORDS = [
    (re.compile(r"democrat|dems"), "Democrat"),
    (re.compile(r"republican|gop"), "Republican"),
]
# Manual corrections: scraper_name -> party name, for press pages whose party
# can't be read from the URL or the side label.
PARTY_OVERRIDES = {}


def site_party(scraper_name, url_base):
    """Party a press page's URL names (or an override gives), else None."""
    if scraper_name in PARTY_OVERRIDES:
        return PARTY_OVERRIDES[scraper_name]
    url = (url_base or "").lower()
    for pattern, party in _PARTY_WORDS:
        if pattern.search(url):
            return party
    return None


def committee_attribution(scraper_name, config, committee_info, date_str, collected_date=None):
    """Build the `committee` object for a committee record.

    A committee press page's archive belongs to the party whose staff run it.
    When control flips, Senate sites swap which party sits under their
    majority/minority pages and House sites move archives between the main
    domain and democrats-/republicans- subdomains. Old releases go with them:
    Senate Rules' majority page today holds Pat Roberts' 2013 releases from
    when he was ranking member. So:

    - party: what the press page's label means when it was scraped. That's a
      party named in the URL, otherwise the configured side combined with
      chamber control on collected_date (default today).
    - side: from party and chamber control on the release date. A 2021
      Klobuchar release is majority even though it now sits on the minority page.

    'full' press pages (one shared site) get party None. Joint committees keep
    the configured side, since control can split between chambers. Undated
    records keep the configured side; collect_text re-attributes them once it
    finds a date.
    """
    info = committee_info or {}
    chamber = info.get("chamber")
    config_side = config.get("side")
    collected = collected_date or str(date.today())

    side, party, basis = config_side, None, None
    if config_side != "full":
        party = site_party(scraper_name, config.get("url_base"))
        if party:
            basis = "site"
        elif chamber in ("House", "Senate"):
            majority_then = majority_party(chamber, collected)
            if majority_then:
                party = majority_then if config_side == "majority" else other_party(majority_then)
                basis = "label"
        if party and date_str and chamber in ("House", "Senate"):
            majority = majority_party(chamber, date_str)
            if majority:
                side = "majority" if majority == party else "minority"

    return {
        "thomas_id": info.get("thomas_id") or config.get("thomas_id"),
        "name": info.get("name"),
        "chamber": chamber,
        "side": side,
        "party": party,
        "party_basis": basis,
    }


_committee_lookup = None


def refresh_committee(record):
    """Recompute a committee record's side/party from its scraper and date.

    Call after a record's date changes (e.g. collect_text backfilled it).
    No-op for member records or scrapers no longer in COMMITTEE_CONFIG.
    """
    global _committee_lookup
    if not record.get("committee"):
        return
    if _committee_lookup is None:
        try:
            from python_statement.committee_config import COMMITTEE_CONFIG
        except ImportError:
            COMMITTEE_CONFIG = {}
        _committee_lookup = (COMMITTEE_CONFIG, load_committee_map())
    configs, committee_map = _committee_lookup
    name = record.get("scraper")
    if name not in configs:
        return
    collected = (record.get("collected_at") or "")[:10] or None
    record["committee"] = committee_attribution(
        name, configs[name], committee_map.get(name), record.get("date"), collected
    )


def month_path(date_str, root=DATA_DIR):
    """Return the JSONL path for a YYYY-MM-DD date string: <root>/YYYY/YYYY-MM.jsonl."""
    year = date_str[:4]
    month_key = date_str[:7]
    year_dir = Path(root) / year
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
