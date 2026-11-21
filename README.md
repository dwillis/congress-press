# congress-press

A collection of more than 670,000 congressional press releases from 2001 to the present, including full text, with daily updates powered by [python-statement](https://github.com/dwillis/python-statement).

## Data

The dataset contains press releases from over 860 members of Congress across both chambers. Data is stored as JSONL files organized by month in `data/YYYY/YYYY-MM.jsonl`.

Committee press releases from House, Senate and joint committee websites (majority and minority pages, or one shared site) are a separate collection in `data/committees/YYYY/YYYY-MM.jsonl`. See [Committee releases](#committee-releases).

Records come from two sources:

- **Legacy import** (2001-2020) - ~486,000 press releases with full text from the ProPublica Congress API
- **Scraper collection** (2020-present) - ~187,000 press releases collected via automated scrapers, with text extracted from individual pages

**IMPORTANT** this is not a comprehensive collection, mostly owing to departed members and periods when scraper coverage was incomplete. As of March 30, 2026, all current members with official websites are covered. See Caveats below for more details.

## How it works

A GitHub Actions workflow runs daily at 5am UTC in two phases:

1. **Metadata collection** - Scrapes press release listings (title, URL, date) from 536 member websites and 84 committee press pages
2. **Text extraction** - Fetches full article text using newspaper4k for new/changed releases, and backfills dates from individual page HTML when the listing page doesn't include the year

## Record format

Each line in a JSONL file is one press release:

```json
{
  "url": "https://pelosi.house.gov/news/press-releases/...",
  "title": "Pelosi Statement on ...",
  "date": "2026-03-29",
  "date_source": "scraper",
  "source": "https://pelosi.house.gov/media/press-releases",
  "domain": "pelosi.house.gov",
  "scraper": "pelosi",
  "member": {
    "bioguide_id": "P000197",
    "name": "Nancy Pelosi",
    "party": "Democrat",
    "state": "CA",
    "chamber": "House"
  },
  "text": "Full article body text...",
  "collected_at": "2026-03-29T05:00:00Z",
  "updated_at": "2026-03-29T05:00:00Z"
}
```

The `date_source` field indicates where the date came from: `scraper` (listing page), `page_html` (extracted from the individual release page), `legacy` (historical import), or `newspaper4k` (article metadata).

## Committee releases

Committee records use the same format, with `member: null` and a `committee` object:

```json
{
  "url": "https://www.rules.senate.gov/news/minority-news/...",
  "scraper": "ssra_minority",
  "member": null,
  "committee": {
    "thomas_id": "SSRA",
    "name": "Senate Committee on Rules and Administration",
    "chamber": "Senate",
    "side": "majority",
    "party": "Democrat",
    "party_basis": "label"
  },
  "...": "other fields as above"
}
```

`thomas_id` matches [`committees-current.yaml`](https://github.com/unitedstates/congress-legislators/blob/main/committees-current.yaml). `chamber` is `House`, `Senate` or `Joint`.

`side` and `party` describe the release on its own date, not the page it was found on. A committee press page's archive moves with the party staff who run it. When control flips, Senate sites swap which party sits under their majority/minority pages, and House archives move between the main committee domain and the `democrats-`/`republicans-` subdomains. So, for example, Chairwoman Klobuchar's 2021 releases now sit on the Rules Committee's *minority* page. Each record gets:

- `party`: the party the press page belonged to when it was scraped. That's read from the URL when it names a party (`party_basis: "site"`), otherwise from its majority/minority label and chamber control on the collection date (`party_basis: "label"`).
- `side`: `majority` if that party controlled the chamber on the release date, otherwise `minority`. Shared committee sites are `full` with `party: null`. Joint committees keep the press page's current majority/minority label.

Chamber control by date lives in `CHAMBER_CONTROL` in `scripts/utils.py`. It needs an entry for each new Congress. Corrections for individual press pages go in `PARTY_OVERRIDES`.

## Caveats

- **Missing text**: Some records have `"text": null` because the original page has been removed (404) or is behind JavaScript rendering. These records still have valid metadata (title, date, member).
- **Date accuracy**: Some congressional websites display dates without a year on their listing pages. In these cases the date is extracted from the individual release page during text collection. Records where no year could be determined have `"date": null` and may include a `"date_partial"` field with the month and day (e.g., `"12-02"`).
- **Legacy data differences**: Records from the ProPublica import (`date_source: "legacy"`) have `"scraper": null` and `"source": null` since they predate the current scraper infrastructure. Member information was constructed from the original dataset fields rather than the current legislators file.
- **Duplicate press releases**: Some members post the same press release on multiple pages or under different URLs. Deduplication is by URL only, so substantively identical releases with different URLs will both appear.
- **HTML artifacts**: Full text is extracted automatically and may contain remnants of page navigation, boilerplate footers, or formatting artifacts from the original HTML.

## Running locally

```bash
uv sync
uv run python scripts/collect_metadata.py
uv run python scripts/collect_text.py
uv run python scripts/collect_text.py --retry-failures  # re-attempt null-text records
```

### Backfilling historical releases

```bash
uv run python scripts/backfill.py                    # All scrapers, all pages
uv run python scripts/backfill.py --scraper pelosi   # Single scraper
uv run python scripts/backfill.py --max-pages 10     # Limit pagination depth
uv run python scripts/backfill.py --resume            # Skip scrapers already in data

# Committees (written to data/committees/; pages up to 500 deep by default)
uv run python scripts/backfill.py --collection committees --workers 4
uv run python scripts/backfill.py --collection committees --committee HSAG
uv run python scripts/collect_text.py --all-files --committees
```

## Contributing

There are two main ways to contribute:

### Scraper issues (python-statement)

If you notice a member's press releases are missing, returning bad dates, or producing broken URLs, file an issue at [python-statement](https://github.com/dwillis/python-statement/issues). Helpful details include:

- The member's name and website URL
- What's wrong (missing entirely, wrong dates, broken links, etc.)
- A sample URL showing the expected content

### Data quality (congress-press)

For issues with the collected data itself — duplicates, bad text extraction, misattributed records, or suggestions for the collection scripts — file an issue or open a pull request on [this repo](https://github.com/dwillis/congress-press/issues). Examples:

- Records with garbled or missing text that should be extractable
- Metadata errors (wrong member, wrong date, etc.)
- Improvements to the text extraction or date backfill logic

## Dependencies

- [python-statement](https://github.com/dwillis/python-statement) - Congressional press release scrapers
- [newspaper4k](https://github.com/codelucas/newspaper) - Article text extraction
