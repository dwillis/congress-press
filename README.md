# congress-press

A collection of more than 670,000 congressional press releases from 2001 to the present, including full text, with daily updates powered by [python-statement](https://github.com/dwillis/python-statement).

## Data

The dataset contains press releases from over 860 members of Congress across both chambers. Data is stored as JSONL files organized by month in `data/YYYY/YYYY-MM.jsonl`.

Records come from two sources:

- **Legacy import** (2001-2020) - ~486,000 press releases with full text from the ProPublica Congress API
- **Scraper collection** (2020-present) - ~187,000 press releases collected via automated scrapers, with text extracted from individual pages

**IMPORTANT** this is not a comprehensive collection, mostly owing to departed members and periods when scraper coverage was incomplete. As of March 30, 2026, all current members with official websites are covered. See Caveats below for more details.

## How it works

A GitHub Actions workflow runs daily at 5am UTC in two phases:

1. **Metadata collection** - Scrapes press release listings (title, URL, date) from 536 congressional websites
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
```

## Dependencies

- [python-statement](https://github.com/dwillis/python-statement) - Congressional press release scrapers
- [newspaper4k](https://github.com/codelucas/newspaper) - Article text extraction
