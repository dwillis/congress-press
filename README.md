# congress-press

Daily collection of congressional press releases with full text, powered by [python-statement](https://github.com/dwillis/python-statement).

## How it works

A GitHub Actions workflow runs daily at 5am UTC in two phases:

1. **Metadata collection** - Scrapes press release listings (title, URL, date) from 536 congressional websites
2. **Text extraction** - Fetches full article text using newspaper4k for new/changed releases

Data is stored as JSONL files organized by month in `data/YYYY/YYYY-MM.jsonl`.

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

## Running locally

```bash
uv sync
uv run python scripts/collect_metadata.py
uv run python scripts/collect_text.py
uv run python scripts/collect_text.py --retry-failures  # re-attempt null-text records
```

## Dependencies

- [python-statement](https://github.com/dwillis/python-statement) - Congressional press release scrapers
- [newspaper4k](https://github.com/codelucas/newspaper) - Article text extraction
