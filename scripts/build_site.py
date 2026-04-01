#!/usr/bin/env python3
"""Build the GitHub Pages site: zip yearly data and generate index.html.

Usage:
    uv run python scripts/build_site.py
"""

import os
import shutil
import zipfile
from datetime import date
from pathlib import Path

from utils import DATA_DIR

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
DOWNLOADS_DIR = DOCS_DIR / "downloads"

CURRENT_YEAR = str(date.today().year)

MONTH_NAMES = {
    "01": "January", "02": "February", "03": "March", "04": "April",
    "05": "May", "06": "June", "07": "July", "08": "August",
    "09": "September", "10": "October", "11": "November", "12": "December",
}


def human_size(nbytes):
    """Format byte count as human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if nbytes < 1024:
            return f"{nbytes:.1f} {unit}" if unit != "B" else f"{nbytes} B"
        nbytes /= 1024
    return f"{nbytes:.1f} TB"


def count_records(path):
    """Count lines in a JSONL file."""
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def build_downloads():
    """Create zip files for past years, copy current year files. Returns metadata."""
    if DOWNLOADS_DIR.exists():
        shutil.rmtree(DOWNLOADS_DIR)
    DOWNLOADS_DIR.mkdir(parents=True)

    years = {}

    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue

        year = year_dir.name
        jsonl_files = sorted(year_dir.glob("*.jsonl"))
        if not jsonl_files:
            continue

        total_records = 0
        months = []

        for f in jsonl_files:
            month_num = f.stem.split("-")[1]  # "2026-03" -> "03"
            rec_count = count_records(f)
            total_records += rec_count
            months.append({
                "month_num": month_num,
                "month_name": MONTH_NAMES.get(month_num, month_num),
                "filename": f.name,
                "records": rec_count,
            })

        if year == CURRENT_YEAR:
            # Copy individual monthly files for current year
            for f in jsonl_files:
                shutil.copy2(f, DOWNLOADS_DIR / f.name)

            years[year] = {
                "type": "current",
                "months": months,
                "total_records": total_records,
            }

            # Also add file sizes after copy
            for m in months:
                fpath = DOWNLOADS_DIR / m["filename"]
                m["size"] = human_size(fpath.stat().st_size)
        else:
            # Create zip for past years
            zip_name = f"{year}.zip"
            zip_path = DOWNLOADS_DIR / zip_name

            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in jsonl_files:
                    zf.write(f, arcname=f"{year}/{f.name}")

            years[year] = {
                "type": "archive",
                "zip_name": zip_name,
                "zip_size": human_size(zip_path.stat().st_size),
                "months": months,
                "total_records": total_records,
            }

    return years


def generate_html(years):
    """Generate the index.html page."""
    total_records = sum(y["total_records"] for y in years.values())
    total_members = 860  # approximate, from README
    year_range = f"{min(years.keys())}-{max(years.keys())}"

    year_sections = []
    for year in sorted(years.keys(), reverse=True):
        info = years[year]
        is_current = info["type"] == "current"
        open_attr = " open" if is_current else ""

        if is_current:
            month_rows = []
            for m in info["months"]:
                month_rows.append(
                    f'          <tr>'
                    f'<td>{m["month_name"]}</td>'
                    f'<td class="num">{m["records"]:,}</td>'
                    f'<td class="num">{m["size"]}</td>'
                    f'<td><a href="downloads/{m["filename"]}" class="dl-link">Download JSONL</a></td>'
                    f'</tr>'
                )
            content = f"""        <table>
          <thead><tr><th>Month</th><th>Records</th><th>Size</th><th></th></tr></thead>
          <tbody>
{chr(10).join(month_rows)}
          </tbody>
        </table>"""
        else:
            month_list = ", ".join(m["month_name"] for m in info["months"])
            content = f"""        <div class="archive-row">
          <a href="downloads/{info['zip_name']}" class="dl-link dl-zip">Download {year}.zip</a>
          <span class="meta">{info['zip_size']} &middot; {info['total_records']:,} records</span>
        </div>
        <p class="months-covered">{month_list}</p>"""

        year_sections.append(f"""      <details{open_attr}>
        <summary>
          <span class="year">{year}</span>
          <span class="year-meta">{info['total_records']:,} records &middot; {len(info['months'])} months</span>
        </summary>
{content}
      </details>""")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Congress Press - Congressional Press Release Archive</title>
  <style>
    :root {{
      --navy: #1a2744;
      --navy-light: #2a3d5e;
      --gold: #c5a44e;
      --gold-light: #d4ba73;
      --bg: #fafafa;
      --text: #2c2c2c;
      --border: #ddd;
    }}

    * {{ margin: 0; padding: 0; box-sizing: border-box; }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: var(--text);
      background: var(--bg);
      line-height: 1.6;
    }}

    header {{
      background: var(--navy);
      color: white;
      padding: 2.5rem 1rem 2rem;
      text-align: center;
    }}

    header h1 {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 2.4rem;
      font-weight: 700;
      letter-spacing: 0.02em;
      margin-bottom: 0.5rem;
    }}

    header h1 span {{
      color: var(--gold);
    }}

    header p {{
      font-size: 1.05rem;
      opacity: 0.85;
      max-width: 600px;
      margin: 0 auto 1rem;
    }}

    .header-links {{
      display: flex;
      gap: 1.5rem;
      justify-content: center;
      flex-wrap: wrap;
    }}

    .header-links a {{
      color: var(--gold-light);
      text-decoration: none;
      font-size: 0.9rem;
      border-bottom: 1px solid transparent;
    }}

    .header-links a:hover {{
      border-bottom-color: var(--gold-light);
    }}

    .stats {{
      display: flex;
      justify-content: center;
      gap: 2.5rem;
      padding: 1.2rem 1rem;
      background: white;
      border-bottom: 2px solid var(--gold);
      flex-wrap: wrap;
    }}

    .stat {{
      text-align: center;
    }}

    .stat-value {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 1.5rem;
      font-weight: 700;
      color: var(--navy);
    }}

    .stat-label {{
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #777;
    }}

    main {{
      max-width: 760px;
      margin: 2rem auto;
      padding: 0 1rem;
    }}

    h2 {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 1.4rem;
      color: var(--navy);
      margin-bottom: 1rem;
      padding-bottom: 0.5rem;
      border-bottom: 2px solid var(--gold);
    }}

    details {{
      border: 1px solid var(--border);
      border-radius: 4px;
      margin-bottom: 0.5rem;
      background: white;
    }}

    summary {{
      padding: 0.8rem 1rem;
      cursor: pointer;
      display: flex;
      justify-content: space-between;
      align-items: center;
      user-select: none;
    }}

    summary:hover {{
      background: #f5f5f5;
    }}

    summary::-webkit-details-marker {{
      display: none;
    }}

    summary::before {{
      content: "\\25B6";
      font-size: 0.7rem;
      margin-right: 0.7rem;
      color: var(--gold);
      transition: transform 0.2s;
    }}

    details[open] > summary::before {{
      transform: rotate(90deg);
    }}

    .year {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 1.15rem;
      font-weight: 700;
      color: var(--navy);
    }}

    .year-meta {{
      font-size: 0.85rem;
      color: #888;
    }}

    details > :not(summary) {{
      padding: 0 1rem 1rem;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.9rem;
    }}

    thead th {{
      text-align: left;
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #888;
      padding: 0.5rem 0.5rem;
      border-bottom: 1px solid var(--border);
    }}

    td {{
      padding: 0.45rem 0.5rem;
      border-bottom: 1px solid #eee;
    }}

    .num {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}

    .dl-link {{
      color: var(--navy);
      text-decoration: none;
      font-weight: 500;
      font-size: 0.85rem;
      padding: 0.25rem 0.6rem;
      border: 1px solid var(--navy);
      border-radius: 3px;
      transition: all 0.15s;
    }}

    .dl-link:hover {{
      background: var(--navy);
      color: white;
    }}

    .dl-zip {{
      font-size: 0.95rem;
      padding: 0.4rem 1rem;
    }}

    .archive-row {{
      display: flex;
      align-items: center;
      gap: 1rem;
      margin-bottom: 0.5rem;
    }}

    .meta {{
      font-size: 0.85rem;
      color: #888;
    }}

    .months-covered {{
      font-size: 0.8rem;
      color: #aaa;
    }}

    footer {{
      text-align: center;
      padding: 2rem 1rem;
      font-size: 0.8rem;
      color: #999;
      border-top: 1px solid var(--border);
      margin-top: 2rem;
    }}

    footer a {{
      color: var(--navy-light);
    }}

    @media (max-width: 600px) {{
      header h1 {{ font-size: 1.8rem; }}
      .stats {{ gap: 1.5rem; }}
      .stat-value {{ font-size: 1.2rem; }}
      .archive-row {{ flex-direction: column; align-items: flex-start; gap: 0.3rem; }}
    }}
  </style>
  <link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:wght@400;700&display=swap" rel="stylesheet">
</head>
<body>
  <header>
    <h1>Congress <span>Press</span></h1>
    <p>An archive of congressional press releases from {year_range}, with daily updates.</p>
    <div class="header-links">
      <a href="https://github.com/dwillis/congress-press">GitHub Repository</a>
      <a href="https://github.com/dwillis/python-statement">python-statement Scrapers</a>
    </div>
  </header>

  <div class="stats">
    <div class="stat">
      <div class="stat-value">{total_records:,}</div>
      <div class="stat-label">Press Releases</div>
    </div>
    <div class="stat">
      <div class="stat-value">{total_members}+</div>
      <div class="stat-label">Members of Congress</div>
    </div>
    <div class="stat">
      <div class="stat-value">{len(years)}</div>
      <div class="stat-label">Years of Data</div>
    </div>
  </div>

  <main>
    <h2>Downloads</h2>

{chr(10).join(year_sections)}

  </main>

  <footer>
    Created by <a href="mailto:dpwillis@umd.edu">Derek Willis</a>.
    Released under the <a href="https://github.com/dwillis/congress-press/blob/main/LICENSE">MIT License</a>.
    <a href="https://github.com/dwillis/congress-press#contributing">How to contribute</a>.
  </footer>
</body>
</html>"""

    return html


def main():
    print("Building site...")
    years = build_downloads()
    print(f"  Processed {len(years)} years")

    html = generate_html(years)
    index_path = DOCS_DIR / "index.html"
    index_path.write_text(html)
    print(f"  Wrote {index_path}")

    total_downloads = len(list(DOWNLOADS_DIR.iterdir()))
    print(f"  {total_downloads} download files in {DOWNLOADS_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()
