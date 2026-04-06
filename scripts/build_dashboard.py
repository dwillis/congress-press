#!/usr/bin/env python3
"""Build the dashboard page: data completeness and activity stats.

Streams all JSONL files in a single pass, computes stats, and generates
a static docs/dashboard.html.

Usage:
    uv run python scripts/build_dashboard.py
"""

import json
from collections import defaultdict
from datetime import date
from pathlib import Path

from utils import DATA_DIR

DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"
CURRENT_YEAR = date.today().year
RECENT_YEARS = {CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2}


def scan_data():
    """Stream all JSONL files and accumulate stats."""
    # Per-year stats
    year_stats = defaultdict(lambda: {"total": 0, "has_text": 0, "null_date": 0})

    # Per-month stats (recent years only)
    month_stats = defaultdict(int)  # "2024-01" -> count

    # Per-member stats (recent years only)
    # bioguide_id -> {name, party, state, chamber, total, has_text}
    member_stats = {}

    # Party/chamber totals (recent years only)
    party_totals = defaultdict(int)
    chamber_totals = defaultdict(int)

    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        is_recent = year in RECENT_YEARS

        for jsonl_path in sorted(year_dir.glob("*.jsonl")):
            month_key = jsonl_path.stem  # e.g. "2024-01"
            with open(jsonl_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    ys = year_stats[year]
                    ys["total"] += 1
                    if rec.get("text"):
                        ys["has_text"] += 1
                    if not rec.get("date"):
                        ys["null_date"] += 1

                    if is_recent:
                        month_stats[month_key] += 1

                        member = rec.get("member") or {}
                        bio_id = member.get("bioguide_id")
                        party = member.get("party", "Unknown")
                        chamber = member.get("chamber", "Unknown")

                        party_totals[party] += 1
                        chamber_totals[chamber] += 1

                        if bio_id:
                            if bio_id not in member_stats:
                                member_stats[bio_id] = {
                                    "name": member.get("name", "Unknown"),
                                    "party": party,
                                    "state": member.get("state", ""),
                                    "chamber": chamber,
                                    "total": 0,
                                    "has_text": 0,
                                }
                            ms = member_stats[bio_id]
                            ms["total"] += 1
                            if rec.get("text"):
                                ms["has_text"] += 1

    return year_stats, month_stats, member_stats, party_totals, chamber_totals


def coverage_color(pct):
    if pct >= 80:
        return "#2d8a4e"
    elif pct >= 50:
        return "var(--gold)"
    else:
        return "#c0392b"


def party_abbrev(party):
    return {"Democrat": "D", "Republican": "R", "Independent": "I"}.get(party, party)


def generate_html(year_stats, month_stats, member_stats, party_totals, chamber_totals):
    total_records = sum(y["total"] for y in year_stats.values())
    total_null_dates = sum(y["null_date"] for y in year_stats.values())
    total_text = sum(y["has_text"] for y in year_stats.values())
    overall_pct = round(100 * total_text / total_records, 1) if total_records else 0

    # Year-by-year table rows
    year_rows = []
    for year in sorted(year_stats.keys(), reverse=True):
        ys = year_stats[year]
        pct = round(100 * ys["has_text"] / ys["total"], 1) if ys["total"] else 0
        color = coverage_color(pct)
        year_rows.append(
            f'<tr><td class="year-cell">{year}</td>'
            f'<td class="num">{ys["total"]:,}</td>'
            f'<td class="num">{ys["has_text"]:,}</td>'
            f'<td class="num">{ys["null_date"]:,}</td>'
            f'<td class="bar-cell">'
            f'<div class="bar-bg"><div class="bar-fill" style="width:{pct}%;background:{color}"></div></div>'
            f'<span class="bar-label" style="color:{color}">{pct}%</span></td></tr>'
        )

    # Monthly SVG chart (recent years)
    sorted_months = sorted(month_stats.keys())
    max_monthly = max(month_stats.values()) if month_stats else 1
    bar_width = 18
    bar_gap = 3
    svg_width = len(sorted_months) * (bar_width + bar_gap) + 60
    chart_height = 200
    svg_bars = []
    svg_labels = []
    for i, mk in enumerate(sorted_months):
        count = month_stats[mk]
        h = int(count / max_monthly * (chart_height - 30)) if max_monthly else 0
        x = 40 + i * (bar_width + bar_gap)
        y = chart_height - h - 20
        svg_bars.append(
            f'<rect x="{x}" y="{y}" width="{bar_width}" height="{h}" '
            f'fill="var(--navy)" rx="2"><title>{mk}: {count:,}</title></rect>'
        )
        # Label every 3rd month
        if i % 3 == 0:
            label = mk[2:]  # "24-01"
            svg_labels.append(
                f'<text x="{x + bar_width // 2}" y="{chart_height - 4}" '
                f'text-anchor="middle" font-size="9" fill="#888">{label}</text>'
            )

    # Y-axis labels
    y_axis = []
    for frac in [0, 0.25, 0.5, 0.75, 1.0]:
        val = int(max_monthly * frac)
        y = chart_height - 20 - int(frac * (chart_height - 30))
        y_axis.append(
            f'<text x="36" y="{y + 3}" text-anchor="end" font-size="9" fill="#aaa">{val:,}</text>'
            f'<line x1="38" y1="{y}" x2="{svg_width}" y2="{y}" stroke="#eee" stroke-width="1"/>'
        )

    svg_chart = (
        f'<svg width="100%" viewBox="0 0 {svg_width} {chart_height}" '
        f'style="max-width:{svg_width}px">'
        f'{"".join(y_axis)}{"".join(svg_bars)}{"".join(svg_labels)}</svg>'
    )

    # Party breakdown
    party_rows = []
    for party in ["Democrat", "Republican", "Independent"]:
        count = party_totals.get(party, 0)
        if count:
            party_rows.append(f"<tr><td>{party}</td><td class='num'>{count:,}</td></tr>")

    # Chamber breakdown
    chamber_rows = []
    for chamber in ["House", "Senate"]:
        count = chamber_totals.get(chamber, 0)
        if count:
            chamber_rows.append(f"<tr><td>{chamber}</td><td class='num'>{count:,}</td></tr>")

    # Lowest text coverage members (min 10 records)
    eligible = [
        (bio, m) for bio, m in member_stats.items() if m["total"] >= 10
    ]
    by_coverage = sorted(eligible, key=lambda x: x[1]["has_text"] / x[1]["total"])
    low_coverage_rows = []
    for bio, m in by_coverage[:20]:
        pct = round(100 * m["has_text"] / m["total"], 1)
        color = coverage_color(pct)
        low_coverage_rows.append(
            f'<tr><td>{m["name"]}</td>'
            f'<td>{party_abbrev(m["party"])}-{m["state"]}</td>'
            f'<td>{m["chamber"]}</td>'
            f'<td class="num">{m["total"]:,}</td>'
            f'<td class="num" style="color:{color}">{pct}%</td></tr>'
        )

    # Most active members
    by_activity = sorted(member_stats.items(), key=lambda x: -x[1]["total"])
    active_rows = []
    for bio, m in by_activity[:25]:
        pct = round(100 * m["has_text"] / m["total"], 1) if m["total"] else 0
        active_rows.append(
            f'<tr><td>{m["name"]}</td>'
            f'<td>{party_abbrev(m["party"])}-{m["state"]}</td>'
            f'<td>{m["chamber"]}</td>'
            f'<td class="num">{m["total"]:,}</td>'
            f'<td class="num">{pct}%</td></tr>'
        )

    recent_label = f"{min(RECENT_YEARS)}\u2013{max(RECENT_YEARS)}"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dashboard - Congress Press</title>
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
      padding: 2rem 1rem 1.5rem;
      text-align: center;
    }}

    header h1 {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 2rem;
      font-weight: 700;
      letter-spacing: 0.02em;
      margin-bottom: 0.3rem;
    }}

    header h1 span {{ color: var(--gold); }}

    header p {{
      font-size: 0.95rem;
      opacity: 0.8;
    }}

    .header-links {{
      display: flex;
      gap: 1.5rem;
      justify-content: center;
      margin-top: 0.8rem;
    }}

    .header-links a {{
      color: var(--gold-light);
      text-decoration: none;
      font-size: 0.9rem;
      border-bottom: 1px solid transparent;
    }}

    .header-links a:hover {{ border-bottom-color: var(--gold-light); }}

    .stats {{
      display: flex;
      justify-content: center;
      gap: 2.5rem;
      padding: 1rem;
      background: white;
      border-bottom: 2px solid var(--gold);
      flex-wrap: wrap;
    }}

    .stat {{ text-align: center; }}

    .stat-value {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 1.4rem;
      font-weight: 700;
      color: var(--navy);
    }}

    .stat-label {{
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #777;
    }}

    main {{
      max-width: 900px;
      margin: 2rem auto;
      padding: 0 1rem;
    }}

    h2 {{
      font-family: "Libre Baskerville", "Georgia", serif;
      font-size: 1.3rem;
      color: var(--navy);
      margin: 2rem 0 0.8rem;
      padding-bottom: 0.4rem;
      border-bottom: 2px solid var(--gold);
    }}

    h2:first-child {{ margin-top: 0; }}

    h3 {{
      font-size: 0.95rem;
      color: var(--navy-light);
      margin: 1.5rem 0 0.5rem;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
      background: white;
      border: 1px solid var(--border);
      border-radius: 4px;
      margin-bottom: 1rem;
    }}

    thead th {{
      text-align: left;
      font-size: 0.7rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: #888;
      padding: 0.5rem 0.6rem;
      border-bottom: 1px solid var(--border);
      background: #f9f9f9;
    }}

    td {{
      padding: 0.4rem 0.6rem;
      border-bottom: 1px solid #eee;
    }}

    .num {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}

    .year-cell {{
      font-weight: 700;
      color: var(--navy);
    }}

    .bar-cell {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}

    .bar-bg {{
      flex: 1;
      height: 14px;
      background: #eee;
      border-radius: 3px;
      overflow: hidden;
    }}

    .bar-fill {{
      height: 100%;
      border-radius: 3px;
      transition: width 0.3s;
    }}

    .bar-label {{
      font-size: 0.8rem;
      font-weight: 600;
      min-width: 3.5em;
      text-align: right;
    }}

    .summary-line {{
      font-size: 0.9rem;
      color: #666;
      margin: 0.5rem 0 1rem;
    }}

    .side-by-side {{
      display: flex;
      gap: 1.5rem;
      flex-wrap: wrap;
    }}

    .side-by-side > div {{
      flex: 1;
      min-width: 200px;
    }}

    .chart-container {{
      background: white;
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 1rem;
      overflow-x: auto;
      margin-bottom: 1rem;
    }}

    footer {{
      text-align: center;
      padding: 2rem 1rem;
      font-size: 0.8rem;
      color: #999;
      border-top: 1px solid var(--border);
      margin-top: 2rem;
    }}

    footer a {{ color: var(--navy-light); }}

    @media (max-width: 600px) {{
      header h1 {{ font-size: 1.6rem; }}
      .stats {{ gap: 1.5rem; }}
      .side-by-side {{ flex-direction: column; }}
    }}
  </style>
  <link href="https://fonts.googleapis.com/css2?family=Libre+Baskerville:wght@400;700&display=swap" rel="stylesheet">
</head>
<body>
  <header>
    <h1>Congress <span>Press</span> Dashboard</h1>
    <p>Data completeness and activity overview</p>
    <div class="header-links">
      <a href="index.html">Downloads</a>
      <a href="https://github.com/dwillis/congress-press">GitHub</a>
    </div>
  </header>

  <div class="stats">
    <div class="stat">
      <div class="stat-value">{total_records:,}</div>
      <div class="stat-label">Total Records</div>
    </div>
    <div class="stat">
      <div class="stat-value">{overall_pct}%</div>
      <div class="stat-label">Text Coverage</div>
    </div>
    <div class="stat">
      <div class="stat-value">{total_null_dates:,}</div>
      <div class="stat-label">Missing Dates</div>
    </div>
  </div>

  <main>
    <h2>Data Completeness</h2>

    <h3>Text Coverage by Year</h3>
    <table>
      <thead><tr>
        <th>Year</th><th>Records</th><th>With Text</th><th>No Date</th><th>Text Coverage</th>
      </tr></thead>
      <tbody>
{chr(10).join(year_rows)}
      </tbody>
    </table>

    <h3>Lowest Text Coverage ({recent_label}, min 10 releases)</h3>
    <table>
      <thead><tr>
        <th>Member</th><th>Party</th><th>Chamber</th><th>Releases</th><th>Text %</th>
      </tr></thead>
      <tbody>
{chr(10).join(low_coverage_rows)}
      </tbody>
    </table>

    <h2>Activity &amp; Volume</h2>

    <h3>Monthly Releases ({recent_label})</h3>
    <div class="chart-container">
      {svg_chart}
    </div>

    <div class="side-by-side">
      <div>
        <h3>By Party ({recent_label})</h3>
        <table>
          <thead><tr><th>Party</th><th>Releases</th></tr></thead>
          <tbody>{chr(10).join(party_rows)}</tbody>
        </table>
      </div>
      <div>
        <h3>By Chamber ({recent_label})</h3>
        <table>
          <thead><tr><th>Chamber</th><th>Releases</th></tr></thead>
          <tbody>{chr(10).join(chamber_rows)}</tbody>
        </table>
      </div>
    </div>

    <h3>Most Active Members ({recent_label})</h3>
    <table>
      <thead><tr>
        <th>Member</th><th>Party</th><th>Chamber</th><th>Releases</th><th>Text %</th>
      </tr></thead>
      <tbody>
{chr(10).join(active_rows)}
      </tbody>
    </table>
  </main>

  <footer>
    Generated {date.today().isoformat()} &middot;
    <a href="https://github.com/dwillis/congress-press">Congress Press</a>
  </footer>
</body>
</html>"""
    return html


def main():
    print("Scanning data...")
    year_stats, month_stats, member_stats, party_totals, chamber_totals = scan_data()

    total = sum(y["total"] for y in year_stats.values())
    print(f"  {total:,} records across {len(year_stats)} years")
    print(f"  {len(member_stats):,} members tracked (recent years)")

    print("Generating dashboard...")
    html = generate_html(year_stats, month_stats, member_stats, party_totals, chamber_totals)
    out_path = DOCS_DIR / "dashboard.html"
    out_path.write_text(html)
    print(f"  Wrote {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
