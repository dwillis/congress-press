"""Remove all press releases for a given member (by bioguide_id) from the dataset.

Usage:
    uv run python scripts/remove_member.py K000388
    uv run python scripts/remove_member.py K000388 --dry-run
"""

import argparse
import sys

from utils import DATA_DIR, load_jsonl, save_jsonl


def main():
    parser = argparse.ArgumentParser(description="Remove all releases for a member")
    parser.add_argument("member_id", help="Bioguide ID of the member to remove")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed without writing")
    args = parser.parse_args()

    member_id = args.member_id.upper()
    total_removed = 0
    files_modified = 0

    for jsonl_path in sorted(DATA_DIR.rglob("*.jsonl")):
        records = load_jsonl(jsonl_path)
        before = len(records)
        filtered = [r for r in records if (r.get("member") or {}).get("bioguide_id") != member_id]
        removed = before - len(filtered)

        if removed == 0:
            continue

        total_removed += removed
        files_modified += 1
        rel = jsonl_path.relative_to(DATA_DIR)
        print(f"{rel}: removed {removed} record(s) ({before} → {len(filtered)})")

        if not args.dry_run:
            save_jsonl(jsonl_path, filtered)

    action = "Would remove" if args.dry_run else "Removed"
    print(f"\n{action} {total_removed} record(s) across {files_modified} file(s) for {member_id}")

    if total_removed == 0:
        print(f"No records found for member {member_id}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
