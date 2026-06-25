#!/usr/bin/env python3
"""Publish press.db to the Hugging Face dataset and restart the search Space.

Run after scripts/build_search_db.py in CI. Requires an HF_TOKEN env var with
write access to the dataset + Space.

Env:
    HF_TOKEN     (required) Hugging Face write token
    HF_DATASET   dataset repo id   (default dpwillis/congress-press-db)
    HF_SPACE     Space repo id      (default dpwillis/congress-press-search)

Usage:
    python scripts/publish_search_db.py [--db press.db] [--no-restart]
"""

import argparse
import os
import sys
from pathlib import Path

from huggingface_hub import HfApi


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("press.db"))
    parser.add_argument("--no-restart", action="store_true", help="skip Space restart")
    args = parser.parse_args()

    token = os.environ.get("HF_TOKEN")
    if not token:
        sys.exit("HF_TOKEN is not set; cannot publish.")
    if not args.db.exists():
        sys.exit(f"{args.db} not found; run build_search_db.py first.")

    dataset = os.environ.get("HF_DATASET", "dpwillis/congress-press-db")
    space = os.environ.get("HF_SPACE", "dpwillis/congress-press-search")
    api = HfApi(token=token)

    api.create_repo(dataset, repo_type="dataset", exist_ok=True)
    size_mb = args.db.stat().st_size / 1_048_576
    print(f"Uploading {args.db} ({size_mb:,.1f} MB) to dataset {dataset} ...")
    api.upload_file(
        path_or_fileobj=str(args.db),
        path_in_repo="press.db",
        repo_id=dataset,
        repo_type="dataset",
        commit_message="Update search index",
    )
    print("  uploaded.")

    if not args.no_restart:
        try:
            api.restart_space(repo_id=space)
            print(f"Restarted Space {space} to pick up the new index.")
        except Exception as exc:  # noqa: BLE001 - non-fatal
            print(f"Space restart failed ({exc}); it will pull the new DB on next boot.")


if __name__ == "__main__":
    main()
