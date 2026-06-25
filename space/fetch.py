"""Download the latest search DB + Datasette config at container startup.

press.db comes from the HF dataset (the daily build artifact); metadata.yml comes
straight from the GitHub repo so config changes don't require a Space rebuild.
"""

import os
import urllib.request

from huggingface_hub import hf_hub_download

DATASET = os.environ.get("HF_DATASET", "dpwillis/congress-press-db")
METADATA_URL = os.environ.get(
    "METADATA_URL",
    "https://raw.githubusercontent.com/dwillis/congress-press/main/metadata.yml",
)

print(f"Downloading press.db from dataset {DATASET} ...")
path = hf_hub_download(
    repo_id=DATASET,
    filename="press.db",
    repo_type="dataset",
    local_dir="/app/data",
)
print(f"  -> {path}")

try:
    urllib.request.urlretrieve(METADATA_URL, "/app/metadata.yml")
    print("Fetched metadata.yml")
except Exception as exc:  # noqa: BLE001 - non-fatal, serve without config
    print(f"metadata.yml fetch failed ({exc}); serving without it")
