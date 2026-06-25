#!/usr/bin/env bash
set -euo pipefail

python /app/fetch.py

ARGS=()
[ -f /app/metadata.yml ] && ARGS+=(-m /app/metadata.yml)

exec datasette serve /app/data/press.db "${ARGS[@]}" \
  -h 0.0.0.0 -p 7860 \
  --setting sql_time_limit_ms 8000 \
  --setting max_returned_rows 2000 \
  --setting facet_time_limit_ms 2000 \
  --setting suggest_facets off
