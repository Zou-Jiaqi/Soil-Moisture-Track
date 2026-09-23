#!/usr/bin/env bash
# End-to-end local smoke test: seeds synthetic data, then runs
# integrate.py for each historical day and retrieval.py for today --
# all as plain `python3` processes against a local_bucket/ directory,
# no Docker, no GCP.
#
# Usage: bash scripts/run_local.sh [PROCESS_DATE]
#   PROCESS_DATE defaults to today (YYYY-MM-DD).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/local.env"

export PROCESS_DATE="${1:-$(date +%Y-%m-%d)}"

echo "== Seeding local_bucket ($GCS_BUCKET_PATH) for PROCESS_DATE=$PROCESS_DATE =="
python3 "$REPO_ROOT/scripts/seed_local_bucket.py"

echo
echo "== Running integrate.py for each seeded historical day =="
for offset in $(seq "$SEED_HISTORY_DAYS" -1 1); do
  day=$(date -d "$PROCESS_DATE -$offset days" +%Y-%m-%d)
  echo "-- integrate $day --"
  PROCESS_DATE="$day" PYTHONPATH="$REPO_ROOT/integration/src" \
    python3 "$REPO_ROOT/integration/src/integrate_entrypoint.py"
done

echo
echo "== Running retrieval.py for $PROCESS_DATE =="
PYTHONPATH="$REPO_ROOT/retrieval/src" \
  python3 "$REPO_ROOT/retrieval/src/retrieval_entrypoint.py"

echo
echo "== Done. Predictions written under: =="
echo "$GCS_BUCKET_PATH$SOIL_MOISTURE_PARQUET_PATH/SOIL_MOISTURE.parquet/date=$PROCESS_DATE"
