#!/usr/bin/env bash
# Download BTS "Reporting Carrier On-Time Performance" monthly files into data/raw/.
# Source: https://www.transtats.bts.gov/ (US DOT, public domain, no login needed).
# Usage: scripts/download_data.sh 2024 1 3   -> Jan..Mar 2024
set -euo pipefail

YEAR="${1:-2024}"
FIRST_MONTH="${2:-1}"
LAST_MONTH="${3:-3}"
RAW_DIR="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
BASE_URL="https://transtats.bts.gov/PREZIP"

mkdir -p "$RAW_DIR"
for m in $(seq "$FIRST_MONTH" "$LAST_MONTH"); do
  name="On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${YEAR}_${m}"
  out_csv="$RAW_DIR/ontime_${YEAR}_$(printf '%02d' "$m").csv"
  if [[ -f "$out_csv" ]]; then
    echo "skip: $out_csv already exists"
    continue
  fi
  tmp="$(mktemp -d)"
  echo "downloading $name.zip"
  curl -fSL --retry 3 -o "$tmp/$name.zip" "$BASE_URL/$name.zip"
  # Each zip holds one CSV plus a readme.html; we only keep the CSV.
  unzip -q -o "$tmp/$name.zip" '*.csv' -d "$tmp"
  mv "$tmp"/*.csv "$out_csv"
  rm -rf "$tmp"
  echo "saved $out_csv"
done
