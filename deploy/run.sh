#!/usr/bin/env bash
# Launcher for the TSX watcher on an always-on host (Oracle VM / VPS).
# Mon–Fri: run the full pre-market + market session in ONE process (no 6h cap,
# so no --until split needed). Saturday: run the weekly ticker discovery scan.
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck disable=SC1091
source .venv/bin/activate

DOW=$(TZ=America/New_York date +%u)   # 1=Mon … 6=Sat 7=Sun
if [ "$DOW" = "6" ]; then
  exec python watcher.py --discover
elif [ "$DOW" -le 5 ]; then
  exec python watcher.py
else
  echo "Sunday — nothing to run."
fi
