#!/usr/bin/env python3
"""
Strip date/time fields from BIDS JSON sidecar files.
Run this after heudiconv if it fails with:
  ValueError: There must be no dates in .json sidecar

Usage:
    python strip_dates.py /path/to/BIDS/sub-001/ses-001
    python strip_dates.py /path/to/BIDS  # entire dataset
"""
import sys, json, re
from pathlib import Path

root = Path(sys.argv[1])
_DATE_RE = re.compile(r'Date|Time')

stripped = 0
for f in root.rglob("*.json"):
    try:
        data = json.loads(f.read_text())
    except Exception:
        continue
    cleaned = {k: v for k, v in data.items() if not _DATE_RE.search(k)}
    if cleaned != data:
        f.write_text(json.dumps(cleaned, indent=2))
        print(f"  stripped: {f}")
        stripped += 1

print(f"\nDone — {stripped} file(s) updated.")
