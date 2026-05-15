#!/usr/bin/env python3
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

parser = argparse.ArgumentParser(description='Run a single subject BIDS.sh script.')
parser.add_argument('script', help='Path to the _BIDS.sh script to run')
parser.add_argument('--log-dir', default=None, help='Directory to write log file')
args = parser.parse_args()

script = Path(args.script)
LOG_DIR = Path(args.log_dir) if args.log_dir else None

if LOG_DIR:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

print(f"Running: {script.name}")
start = datetime.now()

if LOG_DIR:
    log_path = LOG_DIR / f"{script.stem}.log"
    with open(log_path, "w") as lf:
        result = subprocess.run(["bash", str(script)], stdout=lf, stderr=subprocess.STDOUT)
    if result.returncode != 0:
        log_path = log_path.rename(log_path.with_suffix(".FAIL"))
    print(f"Log → {log_path}")
else:
    result = subprocess.run(["bash", str(script)])

elapsed = (datetime.now() - start).total_seconds()
status = "✓ Done" if result.returncode == 0 else "✗ FAILED"
print(f"[{status}] ({elapsed:.1f}s)")
