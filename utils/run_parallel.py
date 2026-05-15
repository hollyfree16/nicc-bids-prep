#!/usr/bin/env python3
import re
import subprocess
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

parser = argparse.ArgumentParser(description='Run shell commands in parallel.')
parser.add_argument('--script-file', required=True, help='File containing commands to run (one per line)')
parser.add_argument('--max-workers', type=int, default=16, help='Maximum parallel workers (default: 16)')
parser.add_argument('--log-dir', default=None, help='Directory to write per-command log files')
args = parser.parse_args()

SCRIPT_FILE = args.script_file
MAX_WORKERS = args.max_workers
LOG_DIR = Path(args.log_dir) if args.log_dir else None

if LOG_DIR:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

_SH_RE = re.compile(r'(\S+\.sh)\b')

def _log_name(cmd: str, index: int) -> str:
    m = _SH_RE.search(cmd)
    if m:
        return Path(m.group(1)).stem
    return f"cmd_{index:04d}"

def run_command(cmd, index, total):
    cmd = cmd.strip()
    if not cmd or cmd.startswith("#"):
        return index, 0, cmd

    print(f"[{index}/{total}] Starting: {cmd[:80]}...")
    start = datetime.now()

    if LOG_DIR:
        log_path = LOG_DIR / f"{_log_name(cmd, index)}.log"
        with open(log_path, "w") as lf:
            result = subprocess.run(cmd, shell=True, stdout=lf, stderr=subprocess.STDOUT)
    else:
        result = subprocess.run(cmd, shell=True)

    elapsed = (datetime.now() - start).total_seconds()
    status = "✓" if result.returncode == 0 else "✗ FAILED"
    print(f"[{status}] ({elapsed:.1f}s) [{index}/{total}] {cmd[:80]}")
    return index, result.returncode, cmd

def main():
    with open(SCRIPT_FILE) as f:
        commands = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    total = len(commands)
    print(f"Found {total} commands — running {MAX_WORKERS} at a time")
    if LOG_DIR:
        print(f"Logs → {LOG_DIR}\n")

    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(run_command, cmd, i+1, total): i for i, cmd in enumerate(commands)}

        for future in as_completed(futures):
            index, returncode, cmd = future.result()
            if returncode != 0:
                failed.append((index, cmd))

    print(f"\n{'='*60}")
    print(f"Done. {total - len(failed)}/{total} succeeded.")
    if failed:
        print(f"\nFailed commands:")
        for index, cmd in failed:
            print(f"  [{index}] {cmd}")

if __name__ == "__main__":
    main()
