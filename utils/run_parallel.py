#!/usr/bin/env python3
import subprocess
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

parser = argparse.ArgumentParser(description='Run shell commands in parallel.')
parser.add_argument('--script-file', required=True, help='File containing commands to run (one per line)')
parser.add_argument('--max-workers', type=int, default=16, help='Maximum parallel workers (default: 16)')
args = parser.parse_args()

SCRIPT_FILE = args.script_file
MAX_WORKERS = args.max_workers

def run_command(cmd, index, total):
    cmd = cmd.strip()
    if not cmd or cmd.startswith("#"):
        return index, 0, cmd

    print(f"[{index}/{total}] Starting: {cmd[:80]}...")
    start = datetime.now()
    result = subprocess.run(cmd, shell=True)  # no capture — tee handles output to per-subject logs
    elapsed = (datetime.now() - start).total_seconds()

    status = "✓" if result.returncode == 0 else "✗ FAILED"
    print(f"[{status}] ({elapsed:.1f}s) [{index}/{total}] {cmd[:80]}")
    return index, result.returncode, cmd

def main():
    with open(SCRIPT_FILE) as f:
        commands = [l.strip() for l in f if l.strip() and not l.startswith("#")]

    total = len(commands)
    print(f"Found {total} commands — running {MAX_WORKERS} at a time\n")

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