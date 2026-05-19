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

_SH_RE      = re.compile(r'(\S+\.sh)\b')
_SUBJECT_RE = re.compile(r'--subject-id\s+(\S+)')
_SESSION_RE = re.compile(r'--session-id\s+(\S+)')


def _log_name(cmd: str, index: int) -> str:
    # 1. Python commands with --subject-id / --session-id  (dcm2dir style)
    subj_m = _SUBJECT_RE.search(cmd)
    ses_m  = _SESSION_RE.search(cmd)
    if subj_m:
        subj = subj_m.group(1)          # e.g. sub-MGHL2p003
        ses  = ses_m.group(1) if ses_m else None
        # derive the script name from the last token of the python path
        script_tok = Path(cmd.split()[1]).name if len(cmd.split()) > 1 else "cmd"
        parts = [subj]
        if ses:
            parts.append(ses)
        parts.append(script_tok)        # e.g. dcm2dir
        return "_".join(parts)          # sub-MGHL2p003_ses-002_dcm2dir

    # 2. Shell scripts  (original behaviour)
    m = _SH_RE.search(cmd)
    if m:
        return Path(m.group(1)).stem    # e.g. sub-MGHL2p003_ses-002_BIDS

    # 3. Fallback
    return f"cmd_{index:04d}"


def run_command(cmd, index, total):
    cmd = cmd.strip()
    if not cmd or cmd.startswith("#"):
        return index, 0, cmd

    print(f"[{index}/{total}] Starting: {cmd}")
    start = datetime.now()

    if LOG_DIR:
        log_path = LOG_DIR / f"{_log_name(cmd, index)}.log"
        with open(log_path, "w") as lf:
            result = subprocess.run(cmd, shell=True, stdout=lf, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            fail_path = log_path.with_suffix(".FAIL")
            log_path.rename(fail_path)
            log_path = fail_path
    else:
        result = subprocess.run(cmd, shell=True)

    elapsed = (datetime.now() - start).total_seconds()
    status = "✓" if result.returncode == 0 else "✗ FAILED"
    print(f"[{status}] ({elapsed:.1f}s) [{index}/{total}] {cmd}")
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