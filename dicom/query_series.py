
"""
query_series.py
===============

Generate per-subject/per-session TSV inventories from DICOM series folders.

Designed for DICOM directories staged by dicom/dcm2dir, where the standard layout is:

    RAW_ROOT/
        SubjectID/
            SessionID/
                <SeriesDescription>_<SeriesNumber>/
                    *.dcm

or, for no session:

    RAW_ROOT/
        SubjectID/
            <SeriesDescription>_<SeriesNumber>/
                *.dcm

The script can run on:
  1. A single subject/session directory.
  2. A raw-data root containing many subjects/sessions.

Outputs one TSV per subject/session. Each row is one DICOM series, ordered by
SeriesNumber/AcquisitionTime so duplicates, partial scans, and reruns are easier to review.

Usage
-----
Single subject/session:
    python query_series.py --input-dir /raw/sub-001/ses-001 --output-dir /logs

Entire raw-data root:
    python query_series.py --input-dir /raw --output-dir /logs --batch

Overwrite existing TSVs:
    python query_series.py --input-dir /raw --output-dir /logs --batch --force
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pydicom


DICOM_EXTENSIONS = {".dcm", ".ima", ""}


def is_probable_dicom_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name.startswith("."):
        return False
    return path.suffix.lower() in DICOM_EXTENSIONS


def read_dicom_header(path: Path):
    return pydicom.dcmread(str(path), stop_before_pixels=True, force=True)


def get_value(ds, name: str) -> str:
    value = getattr(ds, name, "")
    if value is None:
        return ""
    try:
        from pydicom.multival import MultiValue
        if isinstance(value, MultiValue):
            return "\\".join(str(v) for v in value)
    except Exception:
        pass
    if isinstance(value, (list, tuple)):
        return "\\".join(str(v) for v in value)
    return str(value)


def format_dicom_date(value: str) -> str:
    value = str(value or "").strip()
    if not value:
        return ""
    try:
        return datetime.strptime(value[:8], "%Y%m%d").strftime("%Y-%m-%d")
    except Exception:
        return value


def safe_int(value, default: int = 999999) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(str(value)))
    except Exception:
        return default


def sort_time(value: str) -> str:
    value = str(value or "").strip()
    return value if value else "999999"


def find_dicom_files(root: Path) -> Iterable[Path]:
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            path = Path(dirpath) / name
            if is_probable_dicom_file(path):
                yield path


def group_by_series(input_dir: Path) -> Dict[str, Dict]:
    series = defaultdict(lambda: {
        "files": [],
        "example_dicom": "",
        "header": None,
    })

    for path in find_dicom_files(input_dir):
        try:
            ds = read_dicom_header(path)
        except Exception:
            continue

        uid = get_value(ds, "SeriesInstanceUID")
        if not uid:
            uid = f"NO_UID::{path.parent}"

        row = series[uid]
        row["files"].append(path)
        if not row["example_dicom"]:
            row["example_dicom"] = str(path)
        if row["header"] is None:
            row["header"] = ds

    return dict(series)


def summarize_series(uid: str, meta: Dict, subject: str, session: str) -> Dict[str, str]:
    ds = meta["header"]
    files = sorted(meta["files"])

    study_date = format_dicom_date(get_value(ds, "StudyDate"))
    series_date = format_dicom_date(get_value(ds, "SeriesDate"))
    acq_date = format_dicom_date(get_value(ds, "AcquisitionDate"))
    scan_date = study_date or series_date or acq_date

    instance_numbers = []
    for f in files:
        try:
            h = read_dicom_header(f)
            inst = safe_int(get_value(h, "InstanceNumber"), default=-1)
            if inst >= 0:
                instance_numbers.append(inst)
        except Exception:
            continue

    min_instance = min(instance_numbers) if instance_numbers else ""
    max_instance = max(instance_numbers) if instance_numbers else ""
    n_unique_instances = len(set(instance_numbers)) if instance_numbers else ""

    n_files = len(files)
    partial_flag = ""
    if instance_numbers:
        expected_span = max(instance_numbers) - min(instance_numbers) + 1
        if expected_span > n_unique_instances:
            partial_flag = "missing_instance_numbers"
        elif n_files < 5:
            partial_flag = "very_few_files"
    elif n_files < 5:
        partial_flag = "very_few_files"

    return {
        "subject": subject,
        "session": session,
        "scan_date": scan_date,
        "study_date": study_date,
        "series_date": series_date,
        "acquisition_date": acq_date,
        "series_time": get_value(ds, "SeriesTime"),
        "acquisition_time": get_value(ds, "AcquisitionTime"),
        "series_number": get_value(ds, "SeriesNumber"),
        "series_instance_uid": uid,
        "series_description": get_value(ds, "SeriesDescription"),
        "protocol_name": get_value(ds, "ProtocolName"),
        "sequence_name": get_value(ds, "SequenceName"),
        "image_type": get_value(ds, "ImageType"),
        "echo_time": get_value(ds, "EchoTime"),
        "echo_number": get_value(ds, "EchoNumber"),
        "repetition_time": get_value(ds, "RepetitionTime"),
        "phase_encoding_direction": get_value(ds, "PhaseEncodingDirection"),
        "inplane_phase_encoding_direction": get_value(ds, "InPlanePhaseEncodingDirection"),
        "n_files": str(n_files),
        "min_instance_number": str(min_instance),
        "max_instance_number": str(max_instance),
        "n_unique_instance_numbers": str(n_unique_instances),
        "partial_flag": partial_flag,
        "series_dir": str(Path(files[0]).parent) if files else "",
        "example_dicom": meta["example_dicom"],
    }


FIELDNAMES = [
    "subject",
    "session",
    "scan_date",
    "study_date",
    "series_date",
    "acquisition_date",
    "series_time",
    "acquisition_time",
    "series_number",
    "series_instance_uid",
    "series_description",
    "protocol_name",
    "sequence_name",
    "image_type",
    "echo_time",
    "echo_number",
    "repetition_time",
    "phase_encoding_direction",
    "inplane_phase_encoding_direction",
    "n_files",
    "min_instance_number",
    "max_instance_number",
    "n_unique_instance_numbers",
    "partial_flag",
    "series_dir",
    "example_dicom",
]


def looks_like_series_dir(path: Path) -> bool:
    try:
        return any(is_probable_dicom_file(p) for p in path.iterdir() if p.is_file())
    except Exception:
        return False


def looks_like_subject_session_dir(path: Path) -> bool:
    if looks_like_series_dir(path):
        return False
    try:
        for child in path.iterdir():
            if child.is_dir() and looks_like_series_dir(child):
                return True
    except Exception:
        return False
    return False


def infer_subject_session(input_dir: Path, raw_root: Optional[Path] = None) -> Tuple[str, str]:
    input_dir = input_dir.resolve()
    parts = list(input_dir.parts)

    subject = ""
    session = ""

    for p in reversed(parts):
        if re.match(r"^sub-[A-Za-z0-9]+", p, re.IGNORECASE):
            subject = p
            break

    for p in reversed(parts):
        if re.match(r"^ses-[A-Za-z0-9]+", p, re.IGNORECASE):
            session = p
            break

    if not subject and raw_root:
        try:
            rel = input_dir.relative_to(raw_root.resolve())
            rel_parts = rel.parts
            if len(rel_parts) >= 1:
                subject = rel_parts[0]
            if len(rel_parts) >= 2:
                session = rel_parts[1]
        except Exception:
            pass

    if not subject:
        if input_dir.parent != input_dir:
            subject = input_dir.parent.name
            session = input_dir.name
        else:
            subject = input_dir.name

    return subject, session


def discover_subject_session_dirs(raw_root: Path) -> List[Path]:
    found = []
    for dirpath, dirnames, _ in os.walk(raw_root):
        path = Path(dirpath)
        if looks_like_subject_session_dir(path):
            found.append(path)
            dirnames[:] = []
    return sorted(set(found))


def output_path_for(output_dir: Path, subject: str, session: str) -> Path:
    tag = subject
    if session:
        tag = f"{tag}_{session}"
    return output_dir / f"{tag}_series.tsv"


def write_subject_tsv(
    input_dir: Path,
    output_dir: Path,
    subject: Optional[str] = None,
    session: Optional[str] = None,
    raw_root: Optional[Path] = None,
    force: bool = False,
) -> Optional[Path]:
    inferred_subject, inferred_session = infer_subject_session(input_dir, raw_root=raw_root)
    subject = subject or inferred_subject
    session = session if session is not None else inferred_session

    output_dir.mkdir(parents=True, exist_ok=True)
    out_tsv = output_path_for(output_dir, subject, session)

    if out_tsv.exists() and not force:
        print(f"[SKIP] {subject} {session or ''}: existing TSV found: {out_tsv}")
        return None

    series = group_by_series(input_dir)
    rows = [
        summarize_series(uid, meta, subject, session)
        for uid, meta in series.items()
        if meta.get("header") is not None
    ]

    rows.sort(key=lambda r: (
        safe_int(r["series_number"]),
        sort_time(r["acquisition_time"]),
        sort_time(r["series_time"]),
        r["series_description"].casefold(),
    ))

    with out_tsv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"[WRITE] {subject} {session or ''}: {len(rows)} series -> {out_tsv}")
    return out_tsv


def build_parser():
    p = argparse.ArgumentParser(
        description="Generate ordered per-subject DICOM series TSV inventories."
    )
    p.add_argument("--input-dir", required=True, type=Path)
    p.add_argument("--output-dir", required=True, type=Path)
    p.add_argument("--batch", action="store_true")
    p.add_argument("--subject", default=None)
    p.add_argument("--session", default=None)
    p.add_argument("--force", action="store_true")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not input_dir.is_dir():
        print(f"ERROR: input dir does not exist: {input_dir}", file=sys.stderr)
        return 2

    if args.batch:
        subject_dirs = discover_subject_session_dirs(input_dir)
        if not subject_dirs:
            print(f"ERROR: no subject/session DICOM directories found under {input_dir}", file=sys.stderr)
            return 1

        print(f"[INFO] Found {len(subject_dirs)} subject/session directories under {input_dir}")
        n_written = 0
        n_skipped = 0
        for subject_dir in subject_dirs:
            result = write_subject_tsv(
                input_dir=subject_dir,
                output_dir=output_dir,
                raw_root=input_dir,
                force=args.force,
            )
            if result is None:
                n_skipped += 1
            else:
                n_written += 1

        print(f"[SUMMARY] written={n_written} skipped_existing={n_skipped}")
        return 0

    result = write_subject_tsv(
        input_dir=input_dir,
        output_dir=output_dir,
        subject=args.subject,
        session=args.session,
        raw_root=None,
        force=args.force,
    )
    print(f"[SUMMARY] written={1 if result else 0} skipped_existing={1 if result is None else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
