
"""
query_series.py
===============

Fast per-subject/per-session DICOM series inventory.

Designed for DICOM directories staged by dicom/dcm2dir:

    RAW_ROOT/
        SubjectID/
            SessionID/
                <SeriesDescription>_<SeriesNumber>/
                    *.dcm

or:

    RAW_ROOT/
        SubjectID/
            <SeriesDescription>_<SeriesNumber>/
                *.dcm

Default mode is fast:
  - treats each discovered DICOM-containing folder as one series
  - counts files in that folder
  - reads one representative DICOM header per series
  - preserves scan order using SeriesNumber/AcquisitionTime/SeriesTime

Optional deep QC:
  - --check-instances reads every DICOM in each series to detect missing instance numbers
  - this is slower and should only be used when needed

Examples
--------
Single subject/session:
    python query_series.py --input-dir /raw/mri/sub-001/ses-001 --output-dir /logs

Batch all sessions:
    python query_series.py --input-dir /raw/mri --output-dir /logs --batch

Batch only ses-001:
    python query_series.py --input-dir /raw/mri --output-dir /logs --batch --session-filter ses-001

Regenerate existing TSVs:
    python query_series.py --input-dir /raw/mri --output-dir /logs --batch --force

Run slower instance-number QC:
    python query_series.py --input-dir /raw/mri --output-dir /logs --batch --check-instances
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pydicom


DICOM_EXTENSIONS = {".dcm", ".ima", ""}

HEADER_TAGS = [
    "StudyDate",
    "SeriesDate",
    "AcquisitionDate",
    "SeriesTime",
    "AcquisitionTime",
    "SeriesNumber",
    "SeriesInstanceUID",
    "SeriesDescription",
    "ProtocolName",
    "SequenceName",
    "ImageType",
    "EchoTime",
    "EchoNumber",
    "RepetitionTime",
    "PhaseEncodingDirection",
    "InPlanePhaseEncodingDirection",
    "InstanceNumber",
]


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


def is_probable_dicom_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name.startswith("."):
        return False
    return path.suffix.lower() in DICOM_EXTENSIONS


def iter_dicom_candidates(series_dir: Path) -> Iterable[Path]:
    """Yield candidate DICOM files directly inside a series folder."""
    try:
        for p in series_dir.iterdir():
            if is_probable_dicom_file(p):
                yield p
    except Exception:
        return


def read_dicom_header(path: Path):
    try:
        return pydicom.dcmread(
            str(path),
            stop_before_pixels=True,
            force=True,
            specific_tags=HEADER_TAGS,
        )
    except TypeError:
        # Older pydicom compatibility.
        return pydicom.dcmread(str(path), stop_before_pixels=True, force=True)


def first_readable_dicom(series_dir: Path) -> Optional[Path]:
    for p in iter_dicom_candidates(series_dir):
        try:
            read_dicom_header(p)
            return p
        except Exception:
            continue
    return None


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


def normalize_session_label(session: str) -> str:
    s = str(session or "").strip()
    if not s:
        return ""
    if s.lower().startswith("ses-"):
        s = s[4:]
    return f"ses-{s}".casefold()


def infer_subject_session(input_dir: Path, raw_root: Optional[Path] = None) -> Tuple[str, str]:
    input_dir = input_dir.resolve()
    subject = ""
    session = ""

    parts = list(input_dir.parts)
    for p in reversed(parts):
        if re.match(r"^ses-[A-Za-z0-9]+", p, re.IGNORECASE):
            session = p
            break
    for p in reversed(parts):
        if re.match(r"^sub-[A-Za-z0-9]+", p, re.IGNORECASE):
            subject = p
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
        # Single-subject mode fallback.
        if input_dir.parent != input_dir:
            subject = input_dir.parent.name
            session = input_dir.name
        else:
            subject = input_dir.name

    return subject, session


def discover_session_dirs_by_glob(raw_root: Path, session_filters: Optional[List[str]]) -> List[Path]:
    """
    Trust the expected raw layout first.

    This directly supports:
        raw_root/sub-*/ses-001
        raw_root/<subject>/ses-001
    """
    found = []

    if session_filters:
        # Use explicit session globs first; this avoids walking the entire tree.
        for s in session_filters:
            s_norm = normalize_session_label(s)
            session_name = s_norm  # normalize_session_label returns ses-*
            found.extend(p for p in raw_root.glob(f"*/{session_name}") if p.is_dir())

            # Also try the exact user-provided label in case it is non-standard.
            s_raw = str(s).strip()
            if s_raw and s_raw != session_name:
                found.extend(p for p in raw_root.glob(f"*/{s_raw}") if p.is_dir())
    else:
        # Common session layout.
        found.extend(p for p in raw_root.glob("*/ses-*") if p.is_dir())

        # No-session layout: raw_root/subject/series/*.dcm
        for p in raw_root.iterdir() if raw_root.exists() else []:
            if p.is_dir() and list_series_dirs(p):
                found.append(p)

    return sorted(set(found))


def subject_session_dirs_fallback_walk(raw_root: Path, session_filters: Optional[List[str]]) -> List[Path]:
    """
    Slower fallback for unusual nesting. Finds directories that contain DICOM series folders.
    """
    wanted = None
    if session_filters:
        wanted = {normalize_session_label(s) for s in session_filters}

    found = []
    for dirpath, dirnames, _ in os.walk(raw_root):
        path = Path(dirpath)
        subject, session = infer_subject_session(path, raw_root=raw_root)

        if wanted is not None and normalize_session_label(session) not in wanted:
            # If this directory itself looks like a nonmatching session, prune it.
            if path.name.lower().startswith("ses-"):
                dirnames[:] = []
            continue

        if list_series_dirs(path):
            found.append(path)
            dirnames[:] = []

    return sorted(set(found))


def subject_session_dirs(raw_root: Path, session_filters: Optional[List[str]] = None) -> List[Path]:
    """
    Find subject/session directories.

    The fast path uses direct globbing such as raw_root/*/ses-001.
    The fallback walks only if direct globbing finds nothing usable.
    """
    candidates = discover_session_dirs_by_glob(raw_root, session_filters)

    usable = [p for p in candidates if list_series_dirs(p)]
    if usable:
        return sorted(set(usable))

    # If globbed sessions exist but list_series_dirs failed, still try fallback.
    return subject_session_dirs_fallback_walk(raw_root, session_filters)


def output_path_for(output_dir: Path, subject: str, session: str) -> Path:
    tag = subject
    if session:
        tag = f"{tag}_{session}"
    return output_dir / f"{tag}_series.tsv"


def list_series_dirs(input_dir: Path) -> List[Path]:
    """
    Return DICOM-containing series folders under a subject/session directory.

    First checks direct children, as produced by dcm2dir.
    If none are found, falls back to a recursive search for DICOM-containing folders.
    This handles an extra nesting level without requiring a full raw-root scan.
    """
    direct = []
    try:
        for child in input_dir.iterdir():
            if child.is_dir() and first_readable_dicom(child) is not None:
                direct.append(child)
    except Exception:
        pass

    if direct:
        return sorted(direct)

    recursive = []
    for dirpath, dirnames, _ in os.walk(input_dir):
        path = Path(dirpath)
        if path == input_dir:
            continue
        if first_readable_dicom(path) is not None:
            recursive.append(path)
            dirnames[:] = []

    return sorted(set(recursive))


def count_candidate_files(series_dir: Path) -> int:
    return sum(1 for _ in iter_dicom_candidates(series_dir))


def inspect_instances(series_dir: Path) -> Tuple[str, str, str, str]:
    """
    Slow optional QC. Reads every DICOM in the series folder.
    Returns min, max, n_unique, partial_flag.
    """
    instance_numbers = []
    n_files = 0

    for path in iter_dicom_candidates(series_dir):
        n_files += 1
        try:
            ds = read_dicom_header(path)
            inst = safe_int(get_value(ds, "InstanceNumber"), default=-1)
            if inst >= 0:
                instance_numbers.append(inst)
        except Exception:
            continue

    partial_flag = ""
    if instance_numbers:
        min_instance = min(instance_numbers)
        max_instance = max(instance_numbers)
        n_unique = len(set(instance_numbers))
        expected_span = max_instance - min_instance + 1
        if expected_span > n_unique:
            partial_flag = "missing_instance_numbers"
        elif n_files < 5:
            partial_flag = "very_few_files"
        return str(min_instance), str(max_instance), str(n_unique), partial_flag

    if n_files < 5:
        partial_flag = "very_few_files"
    return "", "", "", partial_flag


def summarize_series_dir(series_dir: Path, subject: str, session: str, check_instances: bool) -> Optional[dict]:
    example = first_readable_dicom(series_dir)
    if example is None:
        return None

    try:
        ds = read_dicom_header(example)
    except Exception:
        return None

    study_date = format_dicom_date(get_value(ds, "StudyDate"))
    series_date = format_dicom_date(get_value(ds, "SeriesDate"))
    acq_date = format_dicom_date(get_value(ds, "AcquisitionDate"))
    scan_date = study_date or series_date or acq_date

    n_files = count_candidate_files(series_dir)

    min_instance = ""
    max_instance = ""
    n_unique_instances = ""
    partial_flag = ""
    if check_instances:
        min_instance, max_instance, n_unique_instances, partial_flag = inspect_instances(series_dir)
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
        "series_instance_uid": get_value(ds, "SeriesInstanceUID"),
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
        "min_instance_number": min_instance,
        "max_instance_number": max_instance,
        "n_unique_instance_numbers": n_unique_instances,
        "partial_flag": partial_flag,
        "series_dir": str(series_dir),
        "example_dicom": str(example),
    }


def write_subject_tsv(
    input_dir: Path,
    output_dir: Path,
    subject: Optional[str] = None,
    session: Optional[str] = None,
    raw_root: Optional[Path] = None,
    force: bool = False,
    check_instances: bool = False,
) -> Optional[Path]:
    inferred_subject, inferred_session = infer_subject_session(input_dir, raw_root=raw_root)
    subject = subject or inferred_subject
    session = session if session is not None else inferred_session

    output_dir.mkdir(parents=True, exist_ok=True)
    out_tsv = output_path_for(output_dir, subject, session)

    if out_tsv.exists() and not force:
        print(f"[SKIP] {subject} {session or ''}: existing TSV found: {out_tsv}")
        return None

    series_dirs = list_series_dirs(input_dir)
    rows = []
    for series_dir in series_dirs:
        row = summarize_series_dir(series_dir, subject, session, check_instances=check_instances)
        if row is not None:
            rows.append(row)

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
    p.add_argument(
        "--session-filter",
        action="append",
        default=None,
        help=(
            "Batch mode only: process only matching session labels, e.g. ses-001. "
            "Can be supplied more than once. Matching is case-insensitive and accepts 001 or ses-001."
        ),
    )
    p.add_argument("--force", action="store_true")
    p.add_argument(
        "--check-instances",
        action="store_true",
        help=(
            "Slower QC mode: read every DICOM in each series to compute min/max/unique "
            "InstanceNumber and flag missing instance-number gaps."
        ),
    )
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not input_dir.is_dir():
        print(f"ERROR: input dir does not exist: {input_dir}", file=sys.stderr)
        return 2

    if args.batch:
        subject_dirs = subject_session_dirs(input_dir, session_filters=args.session_filter)
        if not subject_dirs:
            print(f"ERROR: no subject/session DICOM directories found under {input_dir}", file=sys.stderr)
            if args.session_filter:
                wanted = ", ".join(args.session_filter)
                print(
                    f"Hint: expected paths like {input_dir}/sub-*/{wanted}/<series>/*.dcm",
                    file=sys.stderr,
                )
            return 1

        if args.session_filter:
            wanted = sorted({normalize_session_label(s) for s in args.session_filter})
            print(f"[INFO] Session filter {wanted}: {len(subject_dirs)} subject/session directories selected")
        else:
            print(f"[INFO] Found {len(subject_dirs)} subject/session directories under {input_dir}")

        n_written = 0
        n_skipped = 0
        for subject_dir in subject_dirs:
            result = write_subject_tsv(
                input_dir=subject_dir,
                output_dir=output_dir,
                raw_root=input_dir,
                force=args.force,
                check_instances=args.check_instances,
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
        check_instances=args.check_instances,
    )
    print(f"[SUMMARY] written={1 if result else 0} skipped_existing={1 if result is None else 0}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
