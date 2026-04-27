
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

Default mode:
  - one row per series folder
  - count files per series folder
  - read one representative DICOM header per series
  - ordered by SeriesNumber / AcquisitionTime / SeriesTime
  - skip existing TSV unless --force

Optional:
  - --check-instances reads every DICOM in each series to check InstanceNumber gaps
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


def log(message: str):
    print(message, flush=True)


def warn(message: str):
    print(message, file=sys.stderr, flush=True)


def is_probable_dicom_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.name.startswith("."):
        return False
    return path.suffix.lower() in DICOM_EXTENSIONS


def iter_dicom_candidates(series_dir: Path) -> Iterable[Path]:
    try:
        for p in sorted(series_dir.iterdir()):
            if is_probable_dicom_file(p):
                yield p
    except Exception:
        return


def read_dicom_header(path: Path):
    # Match the user's successful manual test. Avoid specific_tags because it can
    # behave differently across pydicom versions / unusual files.
    return pydicom.dcmread(str(path), stop_before_pixels=True, force=True)


def first_readable_dicom(series_dir: Path, verbose: bool = False) -> Optional[Path]:
    n_candidates = 0
    first_error = None

    for p in iter_dicom_candidates(series_dir):
        n_candidates += 1
        try:
            read_dicom_header(p)
            return p
        except Exception as e:
            if first_error is None:
                first_error = f"{type(e).__name__}: {str(e)[:300]}"
            continue

    if verbose and n_candidates:
        warn(f"[WARN] {series_dir}: {n_candidates} candidate file(s), none readable. First error: {first_error}")
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
        if input_dir.parent != input_dir:
            subject = input_dir.parent.name
            session = input_dir.name
        else:
            subject = input_dir.name

    return subject, session


def candidate_session_dirs_by_glob(raw_root: Path, session_filters: Optional[List[str]]) -> List[Path]:
    found = []

    if session_filters:
        for s in session_filters:
            s_norm = normalize_session_label(s)
            s_raw = str(s).strip()

            patterns = [f"*/{s_norm}"]
            if s_raw and s_raw != s_norm:
                patterns.append(f"*/{s_raw}")

            for pattern in patterns:
                matches = [p for p in raw_root.glob(pattern) if p.is_dir()]
                log(f"[DISCOVER] pattern={raw_root / pattern}  matches={len(matches)}")
                found.extend(matches)
    else:
        matches = [p for p in raw_root.glob("*/ses-*") if p.is_dir()]
        log(f"[DISCOVER] pattern={raw_root / '*/ses-*'}  matches={len(matches)}")
        found.extend(matches)

        try:
            subject_dirs = [p for p in raw_root.iterdir() if p.is_dir() and p not in found]
            log(f"[DISCOVER] possible no-session subject dirs={len(subject_dirs)}")
            found.extend(subject_dirs)
        except Exception:
            pass

    return sorted(set(found))


def output_path_for(output_dir: Path, subject: str, session: str) -> Path:
    tag = subject
    if session:
        tag = f"{tag}_{session}"
    return output_dir / f"{tag}_series.tsv"


def list_series_dirs(input_dir: Path, verbose: bool = False) -> List[Path]:
    direct = []
    children = []
    try:
        children = [child for child in sorted(input_dir.iterdir()) if child.is_dir()]
    except Exception as e:
        warn(f"[WARN] Could not list {input_dir}: {type(e).__name__}: {e}")
        return []

    if verbose:
        log(f"[DEBUG] {input_dir}: {len(children)} child directories")

    for child in children:
        # Do not require successful read twice unless needed for validation.
        n_files = sum(1 for _ in iter_dicom_candidates(child))
        if n_files == 0:
            if verbose:
                log(f"[DEBUG] {child.name}: no candidate DICOM files")
            continue

        example = first_readable_dicom(child, verbose=verbose)
        if example is not None:
            direct.append(child)
        elif verbose:
            warn(f"[WARN] {child.name}: {n_files} candidate DICOM files but no readable header")

    if direct:
        return sorted(direct)

    # Fallback: some layouts have one extra nesting level.
    recursive = []
    for dirpath, dirnames, _ in os.walk(input_dir):
        path = Path(dirpath)
        if path == input_dir:
            continue

        n_files = sum(1 for _ in iter_dicom_candidates(path))
        if n_files == 0:
            continue

        example = first_readable_dicom(path, verbose=verbose)
        if example is not None:
            recursive.append(path)
            dirnames[:] = []

    return sorted(set(recursive))


def count_candidate_files(series_dir: Path) -> int:
    return sum(1 for _ in iter_dicom_candidates(series_dir))


def inspect_instances(series_dir: Path) -> Tuple[str, str, str, str]:
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
    verbose: bool = False,
) -> Tuple[str, Optional[Path], int]:
    inferred_subject, inferred_session = infer_subject_session(input_dir, raw_root=raw_root)
    subject = subject or inferred_subject
    session = session if session is not None else inferred_session

    output_dir.mkdir(parents=True, exist_ok=True)
    out_tsv = output_path_for(output_dir, subject, session)

    if out_tsv.exists() and not force:
        log(f"[SKIP] {subject} {session or ''}: existing TSV found: {out_tsv}")
        return "skipped", None, 0

    series_dirs = list_series_dirs(input_dir, verbose=verbose)
    log(f"[SERIES] {subject} {session or ''}: found {len(series_dirs)} series folders")

    if not series_dirs:
        warn(f"[WARN] {subject} {session or ''}: no readable DICOM series folders found under {input_dir}")
        if verbose:
            warn("[WARN] Try checking the first child directory manually with pydicom.dcmread(..., stop_before_pixels=True, force=True)")
        return "no_series", None, 0

    rows = []
    for i, series_dir in enumerate(series_dirs, start=1):
        if i == 1 or i == len(series_dirs) or i % 10 == 0:
            log(f"[READ] {subject} {session or ''}: series {i}/{len(series_dirs)}")
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

    log(f"[WRITE] {subject} {session or ''}: {len(rows)} series -> {out_tsv}")
    return "written", out_tsv, len(rows)


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
        help="Batch mode only: process matching session labels, e.g. ses-001. Can be repeated.",
    )
    p.add_argument("--force", action="store_true")
    p.add_argument(
        "--check-instances",
        action="store_true",
        help="Slower QC mode: read every DICOM in each series to check InstanceNumber gaps.",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print debug details for folder discovery and unreadable files.",
    )
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)

    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not input_dir.is_dir():
        warn(f"ERROR: input dir does not exist: {input_dir}")
        return 2

    if args.batch:
        log(f"[INFO] Batch input root: {input_dir}")
        log(f"[INFO] Output dir: {output_dir}")
        if args.check_instances:
            log("[INFO] --check-instances enabled: this reads every DICOM in each series and may be slow")

        subject_dirs = candidate_session_dirs_by_glob(input_dir, args.session_filter)

        if not subject_dirs:
            warn(f"ERROR: no candidate subject/session directories found under {input_dir}")
            if args.session_filter:
                wanted = ", ".join(args.session_filter)
                warn(f"Hint: expected paths like {input_dir}/sub-*/{wanted}/<series>/*.dcm")
            return 1

        if args.session_filter:
            wanted = sorted({normalize_session_label(s) for s in args.session_filter})
            log(f"[INFO] Session filter {wanted}: {len(subject_dirs)} candidate subject/session directories")
        else:
            log(f"[INFO] Found {len(subject_dirs)} candidate subject/session directories")

        n_written = 0
        n_skipped = 0
        n_no_series = 0
        n_errors = 0

        total = len(subject_dirs)
        for idx, subject_dir in enumerate(subject_dirs, start=1):
            subject, session = infer_subject_session(subject_dir, raw_root=input_dir)
            log(f"[START] {idx}/{total} {subject} {session or ''}  path={subject_dir}")

            try:
                status, _, _ = write_subject_tsv(
                    input_dir=subject_dir,
                    output_dir=output_dir,
                    raw_root=input_dir,
                    force=args.force,
                    check_instances=args.check_instances,
                    verbose=args.verbose,
                )
            except Exception as e:
                n_errors += 1
                warn(f"[ERROR] {subject} {session or ''}: {type(e).__name__}: {e}")
                continue

            if status == "written":
                n_written += 1
            elif status == "skipped":
                n_skipped += 1
            elif status == "no_series":
                n_no_series += 1

        log(
            f"[SUMMARY] candidates={total} written={n_written} "
            f"skipped_existing={n_skipped} no_series={n_no_series} errors={n_errors}"
        )
        return 0 if n_written or n_skipped else 1

    log(f"[INFO] Single input dir: {input_dir}")
    status, _, _ = write_subject_tsv(
        input_dir=input_dir,
        output_dir=output_dir,
        subject=args.subject,
        session=args.session,
        raw_root=None,
        force=args.force,
        check_instances=args.check_instances,
        verbose=args.verbose,
    )
    log(f"[SUMMARY] status={status}")
    return 0 if status in {"written", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
