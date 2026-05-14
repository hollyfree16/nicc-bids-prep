"""
core_protocol_discovery.py
==========================

Discovers core imaging protocols per year from a concatenated *_series.tsv
produced by query_series.py + concat_series_tsvs.py.

Bucketing uses three fields in priority order (all data-driven, no hardcoded
series names):
  1. sequence_name  – most reliable technical indicator (scanner-stamped)
  2. protocol_name  – operator-defined label
  3. series_description – free-text fallback

Exclusions are driven by image_type (DERIVED catches scalar maps, MoCo outputs,
ASL subtraction, etc.) and a small set of structural rules for ORIGINAL rows
that are still not source acquisition data (reports, navigators, SBRef, mIP).

Outputs
-------
  excluded.tsv        every excluded row and why
  distribution.tsv    year × bucket × series_description with n_subjects, pct, is_core
  core_by_year.tsv    year-first: one row per year per bucket showing core and partial variants

Usage
-----
  python dicom/core_protocol_discovery.py \\
      --input  docs/mssm_ses-001_series.tsv \\
      --output-dir review_out/mssm \\
      --site MSSM

  # Focus on a single year
  python dicom/core_protocol_discovery.py \\
      --input  docs/mssm_ses-001_series.tsv \\
      --output-dir review_out/mssm \\
      --site MSSM \\
      --year 2022

  # Multiple years
  python dicom/core_protocol_discovery.py \\
      --input  docs/mssm_ses-001_series.tsv \\
      --output-dir review_out/mssm \\
      --site MSSM \\
      --year 2022 2023 \\
      --core-threshold 0.80
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ── Text normalisation ─────────────────────────────────────────────────────────

def norm_text(value: str) -> str:
    """Lowercase and collapse separators (_, -, ., *) to spaces."""
    value = (value or "").casefold()
    value = re.sub(r"[_\-\.\*]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def image_type_tokens(row: dict) -> Set[str]:
    return {t.casefold() for t in (row.get("image_type") or "").split("\\") if t}


# ── Exclusion ──────────────────────────────────────────────────────────────────
#
# DERIVED catches the vast majority of non-source rows (scalar maps, motion-
# corrected volumes, ASL subtraction, reformats, etc.).  A small set of
# additional rules handles ORIGINAL rows that are still not acquisition data.

def should_exclude(row: dict) -> str:
    """Return an exclusion reason string, or '' if the row should be kept."""
    desc  = (row.get("series_description") or "").strip()
    proto = (row.get("protocol_name") or "").strip()
    seq   = (row.get("sequence_name") or "").strip()
    toks  = image_type_tokens(row)

    # Primary: not a source image.
    if "derived" in toks:
        return "DERIVED"

    # Scanner non-image outputs (reports, phoenix documents).
    if "csa report" in toks or "report" in toks:
        return "scanner report"
    if re.search(r"PhoenixZIPReport|Phoenix Document|DoseReport|Visage", desc, re.IGNORECASE):
        return "scanner report"

    # EPI navigator (motion-correction reference volume, not image data).
    if re.search(r"EpiNav", seq, re.IGNORECASE):
        return "EPI navigator"

    # SBRef is intentionally NOT excluded here — single-band references are
    # valid BIDS _sbref files and must appear in the candidate template.

    # Localizers and scouts.
    if re.search(r"^(Localizer|localizer|AAHScout|AAScout|Scout|Survey)", desc):
        return "localizer/scout"
    if re.search(r"^(Localizer|localizer|AAHScout|AAScout|Scout|Survey)", proto):
        return "localizer/scout"

    # mIP / MNIP projections (ORIGINAL but visually derived max-intensity projection).
    if "mnip" in toks or re.search(r"\bmIP\b|\bMNIP\b", desc):
        return "mIP/MNIP"

    # MPR reformats — protect MPRAGE/MEMPRAGE source series.
    desc_low = desc.casefold()
    if "mprage" not in desc_low and "memprage" not in desc_low:
        if re.search(r"(^|[_\s-])MPR([_\s-]|$)|_MPR_| Ax MPR| Cor MPR| Tra MPR", desc, re.IGNORECASE):
            return "MPR reformat"

    return ""


# ── Bucket assignment ──────────────────────────────────────────────────────────
#
# Buckets are checked in order — more specific before general:
#   FieldMap before fMRI  (SpinEchoFieldMap has FMRI in image_type)
#   FLAIR before T2w      (both can use SPACE/spc sequence prefix)
#
# Each bucket has keyword lists for sequence_name, protocol_name, and
# series_description.  The first field with a match wins (sequence_name first).
# All comparisons use norm_text() — case-insensitive, separators → spaces.

BUCKET_RULES: List[Tuple[str, Dict[str, List[str]]]] = [
    ("Localizer", {
        "sequence_name":      ["fl2d", "fl3d1"],
        "protocol_name":      ["localiz", "aahscout", "aaascout", "scout", "survey", "3pl loc"],
        "series_description": ["localiz", "aahscout", "scout"],
    }),
    ("FieldMap", {
        # SpinEchoFieldMaps often have empty sequence_name; caught via protocol/desc.
        # *fm2d* = GRE fieldmap (magnitude + phase).
        "sequence_name":      ["fm2d"],
        "protocol_name":      ["fieldmap", "spinechofieldmap", "b0 map", "b0map"],
        "series_description": ["fieldmap", "b0 map"],
    }),
    ("FLAIR", {
        # *spcir* = SPACE with inversion recovery = FLAIR.
        "sequence_name":      ["spcir"],
        "protocol_name":      ["flair"],
        "series_description": ["flair"],
    }),
    ("T1w", {
        # tfl3d = 3D TFL = MPRAGE; tfl me3d = multi-echo TFL = MEMPRAGE.
        "sequence_name":      ["tfl3d", "tfl me3d"],
        "protocol_name":      ["mprage", "memprage", "tfl epinav", "tfl3d", "t1w", "sag t1", "t1 3d"],
        "series_description": ["mprage", "memprage", "t1w"],
    }),
    ("T2w", {
        # spc = SPACE (non-IR); tse2d = 2D TSE.  Listed after FLAIR so spcir → FLAIR.
        "sequence_name":      ["spc", "tse2d"],
        "protocol_name":      ["t2w", "t2 spc", "t2 tse", "t2 hires", "sag t2", "ax t2"],
        "series_description": ["t2w", "t2 spc", "t2 tse", "t2 hires", "ax t2"],
    }),
    ("SWI", {
        # swi3d* on Siemens; mag/pha images caught via description fallback.
        "sequence_name":      ["swi"],
        "protocol_name":      ["swi"],
        "series_description": ["swi", "mag images", "pha images"],
    }),
    ("ASL", {
        # tgse = turbo gradient spin echo used for pCASL on Siemens.
        "sequence_name":      ["tgse"],
        "protocol_name":      ["asl", "pcasl", "pasl", "tgse"],
        "series_description": ["asl", "pcasl", "pasl"],
    }),
    ("DWI", {
        # ep_b* = EPI diffusion (b-value prefix on Siemens); ep2d diff = older naming.
        "sequence_name":      ["ep b"],
        "protocol_name":      ["dwi", "dti", "dmri", "diffusion"],
        "series_description": ["dwi", "dti", "dmri", "diffusion"],
    }),
    ("fMRI", {
        # epfid2d = gradient-echo EPI = BOLD fMRI.
        "sequence_name":      ["epfid2d"],
        "protocol_name":      ["bold", "fmri", "rfmri", "ep2d bold", "cmrr ep2d"],
        "series_description": ["bold", "fmri", "rfmri"],
    }),
]

BUCKET_ORDER = [name for name, _ in BUCKET_RULES]

# ── BIDS name suggestion ───────────────────────────────────────────────────────

BUCKET_TO_BIDS: Dict[str, Tuple[str, str]] = {
    "T1w":       ("anat",   "T1w"),
    "T2w":       ("anat",   "T2w"),
    "FLAIR":     ("anat",   "FLAIR"),
    "DWI":       ("dwi",    "dwi"),
    "fMRI":      ("func",   "bold"),
    "ASL":       ("perf",   "asl"),
    "SWI":       ("swi",    "swi"),
    "FieldMap":  ("fmap",   "epi"),
    "Localizer": ("IGNORE", "IGNORE"),
    "Unknown":   ("?",      "?"),
}

# Clinical sequences first; housekeeping (Localizer, Unknown) last.
_TEMPLATE_BUCKET_ORDER = [
    "T1w", "T2w", "FLAIR", "SWI", "DWI", "fMRI", "ASL", "FieldMap", "Localizer", "Unknown",
]
_TEMPLATE_BUCKET_RANK = {b: i for i, b in enumerate(_TEMPLATE_BUCKET_ORDER)}

# ── Rerun detection ───────────────────────────────────────────────────────────
# Mirrors generate_bids_configs.py RERUN_RE + adds dot-number suffix (.2, .3…)
_RERUN_RE        = re.compile(r"[_\s]+(rr|rerun|repeat|redo|2nd)($|(?=_))", re.IGNORECASE)
_RERUN_DOT_RE    = re.compile(r"\.\d+$")          # e.g. EMOTION_SMS4_1.2
_RERUN_PREFIX_RE = re.compile(r"^Repeat_", re.IGNORECASE)


def _is_rerun(desc: str) -> bool:
    return (bool(_RERUN_RE.search(desc))
            or bool(_RERUN_PREFIX_RE.match(desc))
            or bool(_RERUN_DOT_RE.search(desc)))


def _strip_rerun(desc: str) -> str:
    """Return the base series name by removing rerun suffixes/prefixes."""
    s = _RERUN_PREFIX_RE.sub("", desc)
    s = _RERUN_RE.sub("", s)
    s = _RERUN_DOT_RE.sub("", s)
    return s.strip("_ ").strip()


def suggest_bids(bucket: str, row: dict) -> Tuple[str, str]:
    """Return (bids_folder, bids_suffix).

    Suffix includes BIDS entities where detectable so the template is
    immediately usable without further translation (e.g. 'task-rest_bold',
    'dir-AP_dwi', 'part-mag_swi').  Placeholders like 'task-TASKNAME_bold'
    mark spots the reviewer must fill in.
    """
    folder, suffix = BUCKET_TO_BIDS.get(bucket, ("?", "?"))
    desc      = (row.get("series_description") or "").strip()
    desc_norm = norm_text(desc)
    seq_n     = norm_text(row.get("sequence_name") or "")

    is_sbref = bool(re.search(r"SBRef(_|$)", desc, re.IGNORECASE))

    if is_sbref:
        # Folder stays as whatever the parent modality suggests (func/dwi).
        # Build suffix: pick up task or dir entities if visible in the name.
        entities: List[str] = []
        if "rest" in desc_norm:
            entities.append("task-rest")
        elif bucket == "fMRI":
            entities.append("task-TASKNAME")
        for d in ("AP", "PA", "LR", "RL"):
            if re.search(rf"[_\s]{d}([_\s]|$)", desc):
                entities.append(f"dir-{d}")
                break
        suffix = "_".join(entities + ["sbref"]) if entities else "sbref"

    elif bucket == "SWI":
        first = re.split(r"[_\s]", desc)[0].upper()
        if first in ("MAG", "MAGNITUDE"):
            suffix = "part-mag_swi"
        elif first in ("PHA", "PHASE"):
            suffix = "part-phase_swi"
        # else suffix = "swi" (default from BUCKET_TO_BIDS)

    elif bucket == "FieldMap":
        if "fm2d" in seq_n:
            suffix = "phasediff"
        else:
            for d in ("AP", "PA", "LR", "RL"):
                if re.search(rf"[_\s]{d}([_\s]|$)", desc):
                    suffix = f"dir-{d}_epi"
                    break

    elif bucket == "fMRI":
        if "rest" in desc_norm:
            suffix = "task-rest_bold"
        else:
            suffix = "task-TASKNAME_bold"

    elif bucket == "DWI":
        for d in ("AP", "PA", "LR", "RL"):
            if re.search(rf"[_\s]{d}([_\s]|$)", desc):
                suffix = f"dir-{d}_dwi"
                break

    elif bucket == "Unknown":
        suffix = "?"

    return folder, suffix


def assign_bucket(row: dict) -> str:
    seq   = norm_text(row.get("sequence_name") or "")
    proto = norm_text(row.get("protocol_name") or "")
    desc  = norm_text(row.get("series_description") or "")

    for bucket, rules in BUCKET_RULES:
        if seq and any(kw in seq for kw in rules.get("sequence_name", [])):
            return bucket
        if any(kw in proto for kw in rules.get("protocol_name", [])):
            return bucket
        if any(kw in desc for kw in rules.get("series_description", [])):
            return bucket

    return "Unknown"


# ── I/O helpers ───────────────────────────────────────────────────────────────

def read_tsv(path: Path) -> Tuple[List[dict], List[str]]:
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def write_tsv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"[WRITE] {path}  ({len(rows)} rows)")


# ── Subject date helpers ───────────────────────────────────────────────────────

def subject_first_date(all_rows: List[dict]) -> Dict[str, str]:
    dates: Dict[str, str] = {}
    for r in all_rows:
        subj = (r.get("subject") or "").strip()
        date = (r.get("scan_date") or "").strip()
        if not subj:
            continue
        if not date or date.startswith("ses-"):
            dates.setdefault(subj, "0000-00-00")
            continue
        if subj not in dates or date < dates[subj]:
            dates[subj] = date
    return dates


# ── Candidate template builder ────────────────────────────────────────────────

def build_candidate_template(
    kept_labeled: List[Tuple[dict, str]],
    excluded: List[dict],
    site: str,
    protocol_name_base: str,
    core_threshold: float,
    scan_dates: Dict[str, str],
    subj_by_year: Dict[str, List[str]],
    explicit_protocol_name: bool = False,
) -> List[dict]:
    """Build candidate template rows for manual review and use by generate_bids_configs.py.

    Outputs one row per (year × series_description) so protocol drift is visible
    when reviewing the file. Rows are sorted year → bucket → pct descending.

    protocol_name column:
      - explicit_protocol_name=False (default): auto-generates "{site} {year}" per row
      - explicit_protocol_name=True (--protocol-name was passed): uses protocol_name_base
        as-is for all kept rows (useful for epoch-specific runs with --year)

    Columns site…expected are read by generate_bids_configs.py; the annotation
    columns (year, bucket, pct, bids_note) are ignored downstream via
    extrasaction='ignore'.
    """
    # ── Collect per-description data ──────────────────────────────────────────
    desc_bucket:     Dict[str, Counter]             = defaultdict(Counter)
    desc_row:        Dict[str, dict]                = {}
    desc_year_subjs: Dict[str, Dict[str, Set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )

    for r, bucket in kept_labeled:
        subj = (r.get("subject") or "").strip()
        year = scan_dates.get(subj, "")[:4]
        desc = (r.get("series_description") or "").strip()
        if not desc or not subj or not year.isdigit():
            continue
        desc_bucket[desc][bucket] += 1
        desc_year_subjs[desc][year].add(subj)
        desc_row.setdefault(desc, r)

    # ── Pre-compute rank within (bucket, year) by n_subjects descending ─────────
    # desc_rank[(bucket, year, desc)] → 1-based rank (1 = most subjects = primary)
    _bk_items: Dict[Tuple[str, str], List[Tuple[int, str]]] = defaultdict(list)
    for desc, bucket_ctr in desc_bucket.items():
        bucket = bucket_ctr.most_common(1)[0][0]
        for year, subjs in desc_year_subjs[desc].items():
            if year.isdigit() and year != "0000":
                _bk_items[(bucket, year)].append((len(subjs), desc))
    desc_rank: Dict[Tuple[str, str, str], int] = {}
    for (bucket, year), items in _bk_items.items():
        items.sort(key=lambda x: -x[0])
        for rank_i, (_, d) in enumerate(items, 1):
            desc_rank[(bucket, year, d)] = rank_i

    # ── Pre-compute rerun cross-links per year ────────────────────────────────
    # rerun_base[year][rerun_desc] = base_desc (only if base also present that year)
    # base_reruns[year][base_desc] = [rerun_desc, ...]
    all_year_descs: Dict[str, Set[str]] = defaultdict(set)
    for desc, yd in desc_year_subjs.items():
        for year in yd:
            if year.isdigit() and year != "0000":
                all_year_descs[year].add(desc)

    rerun_base:  Dict[str, Dict[str, str]]       = defaultdict(dict)
    base_reruns: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
    for year, descs_set in all_year_descs.items():
        descs_cf = {d.casefold(): d for d in descs_set}
        for d in descs_set:
            if _is_rerun(d):
                base_cf = _strip_rerun(d).casefold()
                if base_cf in descs_cf:
                    base_actual = descs_cf[base_cf]
                    rerun_base[year][d] = base_actual
                    base_reruns[year][base_actual].append(d)

    # ── One row per (year × series_description) ───────────────────────────────
    kept_rows: List[dict] = []
    for desc, bucket_ctr in desc_bucket.items():
        bucket = bucket_ctr.most_common(1)[0][0]
        bids_folder, bids_suffix = suggest_bids(bucket, desc_row[desc])

        for year, subjs in desc_year_subjs[desc].items():
            if not year.isdigit() or year == "0000":
                continue
            n_year      = len(subj_by_year.get(year, []))
            n_subjects  = len(subjs)
            pct         = n_subjects / n_year if n_year else 0.0
            is_core     = pct >= core_threshold
            rank        = desc_rank.get((bucket, year, desc), 0)
            n_variants  = len(_bk_items.get((bucket, year), []))

            # is_rerun / rerun_of: explicit columns so reviewer sees the link
            is_rr   = desc in rerun_base.get(year, {})
            rr_of   = rerun_base[year][desc] if is_rr else ""

            # bids_note: only variant rank context (entity hints live in bids_suffix now)
            note = ""
            if n_variants > 1:
                note = f"{n_variants} {bucket} variants this year — rank {rank}/{n_variants}"
            if bucket == "Unknown":
                note = (f"{note} | " if note else "") + "UNCLASSIFIED — fill in bids_folder and bids_suffix"

            proto = (
                protocol_name_base
                if explicit_protocol_name
                else f"{protocol_name_base} {year}"
            )

            kept_rows.append({
                "series_description": desc,
                "bids_folder":        bids_folder,
                "bids_suffix":        bids_suffix,
                "fingerprint":        "yes" if is_core else "no",
                "expected":           "yes" if is_core else "no",
                "is_rerun":           "yes" if is_rr else "no",
                "rerun_of":           rr_of,
                "site":               site,
                "protocol_name":      proto,
                "year":               year,
                "n_year":             n_year,
                "n_subjects":         n_subjects,
                "bucket":             bucket,
                "bucket_rank":        rank,
                "pct":                f"{pct:.0%}",
                "bids_note":          note,
            })

    kept_rows.sort(key=lambda r: (
        r["year"],
        _TEMPLATE_BUCKET_RANK.get(r["bucket"], 999),
        r["bucket_rank"],
    ))

    # ── Excluded rows: one IGNORE row per (desc × year) seen in ≥ 2 subjects ───
    excl_year_subjs: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    excl_reason: Dict[str, str] = {}
    for r in excluded:
        desc   = (r.get("series_description") or "").strip()
        subj   = (r.get("subject") or "").strip()
        reason = (r.get("exclusion_reason") or "").strip()
        if not desc or not subj:
            continue
        year = scan_dates.get(subj, "")[:4]
        if year.isdigit() and year != "0000":
            excl_year_subjs[desc][year].add(subj)
        excl_reason.setdefault(desc, reason)

    ignore_rows: List[dict] = []
    for desc, year_subjs in excl_year_subjs.items():
        for year, subjs in sorted(year_subjs.items()):
            if len(subjs) < 2:
                continue
            proto = (
                protocol_name_base
                if explicit_protocol_name
                else f"{protocol_name_base} {year}"
            )
            ignore_rows.append({
                "series_description": desc,
                "bids_folder":        "IGNORE",
                "bids_suffix":        "IGNORE",
                "fingerprint":        "no",
                "expected":           "no",
                "is_rerun":           "no",
                "rerun_of":           "",
                "site":               site,
                "protocol_name":      proto,
                "year":               year,
                "n_year":             len(subj_by_year.get(year, [])),
                "n_subjects":         len(subjs),
                "bucket":             "excluded",
                "bucket_rank":        "",
                "pct":                "",
                "bids_note":          excl_reason.get(desc, ""),
            })

    return kept_rows + ignore_rows


# ── Core analysis ─────────────────────────────────────────────────────────────

def run(
    input_tsv: Path,
    output_dir: Path,
    site: str,
    core_threshold: float,
    year_filter: Optional[List[str]] = None,
    protocol_name: Optional[str] = None,
) -> None:
    all_rows, _ = read_tsv(input_tsv)
    print(f"[INFO] {len(all_rows)} total rows  ·  {input_tsv.name}")

    scan_dates = subject_first_date(all_rows)

    # Optional year filter: restrict to subjects whose first scan falls in the
    # specified year(s) before any further processing.
    if year_filter:
        year_set = set(year_filter)
        keep_subjects = {s for s, d in scan_dates.items() if d[:4] in year_set}
        all_rows = [r for r in all_rows if (r.get("subject") or "").strip() in keep_subjects]
        print(f"[INFO] year filter {sorted(year_set)}  →  {len(keep_subjects)} subjects  ·  {len(all_rows)} rows")

    if not all_rows:
        print("[WARN] No rows remain after filtering. Check --year values.")
        return

    # Exclusion pass
    kept: List[dict] = []
    excluded: List[dict] = []
    for r in all_rows:
        reason = should_exclude(r)
        if reason:
            excluded.append({**r, "exclusion_reason": reason})
        else:
            kept.append(r)

    print(f"[INFO] kept={len(kept)}  excluded={len(excluded)}")

    excl_fields = list(all_rows[0].keys()) + ["exclusion_reason"]
    write_tsv(output_dir / "excluded.tsv", excluded, excl_fields)

    # Build per-year subject lists
    all_subjects = sorted(
        {(r.get("subject") or "").strip() for r in kept if (r.get("subject") or "").strip()},
        key=lambda s: (scan_dates.get(s, "0000-00-00"), s),
    )
    years = sorted(
        {scan_dates.get(s, "")[:4] for s in all_subjects
         if scan_dates.get(s, "")[:4].isdigit() and scan_dates.get(s, "") != "0000-00-00"}
    )
    subj_by_year: Dict[str, List[str]] = defaultdict(list)
    for s in all_subjects:
        yr = scan_dates.get(s, "")[:4]
        if yr.isdigit():
            subj_by_year[yr].append(s)

    # Assign bucket once per kept row; reuse for distribution and template.
    kept_labeled: List[Tuple[dict, str]] = [(r, assign_bucket(r)) for r in kept]

    # Accumulate: bucket → year → series_description → set of subjects
    dist: Dict[str, Dict[str, Dict[str, Set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(set))
    )
    unknown_rows: List[dict] = []
    for r, bucket in kept_labeled:
        subj = (r.get("subject") or "").strip()
        year = scan_dates.get(subj, "")[:4]
        desc = (r.get("series_description") or "").strip()
        if not subj or not year.isdigit():
            continue
        if bucket == "Unknown":
            unknown_rows.append(r)
        else:
            dist[bucket][year][desc].add(subj)

    if unknown_rows:
        print(f"[WARN] {len(unknown_rows)} kept rows could not be bucketed → check Unknown section")

    # ── TSV: distribution (year-first) ────────────────────────────────────────
    dist_rows = []
    for year in years:
        n_year = len(subj_by_year[year])
        for bucket in BUCKET_ORDER:
            if bucket not in dist or year not in dist[bucket]:
                continue
            for desc, subjs in sorted(dist[bucket][year].items(), key=lambda x: -len(x[1])):
                n = len(subjs)
                pct = n / n_year if n_year else 0
                dist_rows.append({
                    "year":               year,
                    "n_year":             n_year,
                    "bucket":             bucket,
                    "series_description": desc,
                    "n_subjects":         n,
                    "pct":                f"{pct:.0%}",
                    "is_core":            "yes" if pct >= core_threshold else "no",
                })
    write_tsv(
        output_dir / "distribution.tsv",
        dist_rows,
        ["year", "n_year", "bucket", "series_description", "n_subjects", "pct", "is_core"],
    )

    # ── TSV: core by year ─────────────────────────────────────────────────────
    core_rows = []
    for year in years:
        n_year = len(subj_by_year[year])
        for bucket in BUCKET_ORDER:
            if bucket not in dist or year not in dist[bucket]:
                continue
            core: List[str] = []
            partial: List[str] = []
            for desc, subjs in sorted(dist[bucket][year].items(), key=lambda x: -len(x[1])):
                n = len(subjs)
                pct = n / n_year if n_year else 0
                entry = f"{desc} ({n}/{n_year} {pct:.0%})"
                if pct >= core_threshold:
                    core.append(entry)
                elif pct >= 0.10:
                    partial.append(entry)
            if core or partial:
                core_rows.append({
                    "year":    year,
                    "n_year":  n_year,
                    "bucket":  bucket,
                    "core":    " | ".join(core),
                    "partial": " | ".join(partial),
                })
    write_tsv(
        output_dir / "core_by_year.tsv",
        core_rows,
        ["year", "n_year", "bucket", "core", "partial"],
    )

    # ── TSV: per-year candidate templates for generate_bids_configs.py ──────────
    # One file per year: {year}_{site}_candidate_template.tsv
    # The IGNORE section (globally excluded series) goes at the bottom of each.
    TEMPLATE_COLS = [
        "series_description", "bids_folder", "bids_suffix",
        "fingerprint", "expected", "is_rerun", "rerun_of",
        "site", "protocol_name", "year", "n_year", "n_subjects",
        "bucket", "bucket_rank", "pct", "bids_note",
    ]
    base_name = protocol_name or site
    template_rows = build_candidate_template(
        kept_labeled=kept_labeled,
        excluded=excluded,
        site=site,
        protocol_name_base=base_name,
        core_threshold=core_threshold,
        scan_dates=scan_dates,
        subj_by_year=subj_by_year,
        explicit_protocol_name=protocol_name is not None,
    )

    # Split all rows (kept + IGNORE) by year — every row now has a year.
    rows_by_year: Dict[str, List[dict]] = defaultdict(list)
    for r in template_rows:
        if r["year"]:
            rows_by_year[r["year"]].append(r)

    for yr in years:
        yr_all = rows_by_year.get(yr, [])
        if not yr_all:
            continue
        yr_kept   = [r for r in yr_all if r["bucket"] != "excluded"]
        yr_ignore = [r for r in yr_all if r["bucket"] == "excluded"]
        fname = f"{yr}_{site}_candidate_template.tsv"
        write_tsv(output_dir / fname, yr_kept + yr_ignore, TEMPLATE_COLS)
        print(f"[OUT] {fname}  ({len(yr_kept)} sequences + {len(yr_ignore)} IGNORE)")

    # ── Console: year-first summary ───────────────────────────────────────────
    year_range = f"{years[0]}–{years[-1]}" if years else "?"
    print(f"\n{'─' * 72}")
    print(f"  {site}  ·  {len(all_subjects)} subjects  ·  {year_range}  ·  core ≥ {core_threshold:.0%}")
    print(f"{'─' * 72}")

    for year in years:
        n_year = len(subj_by_year[year])
        print(f"\n  [{year}]  n={n_year}")
        for bucket in BUCKET_ORDER:
            if bucket not in dist or year not in dist[bucket]:
                continue
            core_lines: List[str] = []
            partial_lines: List[str] = []
            for desc, subjs in sorted(dist[bucket][year].items(), key=lambda x: -len(x[1])):
                n = len(subjs)
                pct = n / n_year
                label = f"{desc} ({n}/{n_year} {pct:.0%})"
                if pct >= core_threshold:
                    core_lines.append(label)
                elif pct >= 0.10:
                    partial_lines.append(label)
            if not core_lines and not partial_lines:
                continue
            core_str    = " | ".join(core_lines) if core_lines else "—"
            partial_str = f"  partial: {' | '.join(partial_lines)}" if partial_lines else ""
            print(f"    {bucket:<12s}  core: {core_str}{partial_str}")

    if unknown_rows:
        print(f"\n  [Unknown bucket — review these {len(unknown_rows)} rows]")
        from collections import Counter
        top = Counter((r.get("series_description") or "").strip() for r in unknown_rows).most_common(10)
        for desc, n in top:
            print(f"    {n:4d}  {desc}")


# ── CLI ────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Discover core imaging protocols per year from a concatenated *_series.tsv.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--input",           required=True, type=Path,
                   help="Concatenated *_series.tsv (output of concat_series_tsvs.py)")
    p.add_argument("--output-dir",      required=True, type=Path)
    p.add_argument("--site",            default="",
                   help="Site label shown in console output")
    p.add_argument("--year",            dest="years", nargs="+", metavar="YEAR",
                   help="Restrict to subjects whose first scan falls in these year(s). "
                        "E.g. --year 2022  or  --year 2021 2022 2023")
    p.add_argument("--core-threshold",  type=float, default=0.80,
                   help="Fraction of year's subjects required to call a sequence core (default: 0.80)")
    p.add_argument("--protocol-name",   dest="protocol_name", default=None,
                   help="Value for the protocol_name column in candidate_template.tsv. "
                        "Defaults to '{site} {year_range}'. Combine with --year to generate "
                        "epoch-specific templates (e.g. --year 2014 2015 2016 --protocol-name 'MSSM 2014').")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if not args.input.exists():
        print(f"ERROR: {args.input} not found", file=sys.stderr)
        return 2
    run(
        input_tsv=args.input,
        output_dir=args.output_dir,
        site=args.site,
        core_threshold=args.core_threshold,
        year_filter=args.years,
        protocol_name=args.protocol_name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
