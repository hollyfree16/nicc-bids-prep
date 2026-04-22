#!/usr/bin/env python3
"""
generate_bids_configs.py
========================
Template-based per-subject heudiconv mapping.tsv and _BIDS.sh generator.

Templates are stored as TSV files in a templates/ directory (one per protocol).
Edit the TSV files directly to change BIDS mappings — no need to touch this script.

Directory structure
-------------------
    /code/heudiconv_v1.3.3/
        generate_bids_configs.py   <- this script
        heuristic.py
        templates/
            SiteA.tsv
            SiteB-1.tsv
            SiteB-2.tsv
            ...

Template TSV columns
--------------------
    site                Site label (e.g. SiteA)
    protocol_name       Human-readable protocol name
    fingerprint         yes/no -- used for protocol detection
    series_description  Exact SeriesDescription from the DICOM/log
    bids_folder         BIDS output folder (anat/func/dwi/fmap/perf/swi/IGNORE)
    bids_suffix         BIDS filename suffix (e.g. T1w, FLAIR, dwi) or IGNORE
    expected            yes/no -- warn if missing from subject's log

Usage
-----
    python generate_bids_configs.py \
        --logs_dir   /path/to/logs \
        --output_dir /path/to/output \
        --site       SiteA \
        [--templates_dir /code/heudiconv_v1.3.3/templates] \
        [--heuristic     /code/heudiconv_v1.3.3/heuristic.py] \
        [--dicom_template "raw/mri/sub-{subject}/ses-{session}/*/*.dcm"] \
        [--bids_output   /BIDS/]

Outputs per subject  (<output_dir>/sub-<subject>_ses-<session>/)
----------------------------------------------------------------
    <tag>_mapping.tsv          heudiconv mapping input
    <tag>_BIDS.sh              ready-to-run heudiconv command (with IntendedFor retry)
    <tag>_series_resolved.tsv  every series + its resolved status  [troubleshooting]
    <tag>_protocol_match.txt   template match details               [troubleshooting]

Site-level review files  (<output_dir>/)
----------------------------------------
    00_summary_all.tsv         all subjects x all series
    01_flagged_unknowns.tsv    series not in template and not always-ignored
    02_superseded_by_rerun.tsv bases suppressed by rerun versions
    03_incomplete_sessions.tsv subjects missing expected sequences
    04_protocol_detection.tsv  template matched + confidence per subject
"""

import argparse
import csv
import re
import sys
from pathlib import Path
from collections import defaultdict


# ============================================================================
# ALWAYS-IGNORE LIST
# Series discarded for every protocol regardless of template content.
# ============================================================================

ALWAYS_IGNORE_RE = re.compile(r"""(
    ^AAHScout | ^AAHScout_MPR |
    ^AAScout | ^AAScout_MPR |
    ^Localizer$ | ^Localizer_aligned$ | ^localizer$ |
    ^Loc$ | ^Loc_MPR |
    ^Survey | ^survey |
    ^1\sSLICE\sLOC |
    ^3pl\sloc |
    ^PhoenixZIPReport |
    ^Visage\sPresentation |
    ^SenseRefScan |
    ^MoCoSeries |
    ^relCBF |
    ^Perfusion_Weighted |
    ^WIP\sSOURCE |
    ^PosDisp |
    ^Design$ |
    ^EvaSeries_GLM |
    ^Mean_&_t-Maps |
    _ADC$ | _FA$ | _ColFA$ | _TRACEW$ |
    _TENSOR$ | _TENSOR_B0$ |
    _PhysioLog$ |
    _SBRef$ |
    _EPINav$ |
    _ND$ | _ND\s |
    \bRFMT\b | \bMPR\b |
    ^AX\s | ^COR\s | ^ax\s | ^cor\s |
    ^AXIAL\s | ^FLAIR\sRECON |
    ^AX$ | ^COR$ |
    ^3D_SAG_FLAIR_MPR |
    ^sag3D_Brain_View | ^zoom3D_Brain_View |
    ^Pending_ | ^TEST$ |
    _RR_ADC$ | _RR_FA$ | _RR_ColFA$ | _RR_TRACEW$ |
    _RR_TENSOR$ | _RR_TENSOR_B0$ | _RR_SBRef$ |
    _RR_PhysioLog$
)""", re.IGNORECASE | re.VERBOSE)


def always_ignore(desc):
    return bool(ALWAYS_IGNORE_RE.search(desc))


# ============================================================================
# RERUN DETECTION
# ============================================================================

RERUN_RE        = re.compile(r"[_\s]+(rr|rerun|repeat|redo|2nd)($|(?=_))", re.IGNORECASE)
RERUN_PREFIX_RE = re.compile(r"^Repeat_", re.IGNORECASE)


def is_rerun(desc):
    return bool(RERUN_RE.search(desc)) or bool(RERUN_PREFIX_RE.match(desc))


def strip_rerun(desc):
    s = RERUN_PREFIX_RE.sub("", desc)
    s = RERUN_RE.sub("", s)
    return s.strip("_ ")


# ============================================================================
# TEMPLATE LOADING
# ============================================================================

def load_templates(templates_dir, site):
    tsv_files = sorted(templates_dir.glob("*.tsv"))
    if not tsv_files:
        print(f"ERROR: No template TSV files found in {templates_dir}", file=sys.stderr)
        sys.exit(1)

    proto_rows = {}
    for tsv_path in tsv_files:
        with tsv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            required = {"site", "protocol_name", "fingerprint",
                        "series_description", "bids_folder", "bids_suffix", "expected"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                print(f"  WARNING: {tsv_path.name} missing columns {sorted(missing)} — skipped",
                      file=sys.stderr)
                continue
            for row in reader:
                if row["site"].strip().lower() != site.lower():
                    continue
                proto = row["protocol_name"].strip()
                if proto not in proto_rows:
                    proto_rows[proto] = []
                proto_rows[proto].append(row)

    if not proto_rows:
        print(f"ERROR: No templates found for site '{site}' in {templates_dir}", file=sys.stderr)
        sys.exit(1)

    templates = []
    for proto_name, rows in proto_rows.items():
        fingerprint, mapping, folder, expected = set(), {}, {}, set()
        for row in rows:
            desc    = row["series_description"].strip()
            desc_cf = desc.casefold()
            bfolder = row["bids_folder"].strip()
            bsuffix = row["bids_suffix"].strip()
            if row["fingerprint"].strip().lower() == "yes":
                fingerprint.add(desc_cf)
            if row["expected"].strip().lower() == "yes":
                expected.add(desc)
            mapping[desc_cf] = bsuffix
            folder[desc_cf]  = bfolder
        templates.append({
            "name": proto_name, "site": site,
            "fingerprint": fingerprint, "mapping": mapping,
            "folder": folder, "expected": expected,
        })

    print(f"Loaded {len(templates)} template(s) for site '{site}':")
    for t in templates:
        print(f"  {t['name']}  "
              f"({len(t['mapping'])} series, "
              f"{len(t['fingerprint'])} fingerprint, "
              f"{len(t['expected'])} expected)")
    return templates


# ============================================================================
# PROTOCOL DETECTION
# ============================================================================

def detect_protocol(series_list, templates):
    series_cf = {s.casefold() for s in series_list}
    best_score, best_tmpl = -1.0, None
    for tmpl in templates:
        fp = tmpl["fingerprint"]
        if not fp:
            continue
        score = len(fp & series_cf) / len(fp)
        if score > best_score or (
            score == best_score and best_tmpl and
            len(fp) > len(best_tmpl["fingerprint"])
        ):
            best_score, best_tmpl = score, tmpl
    return best_tmpl, best_score


# ============================================================================
# SERIES RESOLUTION
# ============================================================================

def resolve_series(series_list, template):
    mapping_cf = template["mapping"]
    folder_cf  = template["folder"]
    desc_cf    = {d.casefold(): d for d in series_list}

    # Detect duplicate series names — all but the last occurrence are superseded.
    # This handles cases where a sequence was aborted and rerun with the same name.
    from collections import Counter
    desc_counts = Counter(d.casefold() for d in series_list)
    desc_seen   = Counter()

    superseded_implicit = set()  # indices of non-last duplicates
    for i, desc in enumerate(series_list):
        cf = desc.casefold()
        desc_seen[cf] += 1
        if desc_counts[cf] > 1 and desc_seen[cf] < desc_counts[cf]:
            superseded_implicit.add(i)

    superseded_cf = set()
    rerun_base_cf = {}
    for desc in series_list:
        if is_rerun(desc):
            base_cf = strip_rerun(desc).casefold()
            rerun_base_cf[desc.casefold()] = base_cf
            if base_cf in desc_cf:
                superseded_cf.add(base_cf)

    records = []
    for i, desc in enumerate(series_list):
        cf = desc.casefold()
        # Always-ignore is overridden if the sequence is explicitly mapped in the template
        if always_ignore(desc) and cf not in mapping_cf:
            records.append(_rec(desc, "ignore", "IGNORE", "IGNORE", False, "always-ignore list"))
            continue
        if i in superseded_implicit:
            records.append(_rec(desc, "superseded", "IGNORE", "IGNORE", False, "superseded by repeat acquisition"))
            continue
        if cf in superseded_cf:
            records.append(_rec(desc, "superseded", "IGNORE", "IGNORE", False, "superseded by rerun"))
            continue
        is_rr   = cf in rerun_base_cf
        lookup  = rerun_base_cf[cf] if is_rr else cf
        bsuffix = mapping_cf.get(cf) or mapping_cf.get(lookup)
        bfolder = folder_cf.get(cf)  or folder_cf.get(lookup)
        if bsuffix is None:
            records.append(_rec(desc, "unknown", "UNKNOWN", "UNKNOWN", is_rr, "not in template"))
        elif bsuffix.upper() == "IGNORE":
            records.append(_rec(desc, "ignore", "IGNORE", "IGNORE", is_rr, "template IGNORE"))
        else:
            records.append(_rec(desc, "keep", bfolder, bsuffix, is_rr, "rerun" if is_rr else ""))
    return records


def _rec(desc, status, folder, bids_suffix, is_rerun_, note):
    return {"desc": desc, "status": status, "folder": folder,
            "bids_suffix": bids_suffix, "is_rerun": is_rerun_,
            "note": note, "run": None}


def assign_run_numbers(records):
    groups = defaultdict(list)
    for i, rec in enumerate(records):
        if rec["status"] == "keep":
            groups[rec["bids_suffix"]].append(i)
    for indices in groups.values():
        if len(indices) > 1:
            for run_n, idx in enumerate(indices, start=1):
                records[idx]["run"] = run_n
    return records


# ============================================================================
# BIDS KEY
# ============================================================================

def bids_key(subject, session, folder, suffix):
    if folder in ("IGNORE", "UNKNOWN"):
        return folder
    # Use heudiconv placeholders instead of hardcoded values
    # so heudiconv can correctly template the output paths
    sub       = "sub-{subject}"
    ses_path  = "/{session}" if session else ""
    ses_label = "_{session}" if session else ""
    return f"{sub}{ses_path}/{folder}/{sub}{ses_label}_{suffix}"


# ============================================================================
# FILE GENERATION
# ============================================================================

def generate_mapping_tsv(subject, session, records):
    lines = ["match_type\tmatch_value\tbids_key"]
    for rec in records:
        desc, status = rec["desc"], rec["status"]
        if status == "superseded":
            lines.append(f"exact\t{desc}\tIGNORE\t# SUPERSEDED by rerun")
        elif status == "ignore":
            lines.append(f"exact\t{desc}\tIGNORE")
        elif status == "unknown":
            lines.append(f"exact\t{desc}\t# UNKNOWN — manual review needed")
        else:
            suffix = rec["bids_suffix"]
            if rec["run"] is not None:
                suffix = f"run-{rec['run']:02d}_{suffix}"
            lines.append(f"exact\t{desc}\t{bids_key(subject, session, rec['folder'], suffix)}")
    return "\n".join(lines) + "\n"


def generate_bids_sh(subject, session, mapping_abs,
                     dicom_template, bids_output, heuristic_path):
    ses_line = f" \\\n -ss {session}" if session else ""
    heudiconv_cmd = (
        "heudiconv \\\n"
        f" --dicom_dir_template {dicom_template} \\\n"
        f" -o {bids_output} \\\n"
        f" -f {heuristic_path} \\\n"
        " -c dcm2niix \\\n"
        " -b \\\n"
        " --minmeta \\\n"
        " --overwrite \\\n"
        f" -s {subject}{ses_line}"
    )
    return (
        "#!/bin/bash\n\n"
        f"export HEUDICONV_MAPPING_TSV={mapping_abs}\n\n"
        "# Run heudiconv with IntendedFor auto-population enabled\n"
        f"{heudiconv_cmd}\n\n"
        "if [ $? -ne 0 ]; then\n"
        f'    echo "[RETRY] sub-{subject} failed — retrying with IntendedFor disabled"\n'
        "    export HEUDICONV_DISABLE_INTENDED_FOR=1\n"
        f"    {heudiconv_cmd}\n"
        "fi\n"
    )


def generate_series_resolved_tsv(records):
    lines = ["series_description\tstatus\tfolder\tbids_suffix\trun\tis_rerun\tnote"]
    for rec in records:
        lines.append("\t".join([
            rec["desc"], rec["status"],
            rec.get("folder") or "", rec.get("bids_suffix") or "",
            str(rec["run"]) if rec["run"] is not None else "",
            str(rec["is_rerun"]), rec.get("note") or "",
        ]))
    return "\n".join(lines) + "\n"


def generate_protocol_match_txt(subject, session, tmpl, score, series_list):
    series_cf = {s.casefold() for s in series_list}
    lines = [
        f"Subject   : sub-{subject}",
        f"Session   : ses-{session}",
        f"Template  : {tmpl['name']}",
        f"Confidence: {score:.0%}",
        "", "Fingerprint sequences:",
    ]
    for f in sorted(tmpl["fingerprint"]):
        lines.append(f"  {'V' if f in series_cf else 'X MISSING'}  {f}")
    if tmpl["expected"]:
        lines += ["", "Expected sequences (completeness check):"]
        for e in sorted(tmpl["expected"]):
            lines.append(f"  {'V' if e.casefold() in series_cf else 'X MISSING'}  {e}")
    return "\n".join(lines) + "\n"


# ============================================================================
# LOG PARSING
# ============================================================================

def parse_log(log_path):
    return [ln.strip()
            for ln in log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            if ln.strip()]


def parse_filename(log_path):
    m = re.search(r"sub-([A-Za-z0-9]+)_ses-([A-Za-z0-9]+)", log_path.stem, re.IGNORECASE)
    if m:
        return m.group(1), m.group(2)
    m = re.search(r"sub-(\w+)", log_path.stem, re.IGNORECASE)
    return (m.group(1), None) if m else (None, None)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="Template-based heudiconv config generator.")
    parser.add_argument("--logs_dir",       required=True)
    parser.add_argument("--output_dir",     required=True)
    parser.add_argument("--site",           required=True)
    parser.add_argument("--subject",        default=None, help="Process a single subject ID (e.g. CC001). Omit for batch processing.")
    parser.add_argument("--session",        default=None, help="Process a single session (e.g. 001). Optional, used with --subject.")
    parser.add_argument("--templates_dir",
                        default=str(Path(__file__).resolve().parent / "templates"))
    parser.add_argument("--heuristic",      required=True, help="Path to heuristic.py")
    parser.add_argument("--dicom_template", required=True, help="DICOM dir template with {subject} and {session} placeholders")
    parser.add_argument("--bids_output",    required=True, help="BIDS output directory")
    args = parser.parse_args()

    logs_dir      = Path(args.logs_dir)
    output_dir    = Path(args.output_dir)
    templates_dir = Path(args.templates_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'─'*60}")
    print(f"Site         : {args.site}")
    print(f"Templates dir: {templates_dir}")
    templates = load_templates(templates_dir, args.site)

    # Build file list — single subject or full batch
    if args.subject:
        pattern = f"*sub-{args.subject}*"
        if args.session:
            pattern = f"*sub-{args.subject}*ses-{args.session}*"
        log_files = sorted(logs_dir.glob(pattern))
        if not log_files:
            print(f"ERROR: No log file found for subject {args.subject} in {logs_dir}", file=sys.stderr)
            sys.exit(1)
    else:
        log_files = sorted(logs_dir.glob("*sub-*.txt"))
        if not log_files:
            log_files = sorted(logs_dir.glob("*sub-*.log"))
        if not log_files:
            print(f"ERROR: No log files found in {logs_dir}", file=sys.stderr)
            sys.exit(1)

    print(f"Log files    : {len(log_files)}")
    print(f"Output dir   : {output_dir}")
    print(f"{'─'*60}\n")

    summary_rows, detection_rows, incomplete_rows = [], [], []

    n_skipped_site = 0
    for log_path in log_files:
        subject, session = parse_filename(log_path)
        if subject is None:
            print(f"  SKIP (cannot parse subject): {log_path.name}")
            continue

        # Filter by site — only process subjects whose ID contains the site label
        if args.site.lower() not in subject.lower():
            n_skipped_site += 1
            continue

        series_list = parse_log(log_path)
        tag = f"sub-{subject}_ses-{session}" if session else f"sub-{subject}"

        if not series_list:
            print(f"  SKIP (empty log): {tag}")
            continue

        tmpl, score = detect_protocol(series_list, templates)

        if tmpl is None:
            print(f"  [{tag}]  NO TEMPLATE MATCH")
            detection_rows.append({"tag": tag, "subject": subject,
                                    "session": session or "", "template": "NO MATCH",
                                    "confidence": "0%", "flag": "no template match"})
            continue

        conf_pct = f"{score:.0%}"
        flag     = "LOW CONFIDENCE" if score < 1.0 else ""
        detection_rows.append({"tag": tag, "subject": subject, "session": session or "",
                                "template": tmpl["name"], "confidence": conf_pct, "flag": flag})

        records = resolve_series(series_list, tmpl)
        records = assign_run_numbers(records)

        series_cf = {s.casefold() for s in series_list}
        missing = [e for e in sorted(tmpl["expected"]) if e.casefold() not in series_cf]
        for m in missing:
            incomplete_rows.append({"tag": tag, "subject": subject,
                                     "session": session or "", "missing_sequence": m})

        sub_out = output_dir / tag
        sub_out.mkdir(exist_ok=True)

        mapping_path = sub_out / f"{tag}_mapping.tsv"
        mapping_path.write_text(generate_mapping_tsv(subject, session or "", records), encoding="utf-8")

        sh_path = sub_out / f"{tag}_BIDS.sh"
        sh_path.write_text(
            generate_bids_sh(subject, session or "", str(mapping_path.resolve()),
                             args.dicom_template, args.bids_output, args.heuristic),
            encoding="utf-8")
        sh_path.chmod(0o755)

        (sub_out / f"{tag}_series_resolved.tsv").write_text(
            generate_series_resolved_tsv(records), encoding="utf-8")
        (sub_out / f"{tag}_protocol_match.txt").write_text(
            generate_protocol_match_txt(subject, session or "", tmpl, score, series_list),
            encoding="utf-8")

        for rec in records:
            suffix = rec.get("bids_suffix") or ""
            if rec["run"] is not None and suffix not in ("", "IGNORE", "UNKNOWN"):
                suffix = f"run-{rec['run']:02d}_{suffix}"
            summary_rows.append({
                "subject": subject, "session": session or "",
                "series_description": rec["desc"], "status": rec["status"],
                "folder": rec.get("folder") or "",
                "bids_key": bids_key(subject, session or "", rec.get("folder") or "", suffix),
                "is_rerun": rec.get("is_rerun", False), "note": rec.get("note") or "",
            })

        n_unk = sum(1 for r in records if r["status"] == "unknown")
        n_sup = sum(1 for r in records if r["status"] == "superseded")
        status_str = f"conf={conf_pct}"
        if flag:    status_str += f"  ! {flag}"
        if missing: status_str += f"  missing={len(missing)}"
        if n_unk:   status_str += f"  unknown={n_unk}"
        if n_sup:   status_str += f"  superseded={n_sup}"
        print(f"  [{tag}]  {status_str}")

    sf = ["subject", "session", "series_description", "status", "folder", "bids_key", "is_rerun", "note"]

    def write_tsv(path, fields, rows):
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
            w.writeheader()
            w.writerows(rows)

    write_tsv(output_dir / "00_summary_all.tsv",         sf, summary_rows)
    write_tsv(output_dir / "01_flagged_unknowns.tsv",    sf, [r for r in summary_rows if r["status"] == "unknown"])
    write_tsv(output_dir / "02_superseded_by_rerun.tsv", sf, [r for r in summary_rows if r["status"] == "superseded"])
    write_tsv(output_dir / "03_incomplete_sessions.tsv",
              ["tag", "subject", "session", "missing_sequence"], incomplete_rows)
    write_tsv(output_dir / "04_protocol_detection.tsv",
              ["tag", "subject", "session", "template", "confidence", "flag"], detection_rows)

    n_inc = len(set(r["tag"] for r in incomplete_rows))
    print(f"""
{'─'*60}
Output directory   : {output_dir}
Site               : {args.site}
Log files found    : {len(log_files)}
Skipped (wrong site): {n_skipped_site}
Subjects processed : {len(detection_rows)}
Total series       : {len(summary_rows)}
  kept             : {sum(1 for r in summary_rows if r['status'] == 'keep')}
  ignored          : {sum(1 for r in summary_rows if r['status'] == 'ignore')}
  superseded       : {sum(1 for r in summary_rows if r['status'] == 'superseded')}
  unknown          : {sum(1 for r in summary_rows if r['status'] == 'unknown')}
Incomplete sessions: {n_inc}

Per-subject files (in each sub-*/ses-* folder)
  <tag>_mapping.tsv          heudiconv mapping input
  <tag>_BIDS.sh              ready-to-run heudiconv command (with IntendedFor retry)
  <tag>_series_resolved.tsv  every series + resolved status  [troubleshooting]
  <tag>_protocol_match.txt   template detection details       [troubleshooting]

Site-level review files
  00_summary_all.tsv         full series x subject table
  01_flagged_unknowns.tsv    series needing manual mapping
  02_superseded_by_rerun.tsv rerun superseding log
  03_incomplete_sessions.tsv subjects missing expected sequences
  04_protocol_detection.tsv  template match + confidence per subject
{'─'*60}
""")


if __name__ == "__main__":
    main()