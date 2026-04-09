from __future__ import annotations

from pathlib import Path
import csv
import re
import os

HERE = Path(__file__).resolve().parent

_mapping_env = os.environ.get("HEUDICONV_MAPPING_TSV")
MAPPING_TSV = Path(_mapping_env) if _mapping_env else HERE / "mapping.tsv"


POPULATE_INTENDED_FOR_OPTS = {
    "matching_parameters": ["ImagingVolume"],
    "criterion": "Closest",
}

_ALLOWED = {
    "ImagingVolume",
    "ModalityAcquisitionLabel",
    "CustomAcquisitionLabel",
    "PlainAcquisitionLabel",
    "Shims",
    "Force",
}

bad = [p for p in POPULATE_INTENDED_FOR_OPTS["matching_parameters"] if p not in _ALLOWED]
if bad:
    raise RuntimeError(
        f"Invalid matching_parameters: {bad}. Allowed: {sorted(_ALLOWED)}"
    )


def create_key(template, outtype=("nii.gz",), annotation_classes=None):
    if not template:
        raise ValueError("Template must be a valid format string")
    return template, outtype, annotation_classes


def _load_mapping(tsv_path: Path):
    rules = []
    with tsv_path.open("r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        required = {"match_type", "match_value", "bids_key"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise RuntimeError(f"mapping.tsv missing columns: {sorted(missing)}")

        for row in reader:
            match_type  = row["match_type"].strip().lower()
            match_value = row["match_value"].strip()

            # The bids_key column may have a trailing tab-comment added by the
            # generator (e.g. "IGNORE\t# SUPERSEDED by rerun").  Strip it off.
            bids_key = row["bids_key"].split("\t")[0].strip()

            # Skip IGNORE rows — heudiconv should not convert these at all.
            # Also skip comment-only rows (bids_key starts with #).
            if bids_key.upper() == "IGNORE" or bids_key.startswith("#"):
                continue

            if match_type not in {"exact", "regex"}:
                raise RuntimeError(f"Unknown match_type={match_type} for row={row}")

            key = create_key(bids_key)

            rule = {"match_type": match_type, "key": key}

            if match_type == "exact":
                rule["match_value"] = match_value.casefold()
            else:
                rule["regex"] = re.compile(match_value, re.IGNORECASE)

            rules.append(rule)

    return rules


_RULES = _load_mapping(MAPPING_TSV)


def infotodict(seqinfo):
    info = {}

    for s in seqinfo:
        sd = (s.series_description or "").strip().casefold()

        for rule in _RULES:
            if rule["match_type"] == "exact":
                if sd == rule["match_value"]:
                    info.setdefault(rule["key"], []).append(s.series_id)
                    break
            else:  # regex
                if rule["regex"].search(sd):
                    info.setdefault(rule["key"], []).append(s.series_id)
                    break

    return info