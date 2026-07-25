#!/usr/bin/env python3
"""
run_pipeline.py
===============
Config-driven step runner for the bids-prep pipeline. Wraps the existing
per-stage scripts as subprocesses so a subject/site's parameters only have
to be written down once (in a config file) instead of retyped on the
command line for every stage.

This does NOT replace or rename any existing script — dicom/query_series.py,
bids/generate_bids_configs.py, etc. remain valid standalone entry points
(important since this repo is consumed as a git submodule by other repos
that may call them directly). The step numbers below are a logical registry
defined only in this file.

Pipeline steps
--------------
    1  dcm2dir                   dicom/dcm2dir                    single subject/session only
    2  query_series              dicom/query_series.py            single-dir or --batch
    3  concat_series_tsvs        dicom/concat_series_tsvs.py       whole-cohort (no --subject)
    4  core_protocol_discovery   dicom/core_protocol_discovery.py  whole-cohort (no --subject)
    5  manual_review             (no script -- prints where to edit candidate templates)
    6  generate_bids_configs     bids/generate_bids_configs.py     single-subject or batch, incremental

Usage
-----
    python run_pipeline.py --config configs/example.yaml --steps 6
    python run_pipeline.py --config configs/example.yaml --steps "1 2 3"
    python run_pipeline.py --config configs/example.yaml --steps 1-4 --dry-run
    python run_pipeline.py --config configs/example.yaml --steps 6 --subject CC001 --session 001
    python run_pipeline.py --list-steps

Config file
-----------
YAML (preferred, requires `pip install pyyaml`) or JSON (zero-dependency
fallback), selected by file extension. See configs/example.yaml /
configs/example.json for the full schema. Top-level `site` is inherited by
every step; each step also has its own `steps.<slug>.*` block of parameters
matching that script's flags (with dashes replaced by underscores).

Passthrough
-----------
Any flag not modeled in the config schema can be forwarded verbatim to a
single selected step's underlying script:

    python run_pipeline.py --config configs/example.yaml --steps 6 -- --dcmconfig utils/dcmconfig_bids_anon.json

(Only valid when exactly one step is selected -- otherwise it's ambiguous
which script the extra args belong to.)
"""

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent


# ============================================================================
# STEP REGISTRY
# ============================================================================

def _script(*parts):
    return REPO_ROOT.joinpath(*parts)


STEPS = {
    1: {"slug": "dcm2dir", "script": _script("dicom", "dcm2dir"), "scope": "subject"},
    2: {"slug": "query_series", "script": _script("dicom", "query_series.py"), "scope": "flexible"},
    3: {"slug": "concat_series_tsvs", "script": _script("dicom", "concat_series_tsvs.py"), "scope": "cohort"},
    4: {"slug": "core_protocol_discovery", "script": _script("dicom", "core_protocol_discovery.py"), "scope": "cohort"},
    5: {"slug": "manual_review", "script": None, "scope": "manual"},
    6: {"slug": "generate_bids_configs", "script": _script("bids", "generate_bids_configs.py"), "scope": "flexible"},
}

SLUG_TO_NUMBER = {v["slug"]: k for k, v in STEPS.items()}


def print_registry():
    print("Pipeline steps:")
    for n in sorted(STEPS):
        entry = STEPS[n]
        script = entry["script"].relative_to(REPO_ROOT) if entry["script"] else "(no script -- manual review)"
        print(f"  {n}  {entry['slug']:<24s} {script}   [{entry['scope']}]")


# ============================================================================
# CONFIG LOADING
# ============================================================================

def load_config(path):
    path = Path(path)
    if not path.exists():
        print(f"ERROR: config file not found: {path}", file=sys.stderr)
        sys.exit(1)
    if path.suffix in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            print("ERROR: PyYAML is not installed. Either `pip install pyyaml` "
                  "(setup_env.sh does this for you) or write your config as .json instead.",
                  file=sys.stderr)
            sys.exit(1)
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if path.suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    print(f"ERROR: unsupported config extension {path.suffix} (use .yaml/.yml/.json)", file=sys.stderr)
    sys.exit(1)


def step_cfg(config, slug):
    """Step-specific config, with top-level `site` inherited as a default."""
    cfg = dict(config.get("steps", {}).get(slug) or {})
    cfg.setdefault("site", config.get("site"))
    return cfg


def require(cfg, key, slug):
    if not cfg.get(key):
        print(f"ERROR: config.steps.{slug}.{key} is required but missing/empty", file=sys.stderr)
        sys.exit(1)
    return cfg[key]


# ============================================================================
# PER-STEP ARGV BUILDERS
# Each builder maps config fields -> the underlying script's actual CLI flags.
# ============================================================================

def args_dcm2dir(cfg, subject, session):
    argv = ["--input-dir", require(cfg, "input_dir", "dcm2dir"),
            "--output-dir", require(cfg, "output_dir", "dcm2dir")]
    if subject:
        argv += ["--subject-id", subject]
    if session:
        argv += ["--session-id", session]
    if cfg.get("manifest"):
        argv += ["--manifest", str(cfg["manifest"])]
    if cfg.get("mode"):
        argv += ["--mode", cfg["mode"]]
    if cfg.get("force_decompress"):
        argv.append("--force-decompress")
    if cfg.get("skip_md5"):
        argv.append("--skip-md5")
    if cfg.get("verbose"):
        argv.append("--verbose")
    return argv


def args_query_series(cfg, subject, session):
    argv = ["--input-dir", require(cfg, "input_dir", "query_series"),
            "--output-dir", require(cfg, "output_dir", "query_series")]
    if cfg.get("batch"):
        argv.append("--batch")
    if subject:
        argv += ["--subject", subject]
    if session:
        argv += ["--session", session]
    for sf in cfg.get("session_filter") or []:
        argv += ["--session-filter", sf]
    if cfg.get("check_instances"):
        argv.append("--check-instances")
    if cfg.get("force"):
        argv.append("--force")
    if cfg.get("verbose"):
        argv.append("--verbose")
    return argv


def args_concat_series_tsvs(cfg):
    argv = ["--input-dir", require(cfg, "input_dir", "concat_series_tsvs"),
            "--output-tsv", require(cfg, "output_tsv", "concat_series_tsvs")]
    if cfg.get("sort"):
        argv.append("--sort")
    return argv


def args_core_protocol_discovery(cfg):
    argv = ["--input", require(cfg, "input", "core_protocol_discovery"),
            "--output-dir", require(cfg, "output_dir", "core_protocol_discovery")]
    if cfg.get("site"):
        argv += ["--site", cfg["site"]]
    years = cfg.get("year") or cfg.get("years")
    if years:
        argv += ["--year", *[str(y) for y in years]]
    if cfg.get("core_threshold") is not None:
        argv += ["--core-threshold", str(cfg["core_threshold"])]
    if cfg.get("protocol_name"):
        argv += ["--protocol-name", cfg["protocol_name"]]
    return argv


def args_generate_bids_configs(cfg, subject, session):
    argv = ["--logs_dir", require(cfg, "logs_dir", "generate_bids_configs"),
            "--output_dir", require(cfg, "output_dir", "generate_bids_configs"),
            "--site", require(cfg, "site", "generate_bids_configs")]
    if subject:
        argv += ["--subject", subject]
    if session:
        argv += ["--session", session]
    for key, flag in [("templates_dir", "--templates_dir"), ("heuristic", "--heuristic"),
                       ("dicom_template", "--dicom_template"), ("bids_output", "--bids_output"),
                       ("dcmconfig", "--dcmconfig"), ("queue_file", "--queue-file")]:
        if cfg.get(key):
            argv += [flag, str(cfg[key])]
    if cfg.get("force"):
        argv.append("--force")
    return argv


def run_manual_review(cfg):
    templates_dir = cfg.get("templates_dir", "(not set in config)")
    print(f"""
[manual_review]  This step has no script -- it's a human checkpoint.

  Fill in bids_folder, bids_suffix, fingerprint, expected, is_rerun, and
  rerun_of for every row in the candidate template TSVs at:

      {templates_dir}

  Once reviewed, continue with step 6 (generate_bids_configs), which reads
  the completed templates directly from that directory.
""")


# ============================================================================
# STEP PARSING ("3" / "1 2 3" / "1-4" / "1,3,5-6")
# ============================================================================

def parse_steps(spec):
    numbers = set()
    for token in spec.replace(",", " ").split():
        if "-" in token:
            lo, hi = token.split("-", 1)
            numbers.update(range(int(lo), int(hi) + 1))
        else:
            numbers.add(int(token))
    invalid = numbers - set(STEPS)
    if invalid:
        print(f"ERROR: unknown step number(s): {sorted(invalid)}. "
              f"Valid steps: {sorted(STEPS)}", file=sys.stderr)
        sys.exit(1)
    return sorted(numbers)


# ============================================================================
# MAIN
# ============================================================================

def main():
    argv = sys.argv[1:]
    if "--" in argv:
        idx = argv.index("--")
        own_argv, extra_args = argv[:idx], argv[idx + 1:]
    else:
        own_argv, extra_args = argv, []

    parser = argparse.ArgumentParser(
        description="Config-driven step runner for the bids-prep pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", help="Path to a .yaml/.yml/.json pipeline config")
    parser.add_argument("--steps", help='Step(s) to run, e.g. "6", "1 2 3", "1-4", "1,3,5-6"')
    parser.add_argument("--subject", default=None, help="Subject ID, forwarded to steps 1/2/6")
    parser.add_argument("--session", default=None, help="Session ID, forwarded to steps 1/2/6")
    parser.add_argument("--dry-run", action="store_true", help="Print resolved commands without running them")
    parser.add_argument("--keep-going", action="store_true", help="Continue to the next step if one fails")
    parser.add_argument("--list-steps", action="store_true", help="Print the step registry and exit")
    args = parser.parse_args(own_argv)

    if args.list_steps:
        print_registry()
        return

    if not args.config or not args.steps:
        parser.error("--config and --steps are required (unless using --list-steps)")

    selected = parse_steps(args.steps)

    if extra_args and len(selected) != 1:
        print("ERROR: passthrough args after `--` are only allowed when exactly one step is selected "
              "(otherwise it's ambiguous which script they belong to).", file=sys.stderr)
        sys.exit(1)

    config = load_config(args.config)

    for n in selected:
        entry = STEPS[n]
        slug = entry["slug"]

        if slug in ("concat_series_tsvs", "core_protocol_discovery") and (args.subject or args.session):
            print(f"[{n}:{slug}] NOTE: this step is whole-cohort by design -- "
                  f"--subject/--session are ignored here.")

        if slug == "manual_review":
            run_manual_review(step_cfg(config, slug))
            continue

        cfg = step_cfg(config, slug)
        if slug == "dcm2dir":
            if not args.subject:
                print("ERROR: step 1 (dcm2dir) processes one subject/session per invocation and needs "
                      "--subject (and usually --session). To fan out over many subjects, generate a "
                      "list of dcm2dir commands and run them with utils/run_parallel.py instead.",
                      file=sys.stderr)
                sys.exit(1)
            step_args = args_dcm2dir(cfg, args.subject, args.session)
        elif slug == "query_series":
            step_args = args_query_series(cfg, args.subject, args.session)
        elif slug == "concat_series_tsvs":
            step_args = args_concat_series_tsvs(cfg)
        elif slug == "core_protocol_discovery":
            step_args = args_core_protocol_discovery(cfg)
        elif slug == "generate_bids_configs":
            step_args = args_generate_bids_configs(cfg, args.subject, args.session)
        else:
            raise AssertionError(f"unhandled step slug: {slug}")

        cmd = [sys.executable, str(entry["script"]), *step_args, *extra_args]
        print(f"\n[{n}:{slug}] $ {' '.join(shlex.quote(c) for c in cmd)}")

        if args.dry_run:
            continue

        result = subprocess.run(cmd)
        if result.returncode != 0:
            print(f"[{n}:{slug}] FAILED (exit {result.returncode})", file=sys.stderr)
            if not args.keep_going:
                sys.exit(result.returncode)


if __name__ == "__main__":
    main()
