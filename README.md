# bids-prep

Shared neuroimaging utilities for DICOM inspection and BIDS conversion. See
`CLAUDE.md` for the full pipeline walkthrough, template format, and script
behaviors.

## Structure

```
bids-prep/
├── run_pipeline.py                 # Config-driven step runner for the whole pipeline (see below)
├── configs/
│   └── example.yaml / example.json # Example run_pipeline.py config — copy per site
├── bids/
│   ├── heuristic.py                # HeuDiConv heuristic — reads per-subject mapping.tsv
│   ├── generate_bids_configs.py    # Generates per-subject mapping.tsv + BIDS.sh from a protocol template
│   └── templates/                  # Protocol TSV templates (one per site/protocol)
├── dicom/
│   ├── dcm2dir                     # Sort messy DICOMs into a structured directory
│   ├── query_series.py             # Per-subject/session DICOM series TSV inventory
│   ├── concat_series_tsvs.py       # Merge per-subject series TSVs into one cohort TSV
│   └── core_protocol_discovery.py  # Protocol triage + year-specific candidate templates
└── utils/
    ├── run_parallel.py             # Run a file of shell commands in parallel
    ├── run_subject.py              # Run+log a single generated _BIDS.sh
    └── strip_dates.py              # Strip dates from BIDS sidecar JSON
```

## Usage

### run_pipeline.py (recommended entry point)
Wraps the scripts below as subprocesses driven by a config file, so
per-site parameters only need to be written once. See `CLAUDE.md` →
"Pipeline Orchestration" for the full step registry and config schema.

```bash
python run_pipeline.py --config configs/MGH.yaml --steps 6
python run_pipeline.py --config configs/MGH.yaml --steps "1 2 3"
python run_pipeline.py --list-steps
```

### query_series.py
```bash
python dicom/query_series.py \
    --input-dir /path/to/dicom/sub-001/ses-001 \
    --output-dir /path/to/series/logs
```

### concat_series_tsvs.py
```bash
python dicom/concat_series_tsvs.py \
    --input-dir /path/to/series/logs \
    --output-tsv all_ses-001_series.tsv \
    --sort
```

### core_protocol_discovery.py
```bash
python dicom/core_protocol_discovery.py \
    --input all_ses-001_series.tsv \
    --output-dir review_out \
    --site MYSITE
```

### generate_bids_configs.py
```bash
python bids/generate_bids_configs.py \
    --logs_dir       /path/to/series/logs \
    --output_dir     /path/to/output \
    --site           SiteA \
    --templates_dir  bids/templates \
    --heuristic      bids/heuristic.py \
    --dicom_template /path/to/raw/mri/sub-{subject}/ses-{session}/*/*.dcm \
    --bids_output    /path/to/BIDS/
```
Batch mode is incremental: subjects whose `<tag>_mapping.tsv` is already up
to date are skipped. Pass `--force`/`--reprocess` to regenerate everyone.
Every run also writes/updates `<output_dir>/bids_queue.sh` — a `bash
<tag>_BIDS.sh` line for each subject that still needs converting (no BIDS
data yet, or config changed this run) — ready to feed to
`utils/run_parallel.py --script-file` or run directly with `bash`.

### utils/run_parallel.py
```bash
python utils/run_parallel.py \
    --script-file commands.sh \
    --max-workers 8
```

## Adding to a project as a Git submodule

```bash
git submodule add https://github.com/you/bids-prep code/bids-prep
```
