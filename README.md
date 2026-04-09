# bids-prep

Shared neuroimaging utilities for DICOM inspection and BIDS conversion.

## Structure

```
bids-prep/
├── bids/
│   ├── heuristic.py               # HeuDiConv heuristic — reads per-subject mapping.tsv
│   ├── generate_bids_configs.py   # Generates per-subject mapping.tsv + BIDS.sh from a protocol template
│   └── templates/                 # Protocol TSV templates (one per site/protocol)
└── dicom/
    ├── query_series.py            # List unique SeriesDescriptions from a DICOM directory
    ├── query_study_dates.py       # List study dates per subject from a DICOM directory
    ├── run_parallel.py            # Run a file of shell commands in parallel
    └── dcm2dir                    # Sort messy DICOMs into a structured directory
```

## Usage

### query_series.py
```bash
python dicom/query_series.py \
    --input-dir /path/to/dicom/sub-001 \
    --output-txt sub-001_series.txt
```

### query_study_dates.py
```bash
python dicom/query_study_dates.py \
    --input-dir /path/to/dicom/root \
    --output-txt study_dates.txt
```

### run_parallel.py
```bash
python dicom/run_parallel.py \
    --script-file commands.sh \
    --max-workers 8
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

## Adding to a project as a Git submodule

```bash
git submodule add https://github.com/you/bids-prep code/bids-prep
```
