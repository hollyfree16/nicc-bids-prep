#!/usr/bin/env python3
"""
Minimal heudiconv --anon-cmd script.
Copies a DICOM file after blanking all date (DA), datetime (DT),
and time (TM) VR fields so heudiconv's BIDS date check passes.

Usage (called automatically by heudiconv):
    python strip_dates.py <input_dicom> <output_dicom>
"""
import sys
import shutil
import pydicom

src, dst = sys.argv[1], sys.argv[2]

ds = pydicom.dcmread(src, force=True)
for tag in list(ds.keys()):
    try:
        if ds[tag].VR in ('DA', 'DT', 'TM'):
            ds[tag].value = ''
    except Exception:
        pass
ds.save_as(dst)
