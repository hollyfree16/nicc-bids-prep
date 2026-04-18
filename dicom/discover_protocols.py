#!/usr/bin/env python3
"""
discover_protocols.py
=====================
Reads a directory of query_series log files and groups subjects by unique
sequence sets, giving you a quick overview of how many protocol variants
exist before building templates.

Usage:
    python discover_protocols.py --logs-dir /path/to/logs/query_series/SITE

Output:
    - Console summary of protocol groups
    - Optional TSV file with per-subject protocol group assignment

Example output:
    Protocol Group 1 (n=189):
      Sequences: 3D-FLAIR 1mmISO | DTI64_b1000_MB2_BlipA | DTI64_b1000_MB2_BlipP | ...
      Subjects:  sub-UWp201, sub-UWp203, ...

    Protocol Group 2 (n=60):
      Sequences: 3D-FLAIR 1mmISO | B0_ME | DTI32_b1000_BlipA | ...
      Subjects:  sub-UWp103, sub-UWp104, ...
"""

import os
import glob
import argparse
from collections import defaultdict


ALWAYS_IGNORE = {
    'localizer', 'localizer_aligned', 'aahscout', 'aahscout_mpr_cor',
    'aahscout_mpr_sag', 'aahscout_mpr_tra', 'phoenixzipreport', 'survey_32ch_headcoil',
    'survey_shc', 'mocoseries', 'relcbf', 'perfusion_weighted', 'posdisp',
    'design', 'evaseries_glm', 'mean_&_t-maps', 'senserefscan', '3pl loc 24',
    'wip source - 3d_pcasl_5mm_4pulses_foldoverrl',
}

ALWAYS_IGNORE_SUFFIXES = (
    '_adc', '_fa', '_colfa', '_tracew', '_tensor', '_tensor_b0',
    '_epinav', '_nd', '_rr_adc', '_rr_fa', '_rr_colfa', '_rr_tracew',
)

ALWAYS_IGNORE_PREFIXES = (
    'ax ', 'cor ', 'ax_', 'cor_', 'axial ', 'flair recon',
)


def should_ignore(series):
    sl = series.lower().strip()
    if sl in ALWAYS_IGNORE:
        return True
    for suffix in ALWAYS_IGNORE_SUFFIXES:
        if sl.endswith(suffix):
            return True
    for prefix in ALWAYS_IGNORE_PREFIXES:
        if sl.startswith(prefix):
            return True
    return False


def read_series_file(filepath):
    """Read a query_series log file and return a frozenset of series descriptions."""
    series = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not should_ignore(line):
                series.append(line)
    return frozenset(series)


def parse_subject_session(filename):
    """Extract subject and session from filename like sub-XXX_ses-001_series.txt"""
    base = os.path.basename(filename).replace('_series.txt', '')
    parts = base.split('_ses-')
    subject = parts[0]
    session = f"ses-{parts[1]}" if len(parts) > 1 else 'unknown'
    return subject, session


def main():
    parser = argparse.ArgumentParser(description='Discover protocol groups from query_series log files.')
    parser.add_argument('--logs-dir', required=True, help='Directory containing *_series.txt files')
    parser.add_argument('--output-tsv', default=None, help='Optional output TSV with per-subject group assignments')
    parser.add_argument('--min-subjects', type=int, default=1, help='Minimum subjects to display a group (default: 1)')
    parser.add_argument('--show-subjects', action='store_true', help='List subject IDs under each group')
    args = parser.parse_args()

    # Find all series files
    pattern = os.path.join(args.logs_dir, '*_series.txt')
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"No *_series.txt files found in {args.logs_dir}")
        return

    print(f"Found {len(files)} series log files\n")

    # Group subjects by sequence set
    groups = defaultdict(list)  # frozenset -> list of (subject, session, filepath)
    empty_files = []

    for f in files:
        subject, session = parse_subject_session(f)
        series_set = read_series_file(f)
        if not series_set:
            empty_files.append((subject, session))
            continue
        groups[series_set].append((subject, session, f))

    # Sort groups by size descending
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)

    # Print summary
    print(f"Found {len(sorted_groups)} unique protocol group(s)\n")
    print("=" * 70)

    group_assignments = []  # for TSV output

    for i, (series_set, subjects) in enumerate(sorted_groups, 1):
        n = len(subjects)
        if n < args.min_subjects:
            continue

        sequences = sorted(series_set)
        print(f"\nProtocol Group {i} (n={n}):")
        print(f"  Sequences ({len(sequences)}):")
        for seq in sequences:
            print(f"    - {seq}")

        if args.show_subjects:
            print(f"  Subjects:")
            for subj, sess, _ in sorted(subjects):
                print(f"    {subj} {sess}")

        for subj, sess, _ in subjects:
            group_assignments.append({
                'subject': subj,
                'session': sess,
                'protocol_group': i,
                'n_sequences': len(sequences),
                'sequences': ' | '.join(sequences),
            })

    if empty_files:
        print(f"\n{'=' * 70}")
        print(f"\nEmpty/incomplete sessions ({len(empty_files)}):")
        for subj, sess in empty_files:
            print(f"  {subj} {sess}")

    # Write TSV if requested
    if args.output_tsv:
        import csv
        with open(args.output_tsv, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['subject', 'session', 'protocol_group', 'n_sequences', 'sequences'], delimiter='\t')
            writer.writeheader()
            writer.writerows(sorted(group_assignments, key=lambda x: (x['protocol_group'], x['subject'])))
        print(f"\nWrote per-subject group assignments to {args.output_tsv}")

    print(f"\nTotal subjects processed: {len(files) - len(empty_files)}")
    print(f"Empty sessions: {len(empty_files)}")


if __name__ == '__main__':
    main()