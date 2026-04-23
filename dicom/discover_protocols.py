#!/usr/bin/env python3
"""
discover_protocols.py
=====================
Reads a directory of query_series log files and detects probable MRI protocols
by identifying which sequences appear together at high frequency.

Usage:
    python discover_protocols.py --logs-dir /path/to/logs
    python discover_protocols.py --logs-dir /path/to/logs --similarity 0.8 --core-threshold 0.85
    python discover_protocols.py --logs-dir /path/to/logs --output-tsv assignments.tsv --show-subjects
    python discover_protocols.py --logs-dir /path/to/logs --output-template /path/to/templates/

Options:
    --similarity        Jaccard similarity threshold for clustering (default: 0.75)
    --core-threshold    Min within-protocol prevalence to call a sequence 'core' (default: 0.80)
    --output-tsv        Save per-subject protocol assignments to TSV
    --output-template   Write draft template TSV(s) for generate_bids_configs.py
                        (path to directory → one file per protocol; path ending in .tsv → single file)
    --show-subjects     List subject IDs under each protocol
"""

import os
import re
import glob
import csv
import argparse
from collections import defaultdict


# ============================================================================
# ALWAYS-IGNORE LIST
# Copied from bids/generate_bids_configs.py — keep in sync manually.
# Not imported to preserve module independence between dicom/ and bids/.
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

# Prevalence thresholds for sequence classification
UNIVERSAL_THRESH  = 0.90   # present in ≥90% of subjects — too common to discriminate protocols
NOISE_THRESH      = 0.05   # present in <5% of subjects  — too rare to be meaningful
OPTIONAL_LOW      = 0.20   # min within-cluster prevalence to report as "optional"
FINGERPRINT_SELF  = 0.80   # min within-cluster prevalence to be a fingerprint candidate
FINGERPRINT_OTHER = 0.30   # max cross-cluster prevalence for a fingerprint candidate


# ============================================================================
# I/O helpers
# ============================================================================

def should_ignore(series):
    return bool(ALWAYS_IGNORE_RE.search(series))


def read_series_file(filepath):
    series = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not should_ignore(line):
                series.append(line)
    return frozenset(series)


def parse_subject_session(filename):
    base = os.path.basename(filename).replace('_series.txt', '')
    parts = base.split('_ses-')
    subject = parts[0]
    session = f"ses-{parts[1]}" if len(parts) > 1 else 'unknown'
    return subject, session


# ============================================================================
# Core algorithm
# ============================================================================

def jaccard(set_a, set_b):
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union


def compute_global_prevalence(series_sets):
    """Return {sequence: fraction_of_subjects_with_it} across all subjects."""
    n = len(series_sets)
    if n == 0:
        return {}
    counts = defaultdict(int)
    for s in series_sets:
        for seq in s:
            counts[seq] += 1
    return {seq: count / n for seq, count in counts.items()}


def partition_sequences(global_prev, universal_thresh, noise_thresh):
    """Split sequences into (universal, noise, discriminating) sets."""
    universal      = {s for s, p in global_prev.items() if p >= universal_thresh}
    noise          = {s for s, p in global_prev.items() if p < noise_thresh}
    discriminating = {s for s in global_prev if s not in universal and s not in noise}
    return universal, noise, discriminating


def average_linkage_cluster(items, threshold):
    """
    Greedy average-linkage clustering by Jaccard similarity.

    items: list of (subject_index, projected_frozenset), sorted by set size descending.
    Returns list of clusters; each cluster is a list of subject indices.

    Average-linkage resists single-linkage chaining: a subject joins a cluster only
    if its average similarity to ALL existing cluster members meets the threshold.
    """
    # Each element: list of (subject_idx, proj_set) — kept together for avg computation
    clusters = []

    for idx, proj_set in items:
        best_cluster = None
        best_avg_sim = -1.0

        for cluster in clusters:
            sims = [jaccard(proj_set, member_set) for _, member_set in cluster]
            avg_sim = sum(sims) / len(sims)
            if avg_sim >= threshold and avg_sim > best_avg_sim:
                best_avg_sim = avg_sim
                best_cluster = cluster

        if best_cluster is not None:
            best_cluster.append((idx, proj_set))
        else:
            clusters.append([(idx, proj_set)])

    return [[idx for idx, _ in cluster] for cluster in clusters]


def characterize_cluster(indices, all_series_sets, core_threshold):
    """
    Describe a cluster of subjects in terms of per-sequence within-cluster prevalence.
    Uses the full (unfiltered/unprojected) series sets so universal sequences appear too.
    """
    n = len(indices)
    counts = defaultdict(int)
    for i in indices:
        for seq in all_series_sets[i]:
            counts[seq] += 1

    within_prev = {seq: count / n for seq, count in counts.items()}
    core_seqs     = sorted(s for s, p in within_prev.items() if p >= core_threshold)
    optional_seqs = sorted(
        s for s, p in within_prev.items()
        if OPTIONAL_LOW <= p < core_threshold
    )
    return {
        'n_subjects':   n,
        'within_prev':  within_prev,
        'core_seqs':    core_seqs,
        'optional_seqs': optional_seqs,
        'counts':       counts,
    }


def compute_fingerprints(cluster_indices, all_clusters, all_series_sets):
    """
    Return sequences that are prevalent (≥80%) within this cluster but rare (<30%)
    across all other clusters — i.e., distinctive markers for this protocol.
    """
    n_self = len(cluster_indices)
    self_set = set(cluster_indices)

    other_indices = [i for cluster in all_clusters for i in cluster if i not in self_set]
    n_other = len(other_indices)

    self_counts  = defaultdict(int)
    other_counts = defaultdict(int)

    for i in cluster_indices:
        for seq in all_series_sets[i]:
            self_counts[seq] += 1
    for i in other_indices:
        for seq in all_series_sets[i]:
            other_counts[seq] += 1

    fingerprints = []
    for seq, cnt in self_counts.items():
        self_prev  = cnt / n_self
        other_prev = (other_counts[seq] / n_other) if n_other > 0 else 0.0
        if self_prev >= FINGERPRINT_SELF and other_prev < FINGERPRINT_OTHER:
            fingerprints.append(seq)

    return sorted(fingerprints)


# ============================================================================
# Output writers
# ============================================================================

def write_per_subject_tsv(path, assignments):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['subject', 'session', 'protocol_group',
                        'n_core_seqs', 'n_all_seqs', 'core_sequences'],
            delimiter='\t'
        )
        writer.writeheader()
        writer.writerows(
            sorted(assignments, key=lambda x: (x['protocol_group'], x['subject']))
        )


def _write_protocol_rows(writer, cluster_info, fingerprints, protocol_name):
    core_set = set(cluster_info['core_seqs'])
    fp_set   = set(fingerprints)
    all_seqs = sorted(
        set(cluster_info['core_seqs']) | set(cluster_info['optional_seqs'])
    )
    for seq in all_seqs:
        writer.writerow({
            'site':               'FILL_IN',
            'protocol_name':      protocol_name,
            'fingerprint':        'yes' if seq in fp_set else 'no',
            'series_description': seq,
            'bids_folder':        'FILL_IN',
            'bids_suffix':        'FILL_IN',
            'expected':           'yes' if seq in core_set else 'no',
        })


def write_template_tsv(output_path, cluster_infos, fingerprint_lists):
    """
    Write draft template TSV(s) for generate_bids_configs.py.
    Universal sequences (>90% global prevalence) are excluded — add them manually.
    """
    fieldnames = ['site', 'protocol_name', 'fingerprint',
                  'series_description', 'bids_folder', 'bids_suffix', 'expected']

    use_dir = os.path.isdir(output_path) or not output_path.endswith('.tsv')

    if use_dir:
        os.makedirs(output_path, exist_ok=True)
        for i, (info, fps) in enumerate(zip(cluster_infos, fingerprint_lists), 1):
            dest = os.path.join(output_path, f'protocol_{i}.tsv')
            with open(dest, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
                writer.writeheader()
                _write_protocol_rows(writer, info, fps, f'protocol_{i}')
            print(f"  Wrote {dest}")
    else:
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
            writer.writeheader()
            for i, (info, fps) in enumerate(zip(cluster_infos, fingerprint_lists), 1):
                _write_protocol_rows(writer, info, fps, f'protocol_{i}')
        print(f"  Wrote {output_path}")


# ============================================================================
# main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Detect MRI protocols from query_series log files using frequency analysis.'
    )
    parser.add_argument('--logs-dir',        required=True,
                        help='Directory containing *_series.txt files')
    parser.add_argument('--similarity',      type=float, default=0.75,
                        help='Jaccard similarity threshold for clustering (default: 0.75)')
    parser.add_argument('--core-threshold',  type=float, default=0.80,
                        help='Min within-protocol prevalence to call a sequence core (default: 0.80)')
    parser.add_argument('--output-tsv',      default=None,
                        help='Optional output TSV with per-subject protocol assignments')
    parser.add_argument('--output-template', default=None,
                        help='Write draft template TSV(s) for generate_bids_configs.py '
                             '(directory → one file per protocol; *.tsv → single combined file)')
    parser.add_argument('--show-subjects',   action='store_true',
                        help='List subject IDs under each protocol')
    args = parser.parse_args()

    # ---- Load ---------------------------------------------------------------
    pattern = os.path.join(args.logs_dir, '*_series.txt')
    files   = sorted(glob.glob(pattern))

    if not files:
        print(f"No *_series.txt files found in {args.logs_dir}")
        return

    print(f"Found {len(files)} series log files.")

    subject_data = []   # list of {'subject', 'session', 'series_set'}
    empty_files  = []

    for filepath in files:
        subject, session = parse_subject_session(filepath)
        series_set = read_series_file(filepath)
        if not series_set:
            empty_files.append((subject, session))
        else:
            subject_data.append({
                'subject': subject,
                'session': session,
                'series_set': series_set,
            })

    n_subjects  = len(subject_data)
    all_series  = [d['series_set'] for d in subject_data]

    if n_subjects == 0:
        print("No non-empty series files found.")
        if empty_files:
            print(f"Empty/incomplete sessions ({len(empty_files)}):")
            for subj, sess in empty_files:
                print(f"  {subj} {sess}")
        return

    # ---- Prevalence analysis ------------------------------------------------
    global_prev = compute_global_prevalence(all_series)
    universal, noise, discriminating = partition_sequences(
        global_prev, UNIVERSAL_THRESH, NOISE_THRESH
    )

    n_total_seqs = len(global_prev)
    print(f"After filtering, {n_total_seqs} unique sequences across all subjects.")
    if universal:
        print(f"  {len(universal)} universal (≥{int(UNIVERSAL_THRESH*100)}% prevalence) — "
              f"present in essentially every protocol, omitted from template output.")
    if noise:
        print(f"  {len(noise)} noise (<{int(NOISE_THRESH*100)}% prevalence) — skipped.")
    print(f"  {len(discriminating)} discriminating sequences used for protocol clustering.")

    if not discriminating:
        discriminating = {s for s in global_prev if s not in noise}
        print("  Note: no discriminating sequences — falling back to all non-noise sequences.")

    # ---- Cluster subjects ---------------------------------------------------
    items = []
    for i, d in enumerate(subject_data):
        proj = frozenset(d['series_set'] & discriminating)
        items.append((i, proj))

    # Sort by projected-set size descending so largest anchors clusters first
    items.sort(key=lambda x: len(x[1]), reverse=True)

    cluster_indices_list = average_linkage_cluster(items, args.similarity)

    # Sort clusters by subject count descending
    cluster_indices_list.sort(key=len, reverse=True)

    n_clusters = len(cluster_indices_list)

    # ---- Characterize -------------------------------------------------------
    cluster_infos     = []
    fingerprint_lists = []

    for indices in cluster_indices_list:
        info = characterize_cluster(indices, all_series, args.core_threshold)
        fps  = compute_fingerprints(indices, cluster_indices_list, all_series)
        cluster_infos.append(info)
        fingerprint_lists.append(fps)

    # ---- Console output -----------------------------------------------------
    pct_label = f"≥{int(args.core_threshold * 100)}%"
    print(f"\n=== Protocol Detection  (similarity: {args.similarity},  core: {pct_label}) ===")

    assignments = []

    for i, (indices, info, fps) in enumerate(
        zip(cluster_indices_list, cluster_infos, fingerprint_lists), 1
    ):
        pct = round(100 * info['n_subjects'] / n_subjects)
        print(f"\n  PROTOCOL {i}  —  {info['n_subjects']} subjects ({pct}%)")

        if info['core_seqs']:
            print(f"  Core sequences (present in {pct_label} of subjects in this protocol):")
            for seq in info['core_seqs']:
                cnt = info['counts'][seq]
                p   = info['within_prev'][seq]
                print(f"    {seq:<50}  {cnt:>3}/{info['n_subjects']:<3}  {p:>5.0%}")
        else:
            print(f"  (no sequences meet the {pct_label} core threshold)")

        if info['optional_seqs']:
            print(f"  Optional sequences:")
            for seq in info['optional_seqs']:
                cnt = info['counts'][seq]
                p   = info['within_prev'][seq]
                print(f"    {seq:<50}  {cnt:>3}/{info['n_subjects']:<3}  {p:>5.0%}")

        if fps:
            print(f"  Suggested fingerprints (distinctive to this protocol):")
            for seq in fps:
                print(f"    {seq}")
        else:
            print(f"  Suggested fingerprints: none (sequences not sufficiently distinctive)")

        if args.show_subjects:
            print(f"  Subjects:")
            for idx in sorted(indices, key=lambda i: (subject_data[i]['subject'],
                                                       subject_data[i]['session'])):
                d = subject_data[idx]
                print(f"    {d['subject']}  {d['session']}")

        core_seqs_str = ' | '.join(info['core_seqs'])
        for idx in indices:
            d = subject_data[idx]
            assignments.append({
                'subject':        d['subject'],
                'session':        d['session'],
                'protocol_group': i,
                'n_core_seqs':    len(info['core_seqs']),
                'n_all_seqs':     len(info['within_prev']),
                'core_sequences': core_seqs_str,
            })

    # Warn about singletons
    n_singletons = sum(1 for c in cluster_indices_list if len(c) == 1)
    if n_singletons == n_clusters and n_clusters > 1:
        print(f"\n  Warning: all {n_clusters} clusters are singletons. "
              f"Consider lowering --similarity.")

    # Universal sequences summary
    if universal:
        print(f"\nUniversal sequences (present in >{int(UNIVERSAL_THRESH*100)}% of all subjects):")
        for seq in sorted(universal):
            p = global_prev[seq]
            print(f"  {seq:<50}  {p:>5.0%}")
        print("  (Add these to your template manually with appropriate bids_folder/bids_suffix.)")

    if empty_files:
        print(f"\nEmpty/incomplete sessions ({len(empty_files)}):")
        for subj, sess in empty_files:
            print(f"  {subj}  {sess}")

    print(f"\nTotal subjects: {n_subjects}    Protocols detected: {n_clusters}    "
          f"Empty sessions: {len(empty_files)}")

    # ---- File outputs -------------------------------------------------------
    if args.output_tsv:
        write_per_subject_tsv(args.output_tsv, assignments)
        print(f"\nWrote per-subject assignments to {args.output_tsv}")

    if args.output_template:
        print(f"\nWriting draft template TSV(s) to {args.output_template}")
        print(f"  Note: bids_folder and bids_suffix are placeholders — fill in before use.")
        if universal:
            print(f"  Note: {len(universal)} universal sequences omitted — add manually.")
        write_template_tsv(args.output_template, cluster_infos, fingerprint_lists)


if __name__ == '__main__':
    main()
