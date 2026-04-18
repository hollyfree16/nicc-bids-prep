#!/usr/bin/env python3
"""
discover_protocols.py
=====================
Reads a directory of query_series log files and groups subjects by unique
sequence sets, then clusters similar groups into probable protocols.

Usage:
    python discover_protocols.py --logs-dir /path/to/logs/query_series/SITE
    python discover_protocols.py --logs-dir /path/to/logs --similarity 0.8
    python discover_protocols.py --logs-dir /path/to/logs --output-tsv protocols.tsv --show-subjects

Options:
    --similarity    Jaccard similarity threshold for clustering groups (default: 0.75)
    --output-tsv    Save per-subject protocol assignments to TSV
    --show-subjects List subject IDs under each protocol
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
    'aascout', 'aascout_mpr_cor', 'aascout_mpr_sag', 'aascout_mpr_tra',
}

ALWAYS_IGNORE_SUFFIXES = (
    '_adc', '_fa', '_colfa', '_tracew', '_tensor', '_tensor_b0',
    '_epinav', '_nd', '_rr_adc', '_rr_fa', '_rr_colfa', '_rr_tracew',
    '_mpr_cor', '_mpr_sag', '_mpr_tra', '_mpr cor', '_mpr sag', '_mpr tra',
    ' mpr cor', ' mpr sag', ' mpr tra', ' mpr',
)

ALWAYS_IGNORE_PREFIXES = (
    'ax ', 'cor ', 'ax_', 'cor_', 'axial ', 'flair recon',
    '1 slice loc',
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


def jaccard(set_a, set_b):
    """Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 1.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union


def cluster_groups(groups, threshold):
    """
    Cluster unique sequence sets into probable protocols using Jaccard similarity.
    Returns list of clusters, each cluster is a list of (series_set, subjects) tuples.
    Uses single-linkage: a group joins a cluster if it's similar enough to ANY member.
    """
    # Sort by size descending so largest groups anchor clusters
    sorted_groups = sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)

    clusters = []  # list of lists of (series_set, subjects)

    for series_set, subjects in sorted_groups:
        placed = False
        for cluster in clusters:
            # Check similarity against all members of this cluster
            for member_set, _ in cluster:
                if jaccard(series_set, member_set) >= threshold:
                    cluster.append((series_set, subjects))
                    placed = True
                    break
            if placed:
                break
        if not placed:
            clusters.append([(series_set, subjects)])

    return clusters


def get_cluster_core(cluster):
    """Get the intersection of all sequence sets in a cluster (core sequences)."""
    sets = [s for s, _ in cluster]
    return sets[0].intersection(*sets[1:]) if len(sets) > 1 else sets[0]


def get_cluster_all(cluster):
    """Get the union of all sequence sets in a cluster (all seen sequences)."""
    sets = [s for s, _ in cluster]
    result = sets[0]
    for s in sets[1:]:
        result = result | s
    return result


def main():
    parser = argparse.ArgumentParser(description='Discover protocol groups from query_series log files.')
    parser.add_argument('--logs-dir', required=True, help='Directory containing *_series.txt files')
    parser.add_argument('--output-tsv', default=None, help='Optional output TSV with per-subject group assignments')
    parser.add_argument('--similarity', type=float, default=0.75, help='Jaccard similarity threshold for clustering (default: 0.75)')
    parser.add_argument('--show-subjects', action='store_true', help='List subject IDs under each protocol')
    args = parser.parse_args()

    pattern = os.path.join(args.logs_dir, '*_series.txt')
    files = sorted(glob.glob(pattern))

    if not files:
        print(f"No *_series.txt files found in {args.logs_dir}")
        return

    print(f"Found {len(files)} series log files")
    print(f"Clustering with Jaccard similarity threshold: {args.similarity}\n")

    # Read all files and group by exact sequence set
    exact_groups = defaultdict(list)
    empty_files = []

    for f in files:
        subject, session = parse_subject_session(f)
        series_set = read_series_file(f)
        if not series_set:
            empty_files.append((subject, session))
            continue
        exact_groups[series_set].append((subject, session))

    print(f"Unique exact sequence sets: {len(exact_groups)}")

    # Cluster into probable protocols
    clusters = cluster_groups(exact_groups, args.similarity)

    # Sort clusters by total subject count
    clusters.sort(key=lambda c: sum(len(subjs) for _, subjs in c), reverse=True)

    print(f"Probable protocols after clustering: {len(clusters)}\n")
    print("=" * 70)

    group_assignments = []

    for i, cluster in enumerate(clusters, 1):
        total_subjects = sum(len(subjs) for _, subjs in cluster)
        core_seqs = sorted(get_cluster_core(cluster))
        all_seqs = sorted(get_cluster_all(cluster))
        optional_seqs = sorted(get_cluster_all(cluster) - get_cluster_core(cluster))

        print(f"\nProtocol {i} (n={total_subjects}):")
        print(f"  Core sequences ({len(core_seqs)}) — present in all subjects:")
        for seq in core_seqs:
            print(f"    + {seq}")

        if optional_seqs:
            print(f"  Optional/variable sequences ({len(optional_seqs)}) — present in some subjects:")
            for seq in optional_seqs:
                print(f"    ~ {seq}")

        if len(cluster) > 1:
            print(f"  Sub-groups: {len(cluster)} unique sequence variants collapsed into this protocol")

        if args.show_subjects:
            print(f"  Subjects:")
            for _, subjs in cluster:
                for subj, sess in sorted(subjs):
                    print(f"    {subj} {sess}")

        for _, subjs in cluster:
            for subj, sess in subjs:
                group_assignments.append({
                    'subject':        subj,
                    'session':        sess,
                    'protocol_group': i,
                    'n_core_seqs':    len(core_seqs),
                    'n_all_seqs':     len(all_seqs),
                    'core_sequences': ' | '.join(core_seqs),
                })

    if empty_files:
        print(f"\n{'=' * 70}")
        print(f"\nEmpty/incomplete sessions ({len(empty_files)}):")
        for subj, sess in empty_files:
            print(f"  {subj} {sess}")

    if args.output_tsv:
        import csv
        with open(args.output_tsv, 'w', newline='') as f:
            writer = csv.DictWriter(
                f,
                fieldnames=['subject', 'session', 'protocol_group', 'n_core_seqs', 'n_all_seqs', 'core_sequences'],
                delimiter='\t'
            )
            writer.writeheader()
            writer.writerows(sorted(group_assignments, key=lambda x: (x['protocol_group'], x['subject'])))
        print(f"\nWrote per-subject group assignments to {args.output_tsv}")

    print(f"\nTotal subjects processed: {len(files) - len(empty_files)}")
    print(f"Empty sessions: {len(empty_files)}")


if __name__ == '__main__':
    main()