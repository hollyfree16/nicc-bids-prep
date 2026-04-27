import argparse
import csv
from pathlib import Path


def sort_key(row):
    def as_int(value, default=999999):
        try:
            return int(float(value))
        except Exception:
            return default

    return (
        row.get("scan_date", ""),
        row.get("subject", ""),
        row.get("session", ""),
        as_int(row.get("series_number", "")),
        row.get("acquisition_time", "") or row.get("series_time", ""),
        row.get("series_description", ""),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-tsv", required=True)
    parser.add_argument("--sort", action="store_true")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    files = sorted(input_dir.glob("*_series.tsv"))

    if not files:
        raise SystemExit(f"No *_series.tsv files found in {input_dir}")

    rows = []
    fieldnames = None

    for path in files:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            if fieldnames is None:
                fieldnames = reader.fieldnames
            elif reader.fieldnames != fieldnames:
                raise SystemExit(f"Column mismatch in {path}")

            rows.extend(reader)

    if args.sort:
        rows.sort(key=sort_key)

    with Path(args.output_tsv).open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows from {len(files)} files to {args.output_tsv}")


if __name__ == "__main__":
    main()