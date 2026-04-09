import pydicom, glob, os, argparse

parser = argparse.ArgumentParser(description='Extract study dates per subject from a DICOM directory.')
parser.add_argument('--input-dir', required=True, help='Root directory to search for .dcm files')
parser.add_argument('--output-txt', required=True, help='Output text file to write results')
args = parser.parse_args()

seen_subjects = set()
results = []
for f in glob.iglob(args.input_dir + '/*/*/*/*.dcm'):
    subject = f.split('/')[-4]

    if subject in seen_subjects:
        continue
    seen_subjects.add(subject)

    try:
        d = pydicom.dcmread(f, stop_before_pixels=True)
        date = getattr(d, 'StudyDate', 'N/A')
        results.append((subject, date))
    except Exception:
        continue

sorted_results = sorted(results)

with open(args.output_txt, 'w') as out:
    for subject, date in sorted_results:
        out.write(f"{subject}\t{date}\n")

print(f"Wrote {len(sorted_results)} study dates to {args.output_txt}")