import pydicom, glob, argparse

parser = argparse.ArgumentParser(description='Extract unique DICOM series descriptions.')
parser.add_argument('--input-dir', required=True, help='Root directory to search for .dcm files')
parser.add_argument('--output-txt', required=True, help='Output text file to write results')
args = parser.parse_args()

seen = set()
results = []
for f in glob.iglob(args.input_dir + '/*/*/*.dcm'):
    try:
        d = pydicom.dcmread(f, stop_before_pixels=True)
        desc = getattr(d, 'SeriesDescription', None)
        if desc and desc not in seen:
            seen.add(desc)
            results.append(desc)
    except Exception:
        continue

sorted_results = sorted(results)

with open(args.output_txt, 'w') as out:
    for desc in sorted_results:
        out.write(desc + '\n')

print(f"Wrote {len(sorted_results)} series descriptions to {args.output_txt}")