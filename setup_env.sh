#!/usr/bin/env bash
# Creates a project-local venv with heudiconv installed and the date-check
# line patched. Re-run this script after upgrading heudiconv.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$REPO_DIR/.venv"

echo "Creating venv at $VENV ..."
python3 -m venv "$VENV"

echo "Installing packages ..."
"$VENV/bin/pip" install --upgrade pip --quiet
"$VENV/bin/pip" install heudiconv pydicom

echo "Patching heudiconv date check ..."
"$VENV/bin/python3" - "$VENV" <<'PYEOF'
import glob, sys
venv = sys.argv[1]
matches = glob.glob(f"{venv}/lib/python*/site-packages/heudiconv/bids.py")
if not matches:
    sys.exit("ERROR: heudiconv/bids.py not found in venv")
target      = 'raise ValueError("There must be no dates in .json sidecar")'
replacement = 'pass  # date check disabled'
for path in matches:
    text = open(path).read()
    if target in text:
        open(path, "w").write(text.replace(target, replacement))
        print(f"Patched:         {path}")
    elif replacement in text:
        print(f"Already patched: {path}")
    else:
        print(f"WARNING: expected line not found in {path}", file=sys.stderr)
PYEOF

echo ""
echo "Done. heudiconv binary: $VENV/bin/heudiconv"
echo "Re-run this script after any heudiconv upgrade to re-apply the patch."
