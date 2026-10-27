#!/usr/bin/env bash
# Renders every BigQuery SQL file with test values and dry-runs it against a project,
# catching syntax errors, missing tables/columns and permission problems before deployment.
set -euo pipefail

PROJECT="${1:?usage: bq_dry_run.sh <gcp-project>}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$(mktemp -d)"
trap 'rm -rf "$OUT"' EXIT

python3 - "$ROOT/bigquery" "$OUT" "$PROJECT" <<'PY'
import pathlib, sys
from jinja2 import Environment, StrictUndefined
src, out = (pathlib.Path(a) for a in sys.argv[1:3])
project = sys.argv[3]
env = Environment(undefined=StrictUndefined)
ctx = {"project": project, "location": "europe-west2", "params": {"project": project},
       "ds": "2026-10-26", "run_id": "ci_dry_run"}
for f in sorted(src.rglob("*.sql")):
    target = out / f.relative_to(src)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(env.from_string(f.read_text()).render(**ctx))
    print(f"rendered {f.relative_to(src)}")
PY

status=0
for f in $(find "$OUT/pipelines" "$OUT/dq" -name '*.sql' | sort); do
  if bq query --project_id="$PROJECT" --use_legacy_sql=false --dry_run < "$f" > /dev/null; then
    echo "OK    ${f#$OUT/}"
  else
    echo "FAIL  ${f#$OUT/}"; status=1
  fi
done
exit $status
