#!/bin/bash
#SBATCH --job-name=ns5_submit_blocks
#SBATCH --output=logs/slurm_%x_%j.out
#SBATCH --error=logs/slurm_%x_%j.err

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash spike_plot_pipeline/run_full_session_pipeline_and_plots.sbatch \
    --session t12.2025.11.04 \
    --root-data /path/to/repo/other \
    --root-derived /path/to/derived \
    -- --apply-lrr

This wrapper selects one or more blocks from one session, writes a
single-session selected-block manifest, plots any already-existing mats
immediately, and only submits
`run_selected_session_blocks.sbatch` for blocks whose feature mats are still
missing. If you pass `--block`, it targets exactly one block.

Wrapper arguments:
  --session SESSION          Required. Session name such as t12.2025.11.04.
  --subject SUBJECT          Optional. Defaults to the prefix before the first dot in SESSION.
  --root-data PATH           Required. Passed through in the manifest.
  --root-derived PATH        Required. Passed through in the manifest.
  --bucket NAME              Optional. Default: exp_sessions_nearline.
  --gsutil PATH              Optional. Default: ~/google-cloud-sdk/bin/gsutil.
  --repo-dir PATH            Optional. Default: the ns5_featurizer_chunks/other directory.
  --array-script PATH        Optional. Default: ../spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch.
  --manifest-out PATH        Optional. Output path for the single-session selected-block manifest.
                             Default: ../spiking_electrode_graph_pipeline/manifests/<session>_parallel_blocks_manifest.json.
  --block ID                 Optional. Run exactly one block, e.g. 7.
  --blocks CSV               Optional. Explicit block list, e.g. 0,1,2.
  --max-blocks N             Optional. Default: 10.
  --partition NAME           Optional. Default: normal.
  --time HH:MM:SS            Optional. Default: 12:00:00.
  --mem SIZE                 Optional. Default: 48G.
  --cpus N                   Optional. Default: 4.
  --tx-key KEY               Optional. Plot a specific tx field. Default: tx_from_ns5_45.
  --plot-outdir PATH         Optional. Default: spike_plot_pipeline/output/block_plots/<session>.
                             Existing mats are plotted here immediately, and missing
                             mats are plotted here by the worker tasks after featurization.
  --help                     Show this message.

Any arguments after `--` are passed through to session_featurize_to_mat.py
inside each array task.
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../other" && pwd)"
ARRAY_SCRIPT=""
MANIFEST_OUT=""
BUCKET="exp_sessions_nearline"
GSUTIL="${HOME}/google-cloud-sdk/bin/gsutil"
MAX_BLOCKS=10
PARTITION="normal"
TIME_LIMIT="00:30:00"
MEMORY="48G"
CPUS=1
TX_KEY="tx_from_ns5_45"
PLOT_OUTDIR=""
SUBJECT=""
SESSION=""
ROOT_DATA=""
ROOT_DERIVED=""
BLOCK=""
BLOCKS_CSV=""
declare -a PIPELINE_ARGS=()

die() {
  echo "$*" >&2
  exit 2
}

while (($#)); do
  case "$1" in
    --subject)
      SUBJECT="$2"
      shift 2
      ;;
    --session)
      SESSION="$2"
      shift 2
      ;;
    --root-data)
      ROOT_DATA="$2"
      shift 2
      ;;
    --root-derived)
      ROOT_DERIVED="$2"
      shift 2
      ;;
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    --gsutil)
      GSUTIL="$2"
      shift 2
      ;;
    --repo-dir)
      REPO_DIR="$2"
      shift 2
      ;;
    --array-script)
      ARRAY_SCRIPT="$2"
      shift 2
      ;;
    --manifest-out)
      MANIFEST_OUT="$2"
      shift 2
      ;;
    --block)
      BLOCK="$2"
      shift 2
      ;;
    --blocks)
      BLOCKS_CSV="$2"
      shift 2
      ;;
    --max-blocks)
      MAX_BLOCKS="$2"
      shift 2
      ;;
    --partition)
      PARTITION="$2"
      shift 2
      ;;
    --time)
      TIME_LIMIT="$2"
      shift 2
      ;;
    --mem)
      MEMORY="$2"
      shift 2
      ;;
    --cpus)
      CPUS="$2"
      shift 2
      ;;
    --tx-key)
      TX_KEY="$2"
      shift 2
      ;;
    --plot-outdir)
      PLOT_OUTDIR="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      PIPELINE_ARGS+=("$@")
      break
      ;;
    *)
      die "Unknown wrapper argument: $1"
      ;;
  esac
done

[[ -n "$SESSION" ]] || die "Missing required --session"
[[ -n "$ROOT_DATA" ]] || die "Missing required --root-data"
[[ -n "$ROOT_DERIVED" ]] || die "Missing required --root-derived"
[[ -z "$BLOCK" || -z "$BLOCKS_CSV" ]] || die "Pass only one of --block or --blocks"

if [[ -n "$BLOCK" ]]; then
  BLOCKS_CSV="$BLOCK"
  MAX_BLOCKS=1
fi

if [[ -z "$SUBJECT" ]]; then
  SUBJECT="${SESSION%%.*}"
fi
[[ -n "$SUBJECT" ]] || die "Could not derive subject from session '$SESSION'"

if [[ -z "$ARRAY_SCRIPT" ]]; then
  ARRAY_SCRIPT="$SCRIPT_DIR/../spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch"
fi
[[ -f "$ARRAY_SCRIPT" ]] || die "Array script not found: $ARRAY_SCRIPT"
[[ -x "$GSUTIL" || "$GSUTIL" == "gsutil" ]] || die "gsutil not found: $GSUTIL"

if [[ -z "$MANIFEST_OUT" ]]; then
  MANIFEST_DIR="$SCRIPT_DIR/manifests"
  mkdir -p "$MANIFEST_DIR"
  MANIFEST_OUT="$MANIFEST_DIR/${SESSION}_parallel_blocks_manifest.json"
else
  mkdir -p "$(dirname "$MANIFEST_OUT")"
fi

if [[ -z "$PLOT_OUTDIR" ]]; then
  PLOT_OUTDIR="$SCRIPT_DIR/output/block_plots/$SESSION"
fi
mkdir -p "$PLOT_OUTDIR"

cd "$REPO_DIR"

echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Session: $SESSION"
echo "Subject: $SUBJECT"
echo "Repo: $REPO_DIR"
echo "Array script: $ARRAY_SCRIPT"

if ((${#PIPELINE_ARGS[@]} > 0)); then
PIPELINE_ARGS_JSON=$(python3 - <<'PY' "${PIPELINE_ARGS[@]}"
import json, sys
args = list(sys.argv[1:])
if "--apply-lrr" not in args:
    args.append("--apply-lrr")
print(json.dumps(args))
PY
)
else
PIPELINE_ARGS_JSON='["--apply-lrr"]'
fi

python3 - <<'PY' "$GSUTIL" "$BUCKET" "$SUBJECT" "$SESSION" "$ROOT_DATA" "$ROOT_DERIVED" "$REPO_DIR" "$ARRAY_SCRIPT" "$MANIFEST_OUT" "$BLOCKS_CSV" "$MAX_BLOCKS" "$PARTITION" "$TIME_LIMIT" "$MEMORY" "$CPUS" "$TX_KEY" "$PLOT_OUTDIR" "$PIPELINE_ARGS_JSON"
import json
import re
import subprocess
import sys
from pathlib import Path

block_re = re.compile(r"\((\d+\.?\d*)\)")

def block_key(x: str):
    try:
        return (0, float(x))
    except Exception:
        return (1, x)

gsutil, bucket, subject, session, root_data, root_derived, repo_dir, array_script, manifest_out, blocks_csv, max_blocks, partition, time_limit, mem, cpus, tx_key, plot_outdir, pipeline_args_json = sys.argv[1:]
cmd = [gsutil, "ls", f"gs://{bucket}/{subject}/{session}/**/*.ns5"]
out = subprocess.check_output(cmd, universal_newlines=True)
found = []
for line in out.splitlines():
    line = line.strip()
    if not line:
        continue
    m = block_re.search(line)
    if not m:
        continue
    found.append(m.group(1))
block_ids = sorted(set(found), key=block_key)
if blocks_csv:
    requested = [x.strip() for x in blocks_csv.split(",") if x.strip()]
    missing = [x for x in requested if x not in block_ids]
    if missing:
        raise SystemExit(f"Requested blocks not found in session {session}: {', '.join(missing)}")
    chosen = requested
else:
    chosen = block_ids[: max(0, int(max_blocks))]
if not chosen:
    raise SystemExit(f"No blocks selected for session {session}")
manifest = {
    "bucket": bucket,
    "subject": subject,
    "start_session": session,
    "n_sessions": 1,
    "root_data": str(Path(root_data).resolve()),
    "root_derived": str(Path(root_derived).resolve()),
    "repo_dir": str(Path(repo_dir).resolve()),
    "script_path": str(Path(array_script).resolve()),
    "gsutil": str(Path(gsutil).expanduser().resolve()) if gsutil != "gsutil" else gsutil,
    "slurm": {
        "partition": partition,
        "time": time_limit,
        "mem": mem,
        "cpus": int(cpus),
    },
    "plot_tx_key": tx_key,
    "plot_outdir": plot_outdir,
    "pipeline_args": json.loads(pipeline_args_json),
    "selected_blocks": chosen,
    "entries": [],
}
feature_dir = Path(root_derived).resolve() / session / "ns5_block_features"
existing_feature_paths = []
existing_blocks = []
missing_blocks = []
for block_id in chosen:
    feature_path = None
    for suffix in (".mat", ".npz"):
        candidate = feature_dir / f"{block_id}{suffix}"
        if candidate.exists():
            feature_path = candidate
            break
    if feature_path is not None:
        existing_blocks.append(block_id)
        existing_feature_paths.append(str(feature_path))
        continue
    missing_blocks.append(block_id)
    manifest["entries"].append(
        {
            "session": session,
            "blocks_all": chosen,
            "chosen_block": block_id,
            "selection_rule": "explicit" if blocks_csv else f"first_{len(chosen)}_blocks",
        }
    )
manifest["existing_blocks"] = existing_blocks
manifest["missing_blocks"] = missing_blocks
manifest["existing_feature_paths"] = existing_feature_paths
manifest_path = Path(manifest_out).resolve()
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
print(f"Wrote manifest: {manifest_path}")
for i, block_id in enumerate(chosen):
    state = "existing_mat" if block_id in existing_blocks else "missing_mat"
    print(f"[{i}] {session} -> block {block_id} ({state})")
PY

mapfile -t EXISTING_FEATURES < <(python3 - <<'PY' "$MANIFEST_OUT"
import json
import sys

mf = json.load(open(sys.argv[1]))
for path in mf.get("existing_feature_paths", []):
    print(path)
PY
)

if ((${#EXISTING_FEATURES[@]} > 0)); then
  module purge
  module load devel
  module load math
  module load viz
  module load python/3.12.1
  module load py-numpy/1.26.3_py312
  module load py-scipy/1.12.0_py312
  module load py-matplotlib/3.8.3_py312
  export PYTHONNOUSERSITE=1
  PYTHON_BIN="$(command -v python3)"

  PLOT_CMD=("$PYTHON_BIN" "$SCRIPT_DIR/plot_chunk_mats.py")
  PLOT_CMD+=(--tx-key "$TX_KEY")
  PLOT_CMD+=(--outdir "$PLOT_OUTDIR")
  PLOT_CMD+=("${EXISTING_FEATURES[@]}")
  echo "Plotting existing mats:"
  printf '  %q' "${PLOT_CMD[@]}"
  echo
  "${PLOT_CMD[@]}"
fi

ARRAY_COUNT=$(python3 - <<'PY' "$MANIFEST_OUT"
import json, sys
mf = json.load(open(sys.argv[1]))
print(len(mf["entries"]))
PY
)

if (( ARRAY_COUNT == 0 )); then
  echo "All selected blocks already have mats; no SLURM submission needed."
  echo "Finished: $(date)"
  exit 0
fi

ARRAY_SPEC="0-$((ARRAY_COUNT - 1))"

SUBMIT_CMD=(
  sbatch
  --array="$ARRAY_SPEC"
  --partition="$PARTITION"
  --time="$TIME_LIMIT"
  --mem="$MEMORY"
  --cpus-per-task="$CPUS"
  "$ARRAY_SCRIPT"
  "$MANIFEST_OUT"
)

echo "Submitting array job:"
printf '  %q' "${SUBMIT_CMD[@]}"
echo
"${SUBMIT_CMD[@]}"

echo "Finished: $(date)"
