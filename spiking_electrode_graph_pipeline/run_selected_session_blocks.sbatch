#!/bin/bash
#SBATCH --job-name=t12_10rand_chunks
#SBATCH --output=logs/slurm_%A_%a.out
#SBATCH --error=logs/slurm_%A_%a.err

set -euo pipefail

MANIFEST_JSON="$1"

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

mapfile -t META < <("$PYTHON_BIN" - "$MANIFEST_JSON" "$SLURM_ARRAY_TASK_ID" <<'PY'
import json
import sys

mf = json.load(open(sys.argv[1]))
idx = int(sys.argv[2])
e = mf["entries"][idx]
values = [
    mf["repo_dir"],
    mf["gsutil"],
    mf["bucket"],
    mf["subject"],
    mf["root_data"],
    mf["root_derived"],
    e["session"],
    str(e["chosen_block"]),
    "" if mf.get("chunk_start_s") is None else str(mf.get("chunk_start_s")),
    "" if mf.get("chunk_duration_s") is None else str(mf.get("chunk_duration_s")),
    json.dumps(mf.get("pipeline_args", [])),
    mf.get("plot_tx_key", ""),
    mf.get("plot_outdir", ""),
    mf["script_path"],
]
for v in values:
    print(v)
PY
)

REPO_DIR="${META[0]}"
GSUTIL="${META[1]}"
BUCKET="${META[2]}"
SUBJECT="${META[3]}"
ROOT_DATA="${META[4]}"
ROOT_DERIVED="${META[5]}"
SESSION="${META[6]}"
BLOCK="${META[7]}"
CHUNK_START_S="${META[8]}"
CHUNK_DURATION_S="${META[9]}"
PIPELINE_ARGS_JSON="${META[10]}"
PLOT_TX_KEY="${META[11]}"
PLOT_OUTDIR="${META[12]}"
SCRIPT_PATH="${META[13]}"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PLOT_SCRIPT="$SCRIPT_DIR/../spike_plot_pipeline/plot_chunk_mats.py"
if [ -z "$PLOT_TX_KEY" ]; then
  PLOT_TX_KEY="tx_from_ns5_45"
fi

mapfile -t EXTRA_PIPELINE_ARGS < <("$PYTHON_BIN" - "$PIPELINE_ARGS_JSON" <<'PY'
import json
import sys
for item in json.loads(sys.argv[1]):
    print(item)
PY
)

cd "$REPO_DIR"

echo "Started: $(date)"
echo "Host: $(hostname)"
echo "Repo: $REPO_DIR"
echo "Session: $SESSION"
echo "Block: $BLOCK"
echo "Python: $PYTHON_BIN"
echo "Python version: $("$PYTHON_BIN" --version)"

PIPELINE_CMD=(
  "$PYTHON_BIN" "$REPO_DIR/session_featurize_to_mat.py"
  --gsutil "$GSUTIL"
  --bucket "$BUCKET"
  --subject "$SUBJECT"
  --session "$SESSION"
  --root-data "$ROOT_DATA"
  --root-derived "$ROOT_DERIVED"
  --initial-decimate-to-hz 15000
  --apply-lrr
  --download
  --blocks "$BLOCK"
)

if ((${#EXTRA_PIPELINE_ARGS[@]} > 0)); then
  PIPELINE_CMD+=("${EXTRA_PIPELINE_ARGS[@]}")
fi

if [ -n "$CHUNK_DURATION_S" ]; then
  PIPELINE_CMD+=(--chunk-duration-s "$CHUNK_DURATION_S")
  if [ -n "$CHUNK_START_S" ]; then
    PIPELINE_CMD+=(--chunk-start-s "$CHUNK_START_S")
  fi
fi

echo "Running pipeline:"
printf '  %q' "${PIPELINE_CMD[@]}"
echo
"${PIPELINE_CMD[@]}"

FEATURE_BASE="$ROOT_DERIVED/$SESSION/ns5_block_features/$BLOCK"
if [ -f "$FEATURE_BASE.mat" ]; then
  FEATURE_PATH="$FEATURE_BASE.mat"
elif [ -f "$FEATURE_BASE.npz" ]; then
  FEATURE_PATH="$FEATURE_BASE.npz"
else
  echo "Missing feature output for block $BLOCK under $ROOT_DERIVED/$SESSION/ns5_block_features" >&2
  exit 1
fi

if [ ! -f "$PLOT_SCRIPT" ]; then
  echo "Plot script not found: $PLOT_SCRIPT" >&2
  exit 1
fi

PLOT_CMD=("$PYTHON_BIN" "$PLOT_SCRIPT")
if [ -n "$PLOT_TX_KEY" ]; then
  PLOT_CMD+=(--tx-key "$PLOT_TX_KEY")
fi
if [ -z "$PLOT_OUTDIR" ]; then
  PLOT_OUTDIR="$SCRIPT_DIR/../spike_plot_pipeline/output/block_plots/$SESSION"
fi
mkdir -p "$PLOT_OUTDIR"
PLOT_CMD+=(--outdir "$PLOT_OUTDIR")
PLOT_CMD+=("$FEATURE_PATH")

echo "Running plotting:"
printf '  %q' "${PLOT_CMD[@]}"
echo
"${PLOT_CMD[@]}"

echo "Finished: $(date)"
