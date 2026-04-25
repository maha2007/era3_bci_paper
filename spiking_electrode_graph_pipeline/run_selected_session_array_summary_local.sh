#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKIP_SUMMARY=0

if [[ "${1:-}" == "--skip-summary" ]]; then
  SKIP_SUMMARY=1
  shift
fi

MANIFEST_JSON="${1:-$SCRIPT_DIR/selected_session_blocks_manifest.json}"
ROOT_DERIVED="${2:-$SCRIPT_DIR/input_mats}"

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

cd "$SCRIPT_DIR"

# Scan every manifest entry once and collect only the indices whose expected
# chosen-block `.mat` file is still missing under the requested ROOT_DERIVED.
MISSING_INDICES=()
MISSING_PATHS=()
MISSING_LABELS=()
while IFS=$'\t' read -r IDX SESSION BLOCK MAT_PATH EXISTS; do
  if [[ -z "$IDX" ]]; then
    continue
  fi

  if [[ "$EXISTS" == "1" ]]; then
    echo "Skipping session=$SESSION block=$BLOCK; already exists at $MAT_PATH"
    continue
  fi

  echo "Missing session=$SESSION block=$BLOCK; expected at $MAT_PATH"
  MISSING_INDICES+=("$IDX")
  MISSING_PATHS+=("$MAT_PATH")
  MISSING_LABELS+=("session=$SESSION block=$BLOCK")
done < <("$PYTHON_BIN" - "$MANIFEST_JSON" "$ROOT_DERIVED" <<'PY'
import json
import os
import sys

mf = json.load(open(sys.argv[1]))
root_derived = sys.argv[2]
for idx, entry in enumerate(mf["entries"]):
    session = entry["session"]
    block = str(entry["chosen_block"])
    mat_path = os.path.join(root_derived, session, "ns5_block_features", f"{block}.mat")
    exists = "1" if os.path.exists(mat_path) else "0"
    print("\t".join([str(idx), session, block, mat_path, exists]))
PY
)

# Submit one targeted SLURM array only when there is missing work. The array
# spec is sparse, so only the missing manifest indices are scheduled.
if ((${#MISSING_INDICES[@]} > 0)); then
  if ! command -v sbatch >/dev/null 2>&1; then
    echo "ERROR: sbatch is required for missing entries but is not available in PATH." >&2
    exit 1
  fi
  if ! command -v squeue >/dev/null 2>&1; then
    echo "ERROR: squeue is required for missing entries but is not available in PATH." >&2
    exit 1
  fi
  if [[ ! -f "$SCRIPT_DIR/run_selected_session_blocks.sbatch" ]]; then
    echo "ERROR: Missing SLURM worker script: $SCRIPT_DIR/run_selected_session_blocks.sbatch" >&2
    exit 1
  fi

  mapfile -t SLURM_META < <("$PYTHON_BIN" - "$MANIFEST_JSON" <<'PY'
import json
import sys

mf = json.load(open(sys.argv[1]))
slurm = mf.get("slurm")
if not isinstance(slurm, dict):
    raise SystemExit("Manifest is missing the required 'slurm' object")

required_keys = ["partition", "time", "mem", "cpus"]
for key in required_keys:
    if key not in slurm:
        raise SystemExit(f"Manifest slurm config is missing required key: {key}")

print(str(slurm["partition"]))
print(str(slurm["time"]))
print(str(slurm["mem"]))
print(str(slurm["cpus"]))
PY
)

  SLURM_PARTITION="${SLURM_META[0]}"
  SLURM_TIME="${SLURM_META[1]}"
  SLURM_MEM="${SLURM_META[2]}"
  SLURM_CPUS="${SLURM_META[3]}"
  ARRAY_SPEC="$(IFS=,; echo "${MISSING_INDICES[*]}")"

  SBATCH_CMD=(
    sbatch
    --parsable
    "--array=$ARRAY_SPEC"
    "--partition=$SLURM_PARTITION"
    "--time=$SLURM_TIME"
    "--mem=$SLURM_MEM"
    "--cpus-per-task=$SLURM_CPUS"
    "$SCRIPT_DIR/run_selected_session_blocks.sbatch"
    "$MANIFEST_JSON"
  )

  echo "Submitting SLURM array for missing indices: $ARRAY_SPEC"
  printf '  %q' "${SBATCH_CMD[@]}"
  echo
  SBATCH_OUTPUT="$(${SBATCH_CMD[@]})"
  JOB_ID="${SBATCH_OUTPUT%%;*}"
  if [[ -z "$JOB_ID" ]]; then
    echo "ERROR: Failed to parse job ID from sbatch output: $SBATCH_OUTPUT" >&2
    exit 1
  fi
  echo "Submitted SLURM job $JOB_ID"

  # Poll squeue until the submitted array job leaves the queue. The poll
  # interval is configurable via SQUEUE_POLL_INTERVAL_SECONDS and defaults to 60.
  SQUEUE_POLL_INTERVAL_SECONDS="${SQUEUE_POLL_INTERVAL_SECONDS:-60}"
  if ! [[ "$SQUEUE_POLL_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || ((SQUEUE_POLL_INTERVAL_SECONDS <= 0)); then
    echo "ERROR: SQUEUE_POLL_INTERVAL_SECONDS must be a positive integer; got '$SQUEUE_POLL_INTERVAL_SECONDS'." >&2
    exit 1
  fi

  echo "Waiting for SLURM job $JOB_ID to finish; polling every ${SQUEUE_POLL_INTERVAL_SECONDS}s"
  while true; do
    if ! SQUEUE_OUTPUT="$(squeue -h -j "$JOB_ID" -o '%i %T %M %R')"; then
      echo "ERROR: squeue failed while waiting for job $JOB_ID." >&2
      exit 1
    fi

    if [[ -z "$SQUEUE_OUTPUT" ]]; then
      echo "[$(date)] Job $JOB_ID is no longer in the queue"
      break
    fi

    echo "[$(date)] Job $JOB_ID still active:"
    while IFS= read -r line; do
      if [[ -n "$line" ]]; then
        echo "  $line"
      fi
    done <<< "$SQUEUE_OUTPUT"
    sleep "$SQUEUE_POLL_INTERVAL_SECONDS"
  done

  # Re-check only the paths that were missing before submission and fail with a
  # clear list if any expected `.mat` outputs are still absent.
  STILL_MISSING=()
  for i in "${!MISSING_PATHS[@]}"; do
    if [[ ! -f "${MISSING_PATHS[$i]}" ]]; then
      STILL_MISSING+=("${MISSING_LABELS[$i]} -> ${MISSING_PATHS[$i]}")
    fi
  done
  if ((${#STILL_MISSING[@]} > 0)); then
    echo "ERROR: SLURM job $JOB_ID finished, but some expected .mat files are still missing:" >&2
    for item in "${STILL_MISSING[@]}"; do
      echo "  $item" >&2
    done
    exit 1
  fi
else
  # If nothing is missing, skip submission entirely and go straight to summary
  # generation.
  echo "All selected-session .mat files already exist under $ROOT_DERIVED"
fi

if ((SKIP_SUMMARY)); then
  echo "Skipping selected-session array firing summary build"
  exit 0
fi

"$PYTHON_BIN" "$SCRIPT_DIR/build_selected_session_array_summary.py" \
  "$MANIFEST_JSON" \
  --root-derived "$ROOT_DERIVED"
