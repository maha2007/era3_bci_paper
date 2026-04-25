#!/bin/bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  bash spike_plot_pipeline/run_selected_session_pipeline_and_plots_local.sh \
    --manifest /path/to/selected_session_blocks_manifest.json \
    --root-derived /oak/.../spiking_electrode_graph_pipeline/input_mats \
    --plot-root /oak/.../spike_plot_pipeline/output/block_plots

This mini-pipeline:
  1. uses the multi-session chosen-block manifest
  2. runs run_selected_session_array_summary_local.sh --skip-summary to create
     any missing chosen-block mats via run_selected_session_blocks.sbatch
  3. plots spike raster/panel outputs for every chosen block in the manifest
USAGE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHUNKS_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
GRAPH_DIR="$CHUNKS_DIR/spiking_electrode_graph_pipeline"

MANIFEST_JSON="$GRAPH_DIR/selected_session_blocks_manifest.json"
ROOT_DERIVED="$GRAPH_DIR/input_mats"
PLOT_ROOT="$SCRIPT_DIR/output/block_plots"
TX_KEY="tx_from_ns5_45"

die() {
  echo "$*" >&2
  exit 2
}

while (($#)); do
  case "$1" in
    --manifest)
      MANIFEST_JSON="$2"
      shift 2
      ;;
    --root-derived)
      ROOT_DERIVED="$2"
      shift 2
      ;;
    --plot-root)
      PLOT_ROOT="$2"
      shift 2
      ;;
    --tx-key)
      TX_KEY="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      die "Unknown argument: $1"
      ;;
  esac
done

[[ -f "$MANIFEST_JSON" ]] || die "Manifest not found: $MANIFEST_JSON"
[[ -f "$GRAPH_DIR/run_selected_session_array_summary_local.sh" ]] || die "Missing graph pipeline local runner"
[[ -f "$SCRIPT_DIR/plot_selected_session_manifest.py" ]] || die "Missing manifest plotting script"

echo "Ensuring chosen-block mats exist for multi-session manifest: $MANIFEST_JSON"
bash "$GRAPH_DIR/run_selected_session_array_summary_local.sh" --skip-summary "$MANIFEST_JSON" "$ROOT_DERIVED"

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

PLOT_CMD=(
  "$PYTHON_BIN" "$SCRIPT_DIR/plot_selected_session_manifest.py"
  "$MANIFEST_JSON"
  --root-derived "$ROOT_DERIVED"
  --plot-root "$PLOT_ROOT"
  --tx-key "$TX_KEY"
)

echo "Plotting selected-session spike outputs:"
printf '  %q' "${PLOT_CMD[@]}"
echo
"${PLOT_CMD[@]}"
