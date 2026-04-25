# era3_bci_paper

This repository contains a cleaned, code-only snapshot of the `ns5_featurizer_chunks`
pipeline code.

Included:
- `other/`: standalone NS5 featurization code and vendored `brpylib`
- `spike_plot_pipeline/`: block-level spike raster and panel plotting
- `spiking_electrode_graph_pipeline/`: selected-session block generation and
  array firing summary plotting

Excluded from this snapshot:
- raw `.ns5` recordings
- generated `.mat` and `.npz` outputs
- logs, plots, and summary artifacts
- local virtual environments
- checked-in manifests with machine-specific absolute paths

The code still expects local data paths, `gsutil`, and in some workflows a SLURM
environment. See the per-directory READMEs for usage details.

## Quick Start

### Full pipeline example

This example runs the selected-session graph workflow end to end:

```bash
cd /path/to/repo

python3 spiking_electrode_graph_pipeline/submit_selected_session_blocks.py \
  --subject t12 \
  --start-session t12.2025.11.04 \
  --root-data /path/to/local_data \
  --root-derived /path/to/repo/spiking_electrode_graph_pipeline/input_mats \
  --repo-dir /path/to/repo/other \
  --script-path /path/to/repo/spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch \
  --submit

bash spiking_electrode_graph_pipeline/run_selected_session_array_summary_local.sh \
  /path/to/repo/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json \
  /path/to/repo/spiking_electrode_graph_pipeline/input_mats
```

Parameter notes:
- `--subject t12`: required subject code. No default.
- `--start-session t12.2025.11.04`: required first session in the consecutive run. No default.
- `--root-data`: required local session-data root used by the featurizer. No default.
- `--root-derived`: required output root where generated block features are written. No default.
- `--repo-dir`: points the worker at the `other/` code directory. The default is `../other`.
- `--script-path`: points at the worker sbatch script. The default is `run_selected_session_blocks.sbatch` in the graph pipeline.
- `--submit`: without this flag, the manifest is written but no SLURM array is submitted.

Important defaults if you omit them:
- `--bucket` defaults to `exp_sessions_nearline`.
- `--n-sessions` defaults to `10`.
- `--min-duration-s` defaults to `300`.
- SLURM defaults are `partition=normal`, `time=12:00:00`, `mem=32G`, `cpus=4`.

### Partial pipeline example

This example skips featurization and plots one existing block feature file:

```bash
cd /path/to/repo

python3 spike_plot_pipeline/plot_chunk_mats.py \
  /path/to/derived/t12.2025.11.04/ns5_block_features/0.mat
```

Parameter notes:
- positional argument: required `.mat` or `.npz` feature file to plot.

Important defaults if you omit them:
- `--tx-key` defaults to `tx_from_ns5_45`.
- `--outdir` defaults to the same directory as the input feature file.
