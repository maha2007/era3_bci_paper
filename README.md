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
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/submit_selected_session_blocks.py \
  --subject t12 \
  --start-session t12.2025.06.24 \
  --root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats \
  --repo-dir /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other \
  --script-path /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch \
  --submit

bash /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/run_selected_session_array_summary_local.sh \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats
```

Parameter notes:
- `--subject t12`: required subject code. No default.
- `--start-session t12.2025.06.24`: required first session in the consecutive run. No default.
- `--root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other`: local session-data root used by the featurizer. No default.
- `--root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats`: output root where generated block features are written. No default.
- `--repo-dir /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other`: points the worker at the repo's `other/` code directory. The default is `../other`.
- `--script-path /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch`: points at the worker sbatch script. The default is `run_selected_session_blocks.sbatch` in the graph pipeline.
- `--submit`: without this flag, the manifest is written but no SLURM array is submitted.

Important defaults if you omit them:
- `--bucket` defaults to `exp_sessions_nearline`.
- `--n-sessions` defaults to `10`.
- `--min-duration-s` defaults to `300`.
- SLURM defaults are `partition=normal`, `time=12:00:00`, `mem=32G`, `cpus=4`.

### Partial pipeline example

This example skips featurization and plots one existing block feature file:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/plot_chunk_mats.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats/t12.2025.06.24/ns5_block_features/3.mat
```

Parameter notes:
- positional argument: required `.mat` or `.npz` feature file to plot.

Important defaults if you omit them:
- `--tx-key` defaults to `tx_from_ns5_45`.
- `--outdir` defaults to the same directory as the input feature file.
