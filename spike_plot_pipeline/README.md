# Spike Plot Pipeline

This folder contains the plotting code that turns standalone NS5 feature outputs
into spike diagnostic plots:

- `<block>_spike_raster.png`
- `<block>_spike_panel.png`
- `<block>_spike_summary.pdf`

There are two distinct plotting workflows in this directory:

1. one session, multiple selected blocks
2. multiple sessions, one chosen block per session

Those two workflows share plotting code, but they do not use the same manifest
type.

Terminology used in this README:

- `multi-session chosen-block manifest`
  - typical file: `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json`
  - meaning: one manifest entry per session, with exactly one chosen block in
    each session
- `single-session selected-block manifest`
  - typical file for session `t12.2025.06.24`: `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/manifests/t12.2025.06.24_parallel_blocks_manifest.json`
  - meaning: one manifest for one session, listing the selected blocks from that
    session

This cleaned repository snapshot does not include generated manifests, plots, or
feature outputs. In this environment, the runtime locations used by these
workflows are:

- `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/manifests/`
- `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/`
- `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats/`

## Outputs

By default, plots are written under:

- `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots/<session>/`

The plotting code is:

- `plot_chunk_mats.py`
  - reads one or more standalone block feature `.mat` or `.npz` files
  - writes `*_spike_raster.png`, `*_spike_panel.png`, and `*_spike_summary.pdf`

## Main Files

- `plot_chunk_mats.py`
  - low-level plotting entry point
  - loads one feature file at a time
  - chooses a `tx_from_ns5_*` field, defaulting to `tx_from_ns5_45`
  - writes raster, panel, and summary PDF outputs

- `run_full_session_pipeline_and_plots.sbatch`
  - workflow 1 runner
  - handles one session at a time
  - writes a fresh single-session selected-block manifest each run
  - plots already-existing feature mats immediately
  - submits only missing blocks to the shared worker

- `submit_selected_session_blocks.py`
  - workflow 2 manifest builder wrapper
  - delegates selection to
    `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/submit_selected_session_blocks.py`
  - rewrites the multi-session chosen-block manifest when you run it

- `run_selected_session_pipeline_and_plots_local.sh`
  - workflow 2 local plotting runner
  - consumes an existing multi-session chosen-block manifest
  - ensures chosen-block mats exist
  - then plots every chosen block recorded in that manifest

- `plot_selected_session_manifest.py`
  - loops over all manifest entries in workflow 2
  - resolves each chosen block under `root_derived`
  - calls `plot_chunk_mats.py` logic for each session/block pair

## Plotting Entry Point

The simplest direct call is:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/plot_chunk_mats.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats/t12.2025.06.24/ns5_block_features/3.mat
```

Optional arguments:

- `--tx-key tx_from_ns5_45` (default; override only when you want another threshold)
- `--outdir /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots/t12.2025.06.24`

### Direct partial example

This is the smallest plotting-only invocation:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/plot_chunk_mats.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats/t12.2025.06.24/ns5_block_features/3.mat
```

Parameter notes:
- positional path: required `.mat` or `.npz` feature file to plot.

Defaults used by this command:
- `--tx-key` defaults to `tx_from_ns5_45`.
- `--outdir` defaults to the directory containing the input feature file.

## Workflow 1: One Session, Multiple Selected Blocks

This workflow is for one session only. It does not select one block per
session across multiple sessions.

Runner:

- `run_full_session_pipeline_and_plots.sbatch`

What it does:

1. discovers blocks for one requested session from GCS
2. selects blocks using one of:
   - `--block <id>` for exactly one block
   - `--blocks 0,1,2` for an explicit block list
   - otherwise the first `--max-blocks` blocks found
3. writes a fresh single-session selected-block manifest under:
   - `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/manifests/<session>_parallel_blocks_manifest.json`
4. checks whether each selected block already has a local feature output under:
   - `<root-derived>/<session>/ns5_block_features/<block>.mat`
   - or `<block>.npz`
5. plots the existing feature files immediately
6. submits only the missing blocks to
   `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch`
7. those worker tasks featurize the missing blocks and plot them into the same
   plot output directory

### Full workflow 1 example

This command runs the one-session, multi-block plotting workflow:

```bash
bash /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/run_full_session_pipeline_and_plots.sbatch \
  --session t12.2025.06.24 \
  --root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats
```

What this command does:
- discovers the blocks available for `t12.2025.06.24`
- chooses the first `--max-blocks` blocks
- writes a single-session working manifest
- plots any blocks that already have local feature files
- submits worker jobs only for missing blocks

Parameters set explicitly:
- `--session t12.2025.06.24`: required session name. No default.
- `--root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other`: required local session-data root. No default.
- `--root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats`: required feature-output root. No default.

Important defaults used by this command:
- `--subject` defaults to the prefix before the first dot in `--session`.
- `--bucket` defaults to `exp_sessions_nearline`.
- `--gsutil` defaults to `~/google-cloud-sdk/bin/gsutil`.
- `--max-blocks` defaults to `10`.
- `--partition` defaults to `normal`.
- `--time` defaults to `00:30:00`.
- `--mem` defaults to `48G`.
- `--cpus` defaults to `1`.
- `--tx-key` defaults to `tx_from_ns5_45`.
- `--plot-outdir` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/output/block_plots/t12.2025.06.24`.

### Partial workflow 1 example

This command limits workflow 1 to one block:

```bash
bash /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/run_full_session_pipeline_and_plots.sbatch \
  --session t12.2025.06.24 \
  --root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats \
  --block 3
```

What changes relative to the full example:
- `--block 3` overrides the normal block-selection logic
- only block `3` is checked, featurized if needed, and plotted

### Single-Session Selected-Block Manifest

The single-session selected-block manifest for workflow 1 is recreated every time
`run_full_session_pipeline_and_plots.sbatch` runs.

That manifest is generated locally when you run the workflow. It is not checked
into this cleaned repository snapshot.

This is not the same file or role as the multi-session chosen-block manifest.

This workflow 1 manifest is a single-session working manifest used to track:

- selected blocks
- which selected blocks already have local mats
- which selected blocks still need worker jobs
- shared plotting and featurizer settings

## Workflow 2: Multiple Sessions, One Chosen Block Per Session

This workflow is for multiple sessions. It chooses exactly one representative
block in each session and plots that chosen block for every manifest entry.

This is the workflow behind the question:

- "does this do one block per session for multiple sessions?"

Yes. That is exactly what workflow 2 does.

### File Chain

If you run workflow 2 end-to-end, these are the main files involved:

- `spike_plot_pipeline/submit_selected_session_blocks.py`
  - optional manifest builder wrapper

- `spiking_electrode_graph_pipeline/submit_selected_session_blocks.py`
  - actually chooses the sessions
  - actually chooses one block per session
  - writes the multi-session chosen-block manifest:
    `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json`

- `spike_plot_pipeline/run_selected_session_pipeline_and_plots_local.sh`
  - top-level local plotting runner for this workflow

- `spiking_electrode_graph_pipeline/run_selected_session_array_summary_local.sh`
  - checks whether the chosen-block feature mats already exist
  - if some are missing, submits a sparse SLURM array for only those missing
    manifest indices
  - waits for the SLURM job to leave the queue

- `spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch`
  - one worker task per manifest entry
  - each task handles exactly one `(session, chosen_block)` pair
  - runs `../other/session_featurize_to_mat.py`
  - then runs `plot_chunk_mats.py` on the resulting feature file

- `other/session_featurize_to_mat.py`
  - creates the standalone `.mat` or `.npz` feature output for the selected
    block

- `spike_plot_pipeline/plot_selected_session_manifest.py`
  - loops through all manifest entries and plots every chosen block

- `spike_plot_pipeline/plot_chunk_mats.py`
  - writes the final raster, panel, and summary PDF files

### Multi-Session Chosen-Block Manifest

The default multi-session chosen-block manifest path is:

- `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json`

That file is not recreated by:

- `run_selected_session_pipeline_and_plots_local.sh`
- `plot_selected_session_manifest.py`
- `run_selected_session_array_summary_local.sh`

Those scripts only consume the multi-session chosen-block manifest.

The multi-session chosen-block manifest is recreated only when you explicitly run
one of:

- `spike_plot_pipeline/submit_selected_session_blocks.py`
- `spiking_electrode_graph_pipeline/submit_selected_session_blocks.py`

If you generate `selected_session_blocks_manifest.json`, it is a saved snapshot
of one chosen block per session across multiple sessions. It stays unchanged
across plotting runs until you deliberately regenerate it.

### What The Local Runner Actually Does

Typical commands:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/submit_selected_session_blocks.py \
  --bucket exp_sessions_nearline \
  --subject t12 \
  --start-session t12.2025.06.24 \
  --n-sessions 10 \
  --root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other
```

```bash
bash /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/run_selected_session_pipeline_and_plots_local.sh \
  --manifest /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats \
  --plot-root /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots
```

### Full workflow 2 example

To run workflow 2 from the plotting side, use these two commands:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/submit_selected_session_blocks.py \
  --subject t12 \
  --start-session t12.2025.06.24 \
  --root-data /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other \
  --submit

bash /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/run_selected_session_pipeline_and_plots_local.sh \
  --manifest /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats \
  --plot-root /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots
```

What these commands do:
- the first command regenerates the multi-session chosen-block manifest through the graph-pipeline wrapper and optionally submits missing featurization jobs
- the second command ensures chosen-block feature files exist, then plots one chosen block per session

Important defaults in the first command:
- `--bucket` defaults to `exp_sessions_nearline`.
- `--n-sessions` defaults to `10`.
- `--root-derived` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/input_mats`.
- `--repo-dir` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other`.
- `--script-path` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/run_selected_session_blocks.sbatch`.

Important defaults in the second command:
- `--tx-key` defaults to `tx_from_ns5_45`.
- if omitted, `--manifest` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json`.
- if omitted, `--root-derived` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spiking_electrode_graph_pipeline/input_mats`.
- if omitted, `--plot-root` defaults here to `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/output/block_plots`.

### Partial workflow 2 example

If the chosen-block feature files already exist and you only want plots, run:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/plot_selected_session_manifest.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json \
  --root-derived /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats \
  --plot-root /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots
```

What this command does:
- skips featurization entirely
- reads each `(session, chosen_block)` entry from the manifest
- generates plots directly from the existing feature files

The second command does this in order:

1. reads the existing multi-session chosen-block manifest
2. checks whether each chosen block already has a local feature mat
3. if any chosen-block mats are missing, submits
   `run_selected_session_blocks.sbatch` only for those missing entries
4. waits for those worker jobs to finish
5. loops over every manifest entry and plots the chosen block for each session

### What It Does Not Do

Workflow 2 does not:

- process all blocks in one session
- recreate the multi-session chosen-block manifest unless you separately rerun a
  submit
  script
- choose a new block at plotting time

It always plots the block already recorded as `chosen_block` in the manifest you
pass in.

## Data Locations

Workflow 2 typically uses these generated runtime locations:

- multi-session chosen-block manifest:
  - `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/selected_session_blocks_manifest.json`
- chosen-block feature mats:
  - `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spiking_electrode_graph_pipeline/input_mats/<session>/ns5_block_features/`
- plot outputs:
  - `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots/<session>/`

Workflow 1 typically uses these generated runtime locations:

- single-session selected-block manifest:
  - `/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/spike_plot_pipeline/manifests/<session>_parallel_blocks_manifest.json`
- feature mats:
  - `<root-derived>/<session>/ns5_block_features/`
- plot outputs:
  - `/oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/spike_plot_pipeline/output/block_plots/<session>/`

## Summary

- workflow 1 = one session, multiple selected blocks, fresh single-session
  selected-block manifest
- workflow 2 = multiple sessions, one chosen block per session, persistent
  multi-session chosen-block manifest unless explicitly regenerated
- both workflows ultimately use `plot_chunk_mats.py` to create the spike raster,
  spike panel, and summary PDF outputs
