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
