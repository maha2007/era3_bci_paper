# NS5 Walkthrough

This folder is a teaching module for understanding an `.ns5` file from the
ground up and then connecting that understanding to a small, explicit feature
extraction flow based on this repository's existing NS5 featurizer code.

The goal is not to hide complexity. The goal is to make the structure visible.

## What You Will Learn

By the end of this walkthrough, you should be able to answer these questions
precisely:

- what is stored in the NS5 header
- where channel order comes from
- how continuous samples are laid out in the data section
- why file order is not necessarily physical electrode layout
- how one simple spike-oriented feature is computed from raw NS5 voltage

## Folder Contents

- `inspect_ns5_headers.py`
  Reads the NS5 header directly with `struct` and prints the file layout.
- `plot_ns5_chunk.py`
  Loads a short chunk and plots raw traces in file-order channel indexing.
- `show_channel_mapping.py`
  Shows how file-order channels relate to physical electrode order for
  128-channel or 256-channel arrays in this repo.
- `simple_spike_features.py`
  Runs a deliberately small feature pipeline using low-level functions from
  `other/ns5_featurizer.py`.

## Prerequisites

Run these scripts from the repository root:

```bash
cd /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper
```

The raw header inspector uses only the Python standard library.

The plotting and feature scripts need:

- `numpy`
- `scipy`
- `matplotlib`

Those scripts also import code from:

- [`other/ns5_featurizer.py`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/ns5_featurizer.py:113)
- [`other/session_featurize_to_mat.py`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/session_featurize_to_mat.py:159)

## Lesson Plan

### Lesson 1: Read the Header Before Touching the Signal

Run:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/inspect_ns5_headers.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other/t12.2025.06.24/Data/NSP_Files/Hub1-20250624-085730-001.ns5 \
  --show-channels 16
```

What to focus on:

- `FileTypeID` and `FileSpec`
- `BytesInHeader`
- `ChannelCount`
- the channel ID or electrode ID list
- where the first data packet begins

Key idea:

The NS5 data matrix is not stored with separate channel files or nested channel
objects. The file has one header that defines channel order, then the data
section stores continuous `int16` samples using that order for every sample
frame.

### Lesson 2: Look at a Real Chunk in File Order

Run:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/plot_ns5_chunk.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other/t12.2025.06.24/Data/NSP_Files/Hub1-20250624-085730-001.ns5 \
  --start-time-s 0 \
  --duration-s 1.0 \
  --channel-indices 0,1,2,3 \
  --heatmap-channels 16 \
  --save-prefix /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/example_chunk
```

What to focus on:

- the x-axis is time
- each trace is one file-order channel
- the heatmap keeps file-order channels on the y-axis

Key idea:

When you load a chunk, you get a matrix with shape:

- rows = time samples
- columns = channels in header order

That is the working representation used by the featurizer.

### Lesson 3: Separate File Order From Physical Array Order

Run:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/show_channel_mapping.py \
  --save-prefix /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/channel_mapping
```

What to focus on:

- file-order index is not the same thing as physical electrode number
- this repo later applies an "unscramble" mapping for 128-channel and
  256-channel arrays

Key idea:

The NS5 file preserves acquisition/header order. A separate mapping is needed
if you want plots or features arranged in physical electrode layout order.

### Lesson 4: Run a Minimal Feature Pipeline You Can Trace Line by Line

Run:

```bash
python3 /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/simple_spike_features.py \
  /oak/stanford/groups/henderj/mahanawaz/data/t12/ns5_featurizer_chunks/other/t12.2025.06.24/Data/NSP_Files/Hub1-20250624-085730-001.ns5 \
  --start-time-s 0 \
  --duration-s 2.0 \
  --channel-index 0 \
  --bin-ms 20 \
  --tx-thresh -4.5 \
  --voltage-scale 4.0 \
  --save-prefix /oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/ns5_walkthrough/simple_feature_demo
```

What this script does:

1. loads a short NS5 chunk
2. picks one channel so the math is easy to inspect
3. scales the raw integer samples into voltage units
4. decimates from 30 kHz to 15 kHz when applicable
5. bandpass filters from 250 Hz to 4900 Hz
6. computes spike-band power in bins
7. computes threshold-crossing counts in the same bins
8. plots every stage

This is intentionally smaller than the full session pipeline, but it uses the
same underlying functions and parameter logic as the repository featurizer.

## The Exact Simple Feature Math

The script in Lesson 4 mirrors code in
[`other/ns5_featurizer.py`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/ns5_featurizer.py:364)
and uses these steps:

### 1. Voltage scaling

If the loaded samples are `raw_int16`, then:

```text
scaled_voltage = raw_int16 * voltage_scale
```

In this walkthrough, `voltage_scale` is set explicitly in the command so there
is no ambiguity.

### 2. Optional decimation

If the sampling rate is 30000 Hz, the repo's spike path first downsamples by 2:

```text
30000 Hz -> 15000 Hz
```

This is the same behavior used in
[`extract_features_from_voltage()`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/ns5_featurizer.py:364).

### 3. Spike-band filter

The spike-oriented signal is:

```text
bandpass 250 Hz to 4900 Hz
```

### 4. Bin width

If `bin_ms = 20` and the spike sampling rate is 15000 Hz:

```text
samples_per_bin = round(0.020 * 15000) = 300
```

### 5. Spike-band power

For each bin and each channel:

```text
spike_band_power = mean(filtered_signal^2)
```

This is exactly the `bin_mean(spike_data ** 2, ...)` step in the featurizer.

### 6. Threshold crossings

For one channel:

```text
threshold = std(filtered_signal) * tx_thresh
```

If `tx_thresh = -4.5`, the threshold is negative. A crossing is counted when the
signal moves from not-below-threshold to below-threshold. The featurizer counts
those rising edges of the boolean event mask inside each bin.

## How This Connects To The Full Repo

This walkthrough is a narrow teaching slice of the full pipeline:

- `inspect_ns5_headers.py` explains what `brpylib.NsxFile` is parsing
- `plot_ns5_chunk.py` shows the matrix that `load_ns5_data()` returns
- `show_channel_mapping.py` explains why
  `session_featurize_to_mat.py` later calls `unscramble_channels()`
- `simple_spike_features.py` shows a readable subset of the logic used by
  `extract_features_from_voltage()`

Once this folder makes sense, the next files to read are:

- [`other/ns5_featurizer.py`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/ns5_featurizer.py:113)
- [`other/session_featurize_to_mat.py`](/oak/stanford/groups/henderj/mahanawaz/era3_bci_paper/other/session_featurize_to_mat.py:789)

## Notes

- Generated `.png` files are ignored by git in this repo.
- Generated `.ns5`, `.mat`, and `.npz` files are also ignored.
- The walkthrough scripts are meant to be read alongside the plots they create.
