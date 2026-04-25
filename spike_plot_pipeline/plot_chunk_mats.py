#!/usr/bin/env python3
"""
Generate spike-raster and spike-panel PDFs from standalone NS5-derived feature outputs.

Usage examples:
  python3 spike_plot_pipeline/plot_chunk_mats.py \
    /path/to/derived/t12.2025.11.04/ns5_block_features/0.mat \
    /path/to/derived/t12.2025.11.04/ns5_block_features/1.mat

Optional:
  --tx-key tx_from_ns5_45
  --outdir /some/output/folder
"""

import argparse
import os
from typing import Any, Dict, Tuple

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from scipy import io as spio

DEFAULT_TX_KEY = "tx_from_ns5_45"


def load_feature_any(path: str) -> Dict[str, Any]:
    if path.lower().endswith(".npz"):
        with np.load(path, allow_pickle=True) as d:
            return {k: d[k] for k in d.files}

    try:
        d = spio.loadmat(path, squeeze_me=False, struct_as_record=False)
        return {k: v for k, v in d.items() if not k.startswith("__")}
    except NotImplementedError:
        pass
    except ValueError:
        pass

    try:
        import hdf5storage  # type: ignore
        d = hdf5storage.loadmat(path)
        return {k: v for k, v in d.items() if not k.startswith("__")}
    except Exception as e:
        raise RuntimeError(
            f"Could not load feature file: {path}. "
            "If this is a v7.3 MAT, install hdf5storage."
        ) from e


def choose_tx_key(d: Dict[str, Any], requested: str = DEFAULT_TX_KEY) -> str:
    if requested:
        if requested not in d:
            raise KeyError(f"Requested tx key '{requested}' not found.")
        return requested

    tx_keys = sorted([k for k in d.keys() if k.startswith("tx_from_ns5_")])
    if not tx_keys:
        raise KeyError("No tx_from_ns5_* key found in MAT file.")
    return tx_keys[0]


def as_2d_float(x: Any) -> np.ndarray:
    arr = np.asarray(x)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D array, got shape {arr.shape}")
    return arr.astype(np.float32, copy=False)


def crop_valid_chunk(arr: np.ndarray) -> Tuple[np.ndarray, int, int]:
    """
    Crop to rows that contain at least one non-NaN value.
    Returns (cropped_array, start_row, end_row_exclusive).
    """
    valid_rows = ~np.all(np.isnan(arr), axis=1)
    idx = np.flatnonzero(valid_rows)
    if idx.size == 0:
        return arr[:0], 0, 0
    start = int(idx[0])
    end = int(idx[-1]) + 1
    return arr[start:end], start, end


def make_spike_raster(tx: np.ndarray, out_png: str, title: str, bin_ms: float) -> None:
    binary = (np.nan_to_num(tx, nan=0.0) > 0).astype(np.uint8)
    plt.figure(figsize=(14, 5))
    ax = plt.gca()
    ax.imshow(binary, aspect="auto", cmap="binary", interpolation="none", origin="upper")
    plt.xlabel(f"Time bin ({bin_ms:g} ms)")
    plt.ylabel("Channel")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def make_spike_panel(tx: np.ndarray, out_png: str, title: str, bin_ms: float) -> None:
    tx = np.nan_to_num(tx, nan=0.0)
    tx2 = tx

    plt.figure(figsize=(14, 5))
    im = plt.imshow(tx2, aspect="auto", clim=(0, 2))
    plt.colorbar(im, label="Threshold crossings / 20 ms bin")
    plt.xlabel(f"Time bin ({bin_ms:g} ms)")
    plt.ylabel("Channel")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def images_to_pdf(spike_raster_png: str, spike_panel_png: str, out_pdf: str, title: str) -> None:
    import matplotlib.image as mpimg

    with PdfPages(out_pdf) as pdf:
        fig = plt.figure(figsize=(11, 8.5))
        fig.suptitle(title, fontsize=12)

        ax1 = fig.add_subplot(2, 1, 1)
        ax1.axis("off")
        ax1.imshow(mpimg.imread(spike_raster_png))
        ax1.set_title("Spike Raster")

        ax2 = fig.add_subplot(2, 1, 2)
        ax2.axis("off")
        ax2.imshow(mpimg.imread(spike_panel_png))
        ax2.set_title("Spike Panel")

        plt.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)


def process_mat(mat_path: str, outdir: str, tx_key_requested: str = DEFAULT_TX_KEY) -> str:
    d = load_feature_any(mat_path)
    tx_key = choose_tx_key(d, tx_key_requested)
    tx = as_2d_float(d[tx_key])

    # Stored feature arrays are [time x channels]; plots expect [channels x time]
    tx_chunk, start_bin, end_bin = crop_valid_chunk(tx)
    if tx_chunk.shape[0] == 0:
        raise RuntimeError(f"No non-NaN chunk rows found in {mat_path}")

    tx_plot = tx_chunk.T  # [channels x time]

    bin_ms = 20.0
    if "ns5_featurizer_bin_ms" in d:
        try:
            bin_ms = float(np.asarray(d["ns5_featurizer_bin_ms"]).reshape(-1)[0])
        except Exception:
            pass
    if not np.isclose(bin_ms, 20.0):
        raise ValueError(f"Expected 20 ms bins for plotting, found {bin_ms:g} ms in {mat_path}")

    base = os.path.splitext(os.path.basename(mat_path))[0]
    title = f"{base} | {tx_key} | bins {start_bin}:{end_bin} | bin_ms={bin_ms:g}"

    os.makedirs(outdir, exist_ok=True)
    raster_png = os.path.join(outdir, f"{base}_spike_raster.png")
    panel_png = os.path.join(outdir, f"{base}_spike_panel.png")
    out_pdf = os.path.join(outdir, f"{base}_spike_summary.pdf")

    make_spike_raster(tx_plot, raster_png, title, bin_ms)
    make_spike_panel(tx_plot, panel_png, title, bin_ms)
    images_to_pdf(raster_png, panel_png, out_pdf, title)
    return out_pdf


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("mat_files", nargs="+", help="One or more chunk-attached .mat or .npz files")
    ap.add_argument(
        "--tx-key",
        default=DEFAULT_TX_KEY,
        help=f"Specific tx field. Default: {DEFAULT_TX_KEY}",
    )
    ap.add_argument("--outdir", default="", help="Output directory (default: next to each .mat file)")
    args = ap.parse_args()

    for mat_path in args.mat_files:
        mat_path = os.path.abspath(mat_path)
        outdir = args.outdir or os.path.dirname(mat_path)
        pdf_path = process_mat(mat_path, outdir, args.tx_key)
        print("wrote:", pdf_path)


if __name__ == "__main__":
    main()
