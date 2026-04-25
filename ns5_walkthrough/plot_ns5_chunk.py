#!/usr/bin/env python3
"""Plot a short NS5 chunk in file-order channel indexing."""

import argparse
import sys
from pathlib import Path


def parse_channel_indices(arg: str) -> list[int]:
    values = []
    for token in arg.split(","):
        token = token.strip()
        if not token:
            continue
        values.append(int(token))
    return values


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ns5_path", help="Path to a .ns5 file")
    parser.add_argument("--start-time-s", type=float, default=0.0)
    parser.add_argument("--duration-s", type=float, default=1.0)
    parser.add_argument(
        "--channel-indices",
        type=str,
        default="0,1,2,3",
        help="Comma-separated 0-based file-order channel indices to plot.",
    )
    parser.add_argument(
        "--heatmap-channels",
        type=int,
        default=16,
        help="Number of leading file-order channels to include in the heatmap.",
    )
    parser.add_argument(
        "--voltage-scale",
        type=float,
        default=1.0,
        help="Optional scale factor applied only for plotting.",
    )
    parser.add_argument(
        "--save-prefix",
        type=str,
        default="",
        help="If set, write <prefix>_traces.png instead of opening an interactive window.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "other"))

    import matplotlib

    if args.save_prefix:
        matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    import numpy as np
    from ns5_featurizer import load_ns5_data

    channel_indices = parse_channel_indices(args.channel_indices)
    data, raw_sr, timestamp = load_ns5_data(
        args.ns5_path,
        start_time_s=args.start_time_s,
        data_time_s=args.duration_s,
    )

    if data.size == 0:
        raise SystemExit("No data were loaded from the requested chunk.")

    n_samples, n_channels = data.shape
    bad = [idx for idx in channel_indices if idx < 0 or idx >= n_channels]
    if bad:
        raise SystemExit(f"Requested channel indices out of range for {n_channels} channels: {bad}")

    time_ms = (np.arange(n_samples) / float(raw_sr)) * 1000.0
    plot_data = np.asarray(data, dtype=np.float32) * float(args.voltage_scale)

    fig, axes = plt.subplots(
        2,
        1,
        figsize=(12, 8),
        constrained_layout=True,
        gridspec_kw={"height_ratios": [2, 1]},
    )

    trace_ax = axes[0]
    offset_step = 1.2 * np.max(np.abs(plot_data[:, channel_indices])) if channel_indices else 1.0
    if not np.isfinite(offset_step) or offset_step == 0:
        offset_step = 1.0

    for row, ch in enumerate(channel_indices):
        offset = row * offset_step
        trace_ax.plot(time_ms, plot_data[:, ch] + offset, linewidth=0.8, label=f"file ch {ch}")

    trace_ax.set_title("Raw NS5 chunk in file-order channel indexing")
    trace_ax.set_xlabel("Time (ms)")
    trace_ax.set_ylabel("Scaled amplitude + offset")
    trace_ax.legend(loc="upper right", fontsize=8)

    heatmap_channels = max(1, min(int(args.heatmap_channels), n_channels))
    heatmap = plot_data[:, :heatmap_channels].T
    axes[1].imshow(
        heatmap,
        aspect="auto",
        origin="lower",
        extent=[time_ms[0], time_ms[-1], 0, heatmap_channels - 1],
        cmap="coolwarm",
    )
    axes[1].set_title(f"Heatmap of the first {heatmap_channels} file-order channels")
    axes[1].set_xlabel("Time (ms)")
    axes[1].set_ylabel("File-order channel index")

    print(f"Loaded shape: {data.shape}")
    print(f"Raw sampling rate: {raw_sr:.6f} Hz")
    print(f"Timestamp from NS5 loader: {timestamp}")
    print(f"Plotted file-order channels: {channel_indices}")

    if args.save_prefix:
        out_path = Path(args.save_prefix).with_suffix("")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(out_path) + "_traces.png", dpi=150)
        print(f"wrote: {out_path}_traces.png")
    else:
        plt.show()


if __name__ == "__main__":
    main()
