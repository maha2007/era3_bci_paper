#!/usr/bin/env python3
"""Run a minimal, readable spike-oriented feature walkthrough on one NS5 chunk."""

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("ns5_path", help="Path to a .ns5 file")
    parser.add_argument("--start-time-s", type=float, default=0.0)
    parser.add_argument("--duration-s", type=float, default=2.0)
    parser.add_argument(
        "--channel-index",
        type=int,
        default=0,
        help="0-based file-order channel index to inspect in detail.",
    )
    parser.add_argument("--bin-ms", type=float, default=20.0)
    parser.add_argument("--tx-thresh", type=float, default=-4.5)
    parser.add_argument(
        "--voltage-scale",
        type=float,
        default=4.0,
        help="Explicit scale factor applied before filtering.",
    )
    parser.add_argument(
        "--save-prefix",
        type=str,
        default="",
        help="If set, write <prefix>_simple_spike_features.png instead of opening an interactive window.",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root / "other"))

    import matplotlib

    if args.save_prefix:
        matplotlib.use("Agg")

    import matplotlib.pyplot as plt
    import numpy as np
    from ns5_featurizer import (
        bandpass_filter,
        bin_mean,
        compute_binned_tx,
        decimate_data,
        load_ns5_data,
    )

    data, raw_sr, timestamp = load_ns5_data(
        args.ns5_path,
        start_time_s=args.start_time_s,
        data_time_s=args.duration_s,
    )
    if data.size == 0:
        raise SystemExit("No data were loaded from the requested chunk.")
    if args.channel_index < 0 or args.channel_index >= data.shape[1]:
        raise SystemExit(
            f"Requested channel index {args.channel_index} is out of range for {data.shape[1]} channels."
        )

    raw_channel = np.asarray(data[:, args.channel_index], dtype=np.float32)
    scaled_channel = raw_channel * float(args.voltage_scale)

    spike_channel = scaled_channel
    spike_sr = float(raw_sr)
    if int(round(raw_sr)) == 30000:
        spike_channel = decimate_data(spike_channel[:, None], 2)[:, 0]
        spike_sr = 15000.0

    filtered_channel = bandpass_filter(spike_channel[:, None], spike_sr, 250.0, 4900.0)[:, 0]

    samples_per_bin = int(round((float(args.bin_ms) / 1000.0) * spike_sr))
    if samples_per_bin <= 0:
        raise SystemExit("bin-ms is too small for the resulting spike sampling rate.")

    spike_power = bin_mean((filtered_channel[:, None] ** 2), samples_per_bin)[:, 0]
    tx_counts = compute_binned_tx(filtered_channel[:, None], [float(args.tx_thresh)], samples_per_bin)[0][:, 0]

    threshold = float(np.std(filtered_channel, ddof=0) * float(args.tx_thresh))
    event_mask = filtered_channel < threshold if args.tx_thresh < 0 else filtered_channel > threshold

    raw_time_ms = (np.arange(raw_channel.shape[0]) / float(raw_sr)) * 1000.0
    spike_time_ms = (np.arange(filtered_channel.shape[0]) / float(spike_sr)) * 1000.0
    bin_time_ms = np.arange(spike_power.shape[0]) * float(args.bin_ms)

    print(f"Loaded chunk shape: {data.shape}")
    print(f"Original raw sampling rate: {raw_sr:.6f} Hz")
    print(f"NS5 timestamp from loader: {timestamp}")
    print(f"Inspected file-order channel index: {args.channel_index}")
    print(f"Voltage scale applied: {args.voltage_scale}")
    print(f"Spike-path sampling rate after optional decimation: {spike_sr:.6f} Hz")
    print(f"Bandpass range: 250 Hz to 4900 Hz")
    print(f"Bin width: {args.bin_ms} ms")
    print(f"Samples per bin: {samples_per_bin}")
    print(f"Threshold scale: {args.tx_thresh}")
    print(f"Threshold value on filtered channel: {threshold:.6f}")
    print(f"Number of bins: {spike_power.shape[0]}")

    fig, axes = plt.subplots(4, 1, figsize=(12, 12), constrained_layout=True)

    axes[0].plot(raw_time_ms, scaled_channel, linewidth=0.8)
    axes[0].set_title("Raw channel after explicit voltage scaling")
    axes[0].set_xlabel("Time (ms)")
    axes[0].set_ylabel("Scaled amplitude")

    axes[1].plot(spike_time_ms, filtered_channel, linewidth=0.8, color="tab:blue")
    axes[1].axhline(threshold, color="tab:red", linestyle="--", linewidth=1.0)
    axes[1].fill_between(
        spike_time_ms,
        filtered_channel,
        threshold,
        where=event_mask,
        color="tab:red",
        alpha=0.2,
    )
    axes[1].set_title("Spike-band filtered channel with threshold")
    axes[1].set_xlabel("Time (ms)")
    axes[1].set_ylabel("Filtered amplitude")

    axes[2].plot(bin_time_ms, spike_power, marker="o", linewidth=1.0, color="tab:green")
    axes[2].set_title("Spike-band power per bin = mean(filtered^2)")
    axes[2].set_xlabel("Bin start time (ms)")
    axes[2].set_ylabel("Power")

    axes[3].step(bin_time_ms, tx_counts, where="post", color="tab:purple")
    axes[3].set_title("Threshold-crossing counts per bin")
    axes[3].set_xlabel("Bin start time (ms)")
    axes[3].set_ylabel("Count")

    if args.save_prefix:
        out_path = Path(args.save_prefix).with_suffix("")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(str(out_path) + "_simple_spike_features.png", dpi=150)
        print(f"wrote: {out_path}_simple_spike_features.png")
    else:
        plt.show()


if __name__ == "__main__":
    main()
