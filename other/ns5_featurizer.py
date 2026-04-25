#!/usr/bin/env python3
"""NS5 featurizer: spike-band and LFP features with chunk-aware loading."""

import argparse
from typing import Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
from scipy import signal

class FeatureOutput(object):
    def __init__(
        self,
        bin_ms: float,
        bin_times_ms: np.ndarray,
        spike_band_power: np.ndarray,
        binned_tx: List[np.ndarray],
        tx_thresh: List[float],
        lfp_env: Dict[str, np.ndarray],
        lmp: np.ndarray,
        lfp_band_hz: Dict[str, Tuple[float, float]],
        spike_sr_hz: float,
        lfp_sr_hz: float,
        timestamp: Optional[int],
    ) -> None:
        self.bin_ms = bin_ms
        self.bin_times_ms = bin_times_ms
        self.spike_band_power = spike_band_power
        self.binned_tx = binned_tx
        self.tx_thresh = tx_thresh
        self.lfp_env = lfp_env
        self.lmp = lmp
        self.lfp_band_hz = lfp_band_hz
        self.spike_sr_hz = spike_sr_hz
        self.lfp_sr_hz = lfp_sr_hz
        self.timestamp = timestamp


def _as_list(x):
    if isinstance(x, list):
        return x
    return [x]


def _parse_channel_sets(arg: Optional[str], n_ch: int) -> List[List[int]]:
    if not arg or arg.lower() == "all":
        return [list(range(n_ch))]

    sets = []
    for part in arg.split(";"):
        part = part.strip()
        if not part:
            continue
        chans = []
        for token in part.split(","):
            token = token.strip()
            if "-" in token:
                start_s, end_s = token.split("-", 1)
                start = int(start_s)
                end = int(end_s)
                chans.extend(list(range(start - 1, end)))
            else:
                chans.append(int(token) - 1)
        chans = [c for c in chans if 0 <= c < n_ch]
        if chans:
            sets.append(chans)
    return sets if sets else [list(range(n_ch))]


def _concat_ns5_chunks(all_dat: dict, fill_value: float = 0.0) -> Tuple[np.ndarray, float, Optional[int]]:
    """Concatenate BRPy NSx chunks and pad gaps with zeros when timestamps jump."""
    data_list = _as_list(all_dat["data"])
    header_list = _as_list(all_dat["data_headers"])
    sr = float(all_dat["samp_per_s"])

    concat = []
    prev_last_ts = None
    dt_ticks = None
    timestamp = None

    for idx, (chunk, header) in enumerate(zip(data_list, header_list)):
        if chunk.size == 0:
            continue

        if timestamp is None and "Timestamp" in header:
            ts_val = np.asarray(header["Timestamp"])
            if ts_val.size:
                timestamp = int(ts_val.ravel()[0])

        ts = np.asarray(header.get("Timestamp", []))
        if ts.size >= 2:
            local_dt = float(np.median(np.diff(ts)))
            if dt_ticks is None:
                dt_ticks = local_dt

        if idx > 0 and prev_last_ts is not None and ts.size and dt_ticks:
            gap_ticks = float(ts.ravel()[0] - prev_last_ts - dt_ticks)
            gap_samples = int(round(gap_ticks / dt_ticks))
            if gap_samples > 0:
                n_ch = chunk.shape[0]
                concat.append(np.full((gap_samples, n_ch), fill_value, dtype=chunk.dtype))

        concat.append(chunk.T)

        if ts.size:
            prev_last_ts = float(ts.ravel()[-1])

    if not concat:
        return np.empty((0, 0)), sr, timestamp

    return np.vstack(concat), sr, timestamp


def load_ns5_data(
    ns5_path: str,
    start_time_s: float = 0.0,
    data_time_s: Union[str, float] = "all",
    downsample: int = 1,
    initial_decimate_to_hz: Optional[float] = None,
    elec_ids: Union[Sequence[int], str] = "all",
) -> Tuple[np.ndarray, float, Optional[int]]:
    # Import lazily so simulation/tests can run without BRPy installed.
    from brpylib import NsxFile

    nsx_file = NsxFile(ns5_path)
    all_dat = nsx_file.getdata(
        elec_ids, start_time_s, data_time_s, downsample, full_timestamps=True
    )
    nsx_file.close()
    data, raw_sr, timestamp = _concat_ns5_chunks(all_dat)

    if initial_decimate_to_hz is not None:
        target_fs = float(initial_decimate_to_hz)
        if target_fs <= 0:
            raise ValueError("initial_decimate_to_hz must be positive")
        if target_fs > raw_sr:
            raise ValueError(
                f"initial_decimate_to_hz={target_fs} exceeds loaded sample rate {raw_sr}"
            )
        data, raw_sr = downsample_to_target(data, raw_sr, target_fs)

    return data, raw_sr, timestamp


def bandpass_filter(
    data: np.ndarray, fs: float, low_hz: float, high_hz: float
) -> np.ndarray:
    nyq = fs / 2.0
    high_hz = min(high_hz, nyq * 0.99)
    low = low_hz / nyq
    high = high_hz / nyq
    if high <= low:
        raise ValueError(f"Invalid bandpass range {low_hz}-{high_hz} Hz for fs={fs}")
    sos = signal.butter(4, [low, high], btype="bandpass", output="sos")
    return signal.sosfiltfilt(sos, data, axis=0)


def lowpass_filter(data: np.ndarray, fs: float, cutoff_hz: float) -> np.ndarray:
    nyq = fs / 2.0
    cutoff = min(cutoff_hz, nyq * 0.99) / nyq
    sos = signal.butter(4, cutoff, btype="low", output="sos")
    return signal.sosfiltfilt(sos, data, axis=0)


def decimate_data(data: np.ndarray, factor: int) -> np.ndarray:
    # Fast polyphase downsampling; introduces a constant group delay (not zero-phase).
    # BRPy returns raw NS5 voltage as int16. Resample in float so polyphase
    # filtering does not collapse low-amplitude integer input to zeros.
    data_f = np.asarray(data, dtype=np.float32)
    out = signal.resample_poly(data_f, up=1, down=factor, axis=0)
    return out.astype(np.float32, copy=False)


def apply_car(data: np.ndarray, chan_sets: List[List[int]]) -> np.ndarray:
    out = data.copy()
    n_ch = data.shape[1]
    for chans in chan_sets:
        chans = [c for c in chans if 0 <= c < n_ch]
        if not chans:
            continue
        ref = np.mean(out[:, chans], axis=1, keepdims=True)
        out[:, chans] = out[:, chans] - ref
    return out


def apply_lrr(
    data: np.ndarray, chan_sets: List[List[int]]
) -> Tuple[np.ndarray, List[np.ndarray]]:
    # `apply_lrr_approx(..., ridge=0)` computes the same OLS solution via
    # sufficient statistics, but avoids materializing a huge T x (C-1) design
    # matrix for every target channel.
    return apply_lrr_approx(data, chan_sets, ridge=0.0)


def apply_lrr_approx(
    data: np.ndarray, chan_sets: List[List[int]], ridge: float = 0.0, block_size: Optional[int] = None
) -> Tuple[np.ndarray, List[np.ndarray]]:
    """
    Fast LRR via sufficient statistics (normal equations), without reducing rank.

    For a channel set Y (T x C), for each target channel i we want OLS:
      beta_i = argmin || y_i - Y_{-i} beta ||^2
    OLS depends only on:
      G_i = X_i^T X_i   and   b_i = X_i^T y_i
    where X_i = Y with column i removed.

    We compute the full Gram matrix once:
      K = Y^T Y   (C x C)
    (optionally in blocks to reduce peak memory), then use the identity:
      beta_i = -P_{-i,i} / P_{i,i}   where   P = (K + ridge*I)^{-1}
    The diagonal constraint beta_i[i]=0 is handled by construction.

    Notes:
    - With ridge=0 this matches the normal-equation solution; small ridge can help
      if K is ill-conditioned/singular (but then it becomes ridge regression).
    - This avoids per-channel T x (C-1) least-squares solves and is typically much
      faster when T is large.
    """
    out = data.copy()
    n_ch = data.shape[1]
    coeffs = []

    for chans in chan_sets:
        chans = [c for c in chans if 0 <= c < n_ch]
        if len(chans) < 2:
            coeffs.append(np.empty((0, 0)))
            continue

        y = out[:, chans].astype(np.float64, copy=False)  # [T x Cset]
        cset = y.shape[1]

        if block_size and int(block_size) > 0 and y.shape[0] > int(block_size):
            bs = int(block_size)
            k_mat = np.zeros((cset, cset), dtype=np.float64)
            for start in range(0, y.shape[0], bs):
                blk = y[start : start + bs]
                k_mat += blk.T.dot(blk)
        else:
            k_mat = y.T.dot(y)

        if ridge and ridge != 0.0:
            k_mat = k_mat + float(ridge) * np.eye(cset, dtype=np.float64)

        try:
            p = np.linalg.inv(k_mat)
        except np.linalg.LinAlgError:
            p = np.linalg.pinv(k_mat)

        diag_p = np.diag(p).copy()
        w = np.zeros((cset, cset), dtype=np.float64)
        for i in range(cset):
            den = diag_p[i]
            if den == 0:
                continue
            w[:, i] = -p[:, i] / den
            w[i, i] = 0.0

        noise = y.dot(w)
        out[:, chans] = (y - noise).astype(out.dtype, copy=False)

        # Match MATLAB usage: noise = data * coef'  => coef' == W
        coeffs.append(w.T.astype(np.float32))

    return out, coeffs


def interpolate_masked_samples(data: np.ndarray, mask: np.ndarray) -> np.ndarray:
    out = data.copy()
    for ch in range(out.shape[1]):
        bad = mask[:, ch]
        if not np.any(bad):
            continue
        good_idx = np.flatnonzero(~bad)
        if good_idx.size < 2:
            out[bad, ch] = 0.0
            continue
        bad_idx = np.flatnonzero(bad)
        out[bad_idx, ch] = np.interp(bad_idx, good_idx, out[good_idx, ch])
    return out


def bin_mean(data: np.ndarray, n_samples_per_bin: int) -> np.ndarray:
    n_bins = data.shape[0] // n_samples_per_bin
    use_len = n_bins * n_samples_per_bin
    if n_bins == 0:
        return np.empty((0, data.shape[1]))
    reshaped = data[:use_len].reshape(n_bins, n_samples_per_bin, data.shape[1])
    return reshaped.mean(axis=1)


def map_spike_mask_to_target_sr(
    spike_mask: np.ndarray,
    spike_sr_hz: float,
    target_len: int,
    target_sr_hz: float,
    blank_radius_samples: int = 0,
) -> np.ndarray:
    c = spike_mask.shape[1]
    out = np.zeros((target_len, c), dtype=bool)

    ratio = spike_sr_hz / target_sr_hz
    if abs(ratio - round(ratio)) < 1e-6 and ratio >= 1:
        ratio_i = int(round(ratio))
        n = min(target_len, spike_mask.shape[0] // ratio_i)
        if n > 0:
            mapped = spike_mask[: n * ratio_i].reshape(n, ratio_i, c).any(axis=1)
            out[:n] = mapped
    else:
        scale = target_sr_hz / spike_sr_hz
        for ch in range(c):
            idx = np.flatnonzero(spike_mask[:, ch])
            if idx.size == 0:
                continue
            mapped = np.unique(np.rint(idx * scale).astype(np.int64))
            mapped = mapped[(mapped >= 0) & (mapped < target_len)]
            out[mapped, ch] = True

    r = int(blank_radius_samples)
    for shift in range(1, r + 1):
        out[shift:] |= out[:-shift]
        out[:-shift] |= out[shift:]

    return out


def compute_binned_tx(
    data: np.ndarray, tx_thresh: Sequence[float], n_samples_per_bin: int
) -> List[np.ndarray]:
    n_bins = data.shape[0] // n_samples_per_bin
    if n_bins <= 0:
        return [np.empty((0, data.shape[1]), dtype=np.uint32) for _ in tx_thresh]

    use_len = n_bins * n_samples_per_bin
    std = np.std(data, axis=0, ddof=0)

    out = []
    for thresh_scale in tx_thresh:
        thresh = std * thresh_scale
        if thresh_scale < 0:
            below = data < thresh
        else:
            below = data > thresh

        # Rising edges. `edges[t]` corresponds to the transition from sample t -> t+1.
        edges = below[1:] & ~below[:-1]  # [T-1 x C]
        edges = edges[: max(use_len - 1, 0)]
        if edges.shape[0] < use_len:
            edges = np.concatenate(
                [edges, np.zeros((use_len - edges.shape[0], data.shape[1]), dtype=bool)],
                axis=0,
            )
        counts = (
            edges.reshape(n_bins, n_samples_per_bin, data.shape[1])
            .sum(axis=1)
            .astype(np.uint32)
        )
        out.append(counts)

    return out


def downsample_to_target(data: np.ndarray, fs: float, target_fs: float) -> Tuple[np.ndarray, float]:
    if fs == target_fs:
        return data, fs
    ratio = fs / target_fs
    if ratio.is_integer():
        dec_factor = int(ratio)
        return decimate_data(data, dec_factor), target_fs
    target_int = int(round(target_fs))
    fs_int = int(round(fs))
    resampled = signal.resample_poly(data, target_int, fs_int, axis=0)
    return resampled.astype(data.dtype, copy=False), float(target_int)


def extract_features_from_voltage(
    data: np.ndarray,
    raw_sr: float,
    bin_ms: float = 20.0,
    tx_thresh: Sequence[float] = (-4.5,),
    voltage_scale: float = 0.25,
    apply_car_filter: bool = False,
    car_sets: Optional[List[List[int]]] = None,
    apply_lrr_filter: bool = False,
    lrr_sets: Optional[List[List[int]]] = None,
    lrr_approx: bool = False,
    lrr_ridge: float = 0.0,
    lrr_block_size: Optional[int] = None,
    lfp_target_sr: float = 1000.0,
    blank_spike_thresh: float = 4.0,
    blank_radius_ms: float = 0.0,
    lfp_band_hz: Optional[Dict[str, Tuple[float, float]]] = None,
    lmp_lowpass_hz: float = 5.0,
    timestamp: Optional[int] = None,
) -> FeatureOutput:
    data = data.astype(np.float32) * voltage_scale

    if data.size == 0:
        raise ValueError("No data loaded from ns5 file.")

    #--spike--
    spike_data = data
    spike_sr = raw_sr
    if int(raw_sr) == 30000:
        spike_data = decimate_data(spike_data, 2)
        spike_sr = 15000.0

    spike_data = bandpass_filter(spike_data, spike_sr, 250.0, 4900.0)

    lrr_coeffs = []
    if apply_lrr_filter:
        if lrr_sets is None:
            lrr_sets = [list(range(spike_data.shape[1]))]
        if lrr_approx:
            spike_data, lrr_coeffs = apply_lrr_approx(
                spike_data,
                lrr_sets,
                ridge=lrr_ridge,
                block_size=lrr_block_size,
            )
        else:
            spike_data, lrr_coeffs = apply_lrr(spike_data, lrr_sets)

    spike_bin_samp = int(round((bin_ms / 1000.0) * spike_sr))
    spike_power = bin_mean(spike_data ** 2, spike_bin_samp)
    binned_tx = compute_binned_tx(spike_data, tx_thresh, spike_bin_samp)

    #--lfp--
    # Downsample early for speed; blank spike-times on this target-sample-rate time base
    # before any LFP band filtering so spikes do not bleed into the LFP features.
    lfp_data, lfp_sr = downsample_to_target(data, raw_sr, lfp_target_sr)

    if blank_spike_thresh is not None and blank_spike_thresh > 0:
        spike_std = np.std(spike_data, axis=0, ddof=0)
        spike_mask = np.abs(spike_data) > (blank_spike_thresh * spike_std)

        blank_radius_samples = int(round((blank_radius_ms / 1000.0) * lfp_sr))
        lfp_mask = map_spike_mask_to_target_sr(
            spike_mask,
            spike_sr_hz=spike_sr,
            target_len=lfp_data.shape[0],
            target_sr_hz=lfp_sr,
            blank_radius_samples=blank_radius_samples,
        )
        lfp_data = interpolate_masked_samples(lfp_data, lfp_mask)

    if apply_car_filter:
        if car_sets is None:
            car_sets = [list(range(lfp_data.shape[1]))]
        lfp_data = apply_car(lfp_data, car_sets)

    lmp = lowpass_filter(lfp_data, lfp_sr, lmp_lowpass_hz)
    lmp_binned = bin_mean(lmp, int(round((bin_ms / 1000.0) * lfp_sr))).astype(np.float32)

    if lfp_band_hz is None:
        lfp_band_hz = {
            "delta": (1.0, 4.0),
            "theta": (4.0, 8.0),
            "beta": (8.0, 12.0),
            "gamma": (30.0, 80.0),
        }

    lfp_env = {}
    for name, (low_hz, high_hz) in lfp_band_hz.items():
        if high_hz >= lfp_sr / 2.0:
            raise ValueError(
                f"Band {name} upper edge {high_hz} exceeds Nyquist {lfp_sr/2.0} Hz"
            )
        bp = bandpass_filter(lfp_data, lfp_sr, low_hz, high_hz)
        env = np.abs(signal.hilbert(bp, axis=0))
        lfp_env[name] = bin_mean(env, int(round((bin_ms / 1000.0) * lfp_sr))).astype(
            np.float32
        )

    size_candidates = [
        spike_power.shape[0] if spike_power.size else 0,
        lmp_binned.shape[0] if lmp_binned.size else 0,
    ]
    size_candidates.extend(arr.shape[0] for arr in lfp_env.values())
    n_bins_common = min(size_candidates) if size_candidates else 0
    if n_bins_common == 0:
        bin_times_ms = np.empty((0,))
    else:
        bin_times_ms = np.arange(n_bins_common) * bin_ms

    def _trim(arr: np.ndarray) -> np.ndarray:
        return arr[:n_bins_common] if arr.size else arr

    spike_power = _trim(spike_power).astype(np.float32)
    lmp_binned = _trim(lmp_binned).astype(np.float32)
    lfp_env = {k: _trim(v) for k, v in lfp_env.items()}
    binned_tx = [_trim(x) for x in binned_tx]

    return FeatureOutput(
        bin_ms=bin_ms,
        bin_times_ms=bin_times_ms.astype(np.float32),
        spike_band_power=spike_power,
        binned_tx=binned_tx,
        tx_thresh=list(tx_thresh),
        lfp_env=lfp_env,
        lmp=lmp_binned,
        lfp_band_hz=lfp_band_hz,
        spike_sr_hz=spike_sr,
        lfp_sr_hz=lfp_sr,
        timestamp=timestamp,
    )


def extract_ns5_features(
    ns5_path: Optional[str] = None,
    data: Optional[np.ndarray] = None,
    raw_sr: Optional[float] = None,
    timestamp: Optional[int] = None,
    start_time_s: float = 0.0,
    data_time_s: Union[str, float] = "all",
    downsample: int = 1,
    initial_decimate_to_hz: Optional[float] = None,
    elec_ids: Union[Sequence[int], str] = "all",
    **kwargs
) -> FeatureOutput:
    """
    Convenience wrapper:
      - Pass `ns5_path=...` to load from disk via BRPy.
      - Or pass `data` (T x C) and `raw_sr` to featurize in-memory voltage.

    New optional chunking arguments (backward compatible):
      - start_time_s: where to start loading from the NS5
      - data_time_s: duration to load in seconds, or "all"
      - downsample / elec_ids: passed through to BRPy loading
    """
    if data is None:
        if not ns5_path:
            raise ValueError("Provide either `ns5_path` or `data`+`raw_sr`.")
        data_loaded, raw_sr_loaded, ts_loaded = load_ns5_data(
            ns5_path,
            start_time_s=start_time_s,
            data_time_s=data_time_s,
            downsample=downsample,
            initial_decimate_to_hz=initial_decimate_to_hz,
            elec_ids=elec_ids,
        )
        return extract_features_from_voltage(
            data_loaded, raw_sr_loaded, timestamp=ts_loaded, **kwargs
        )

    if raw_sr is None:
        raise ValueError("When providing `data`, you must also provide `raw_sr`.")
    return extract_features_from_voltage(data, raw_sr, timestamp=timestamp, **kwargs)


def _save_features_npz(out_path: str, feats: FeatureOutput) -> None:
    np.savez_compressed(
        out_path,
        bin_ms=feats.bin_ms,
        bin_times_ms=feats.bin_times_ms,
        spike_band_power=feats.spike_band_power,
        binned_tx=np.array(feats.binned_tx, dtype=object),
        tx_thresh=np.array(feats.tx_thresh, dtype=np.float32),
        lfp_env=feats.lfp_env,
        lmp=feats.lmp,
        lfp_band_hz=feats.lfp_band_hz,
        spike_sr_hz=feats.spike_sr_hz,
        lfp_sr_hz=feats.lfp_sr_hz,
        timestamp=feats.timestamp,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extract binned features from ns5 files.")
    parser.add_argument("ns5_path", help="Path to .ns5 file")
    parser.add_argument("--bin-ms", type=float, default=20.0)
    parser.add_argument("--tx-thresh", type=str, default="-4.5")
    parser.add_argument("--voltage-scale", type=float, default=0.25)
    parser.add_argument("--apply-car", action="store_true")
    parser.add_argument("--car-sets", type=str, default=None)
    parser.add_argument("--apply-lrr", action="store_true")
    parser.add_argument("--lrr-sets", type=str, default=None)
    parser.add_argument("--lfp-target-sr", type=float, default=1000.0)
    parser.add_argument("--blank-spike-thresh", type=float, default=4.0)
    parser.add_argument(
        "--blank-radius-ms",
        type=float,
        default=0.0,
        help="Dilate spike-blanking mask by this radius (ms) on the LFP sample-rate grid.",
    )
    parser.add_argument("--start-time-s", type=float, default=0.0)
    parser.add_argument(
        "--data-time-s",
        type=str,
        default="all",
        help="Duration in seconds to load from the NS5, or 'all' for full file.",
    )
    parser.add_argument("--output", type=str, default="features.npz")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    tx_thresh = [float(x) for x in args.tx_thresh.split(",") if x.strip()]

    data_time_s: Union[str, float]
    if str(args.data_time_s).lower() == "all":
        data_time_s = "all"
    else:
        data_time_s = float(args.data_time_s)

    car_sets = None
    lrr_sets = None
    if args.car_sets or args.lrr_sets:
        sample_data, _, _ = load_ns5_data(
            args.ns5_path,
            start_time_s=args.start_time_s,
            data_time_s=data_time_s,
        )
        n_ch = sample_data.shape[1]
        if args.car_sets:
            car_sets = _parse_channel_sets(args.car_sets, n_ch)
        if args.lrr_sets:
            lrr_sets = _parse_channel_sets(args.lrr_sets, n_ch)

    feats = extract_ns5_features(
        args.ns5_path,
        start_time_s=args.start_time_s,
        data_time_s=data_time_s,
        bin_ms=args.bin_ms,
        tx_thresh=tx_thresh,
        voltage_scale=args.voltage_scale,
        apply_car_filter=args.apply_car,
        car_sets=car_sets,
        apply_lrr_filter=args.apply_lrr,
        lrr_sets=lrr_sets,
        lfp_target_sr=args.lfp_target_sr,
        blank_spike_thresh=args.blank_spike_thresh,
        blank_radius_ms=args.blank_radius_ms,
    )

    _save_features_npz(args.output, feats)

if __name__ == "__main__":
    main()
