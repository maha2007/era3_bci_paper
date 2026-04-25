#!/usr/bin/env python3
"""
Generate synthetic raw voltage traces with known signals encoded into:
  - spike threshold crossings (rate-modulated Poisson spikes)
  - spike-band power (driven by the same spikes)
  - LFP band envelopes (amplitude-modulated sines in specific bands)
  - LMP (slow voltage component)

Then run feature extraction on the synthetic data and quantify recovery via
correlation between extracted features and ground-truth modulators.
"""

import argparse
from typing import Dict, Tuple

import numpy as np

from ns5_featurizer import extract_ns5_features


def _zscore(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=np.float64)
    s = x.std()
    if s == 0:
        return x * 0.0
    return (x - x.mean()) / s


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    a = _zscore(a)
    b = _zscore(b)
    denom = np.sqrt((a * a).sum() * (b * b).sum())
    if denom == 0:
        return 0.0
    return float((a * b).sum() / denom)


def make_spike_waveform(sr_hz: float) -> np.ndarray:
    # Biphasic waveform ~1.2 ms long.
    w_ms = 1.2
    n = int(round((w_ms / 1000.0) * sr_hz))
    n = max(n, 8)
    t = np.arange(n) / sr_hz
    t0 = 0.0004
    t1 = 0.0008
    s0 = 0.00012
    s1 = 0.00018
    wf = -np.exp(-0.5 * ((t - t0) / s0) ** 2) + 0.6 * np.exp(-0.5 * ((t - t1) / s1) ** 2)
    wf /= np.max(np.abs(wf)) + 1e-12
    return wf.astype(np.float32)


def simulate_voltage(
    duration_s: float,
    raw_sr_hz: int,
    n_ch: int,
    seed: int,
    spike_amp_uV: float = 120.0,
    noise_uV: float = 10.0,
    add_noise: bool = False,
    noise_common_lfp_uV: float = 80.0,
    noise_common_spike_uV: float = 40.0,
) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
    rng = np.random.RandomState(seed)
    n = int(round(duration_s * raw_sr_hz))
    t = np.arange(n, dtype=np.float64) / float(raw_sr_hz)

    data = noise_uV * rng.randn(n, n_ch).astype(np.float32)
    gt = {}

    # Ground truth binned timebase is the feature binning; but for simulation we also
    # keep the continuous modulators here and evaluate at bin centers later.
    gt["t_s"] = t

    # LFP bands: amplitude-modulated sine waves placed on distinct channels.
    # Each band gets its own carrier frequency+phase and its own slow modulator
    # so envelope features can be independently assessed.
    lfp_defs = {
        "delta": (2.3, 0),
        "theta": (6.7, 1),
        "beta": (11.2, 2),
        "gamma": (43.5, 3),
    }

    lfp_base_amp = 40.0
    gt["lfp_base_amp_uV"] = np.array([lfp_base_amp], dtype=np.float32)

    for band, (freq, ch) in lfp_defs.items():
        if ch >= n_ch:
            continue
        mod_freq = 0.12 + 0.08 * rng.rand()
        mod_phase = rng.rand() * 2 * np.pi
        amp_mod = 1.0 + 0.8 * np.sin(2 * np.pi * mod_freq * t + mod_phase)

        carrier_phase = rng.rand() * 2 * np.pi
        sig = (lfp_base_amp * amp_mod * np.sin(2 * np.pi * freq * t + carrier_phase)).astype(np.float32)
        data[:, ch] += sig
        gt["lfp_ch_%s" % band] = np.array([ch], dtype=np.int32)
        gt["lfp_carrier_freq_hz_%s" % band] = np.array([freq], dtype=np.float32)
        gt["lfp_carrier_phase_rad_%s" % band] = np.array([carrier_phase], dtype=np.float32)
        gt["lfp_mod_freq_hz_%s" % band] = np.array([mod_freq], dtype=np.float32)
        gt["lfp_mod_phase_rad_%s" % band] = np.array([mod_phase], dtype=np.float32)
        gt["lfp_signal_uV_%s" % band] = sig.astype(np.float32)
        gt["lfp_env_gt_uV_%s" % band] = (lfp_base_amp * amp_mod).astype(np.float32)

    # Add common-mode LFP noise that should be reduced by CAR.
    if add_noise and noise_common_lfp_uV > 0:
        phases = rng.rand(4) * 2 * np.pi
        freqs = np.array([2.0, 6.0, 10.0, 40.0], dtype=np.float64)
        common = (
            np.sin(2 * np.pi * freqs[0] * t + phases[0])
            + 0.8 * np.sin(2 * np.pi * freqs[1] * t + phases[1])
            + 0.6 * np.sin(2 * np.pi * freqs[2] * t + phases[2])
            + 0.4 * np.sin(2 * np.pi * freqs[3] * t + phases[3])
        )
        common = (noise_common_lfp_uV * common).astype(np.float32)
        data += common[:, None]
        gt["noise_common_lfp"] = common

    # LMP: slow component on channel 4.
    if n_ch > 4:
        lmp = 60.0 * np.sin(2 * np.pi * 0.8 * t + 0.3)
        data[:, 4] += lmp.astype(np.float32)
        gt["lmp_ch"] = np.array([4], dtype=np.int32)
        gt["lmp_gt"] = lmp.astype(np.float32)

    # Add common-mode spike-band-ish noise (high frequency) that should be reduced by LRR.
    if add_noise and noise_common_spike_uV > 0:
        phases = rng.rand(3) * 2 * np.pi
        freqs = np.array([900.0, 1700.0, 2600.0], dtype=np.float64)
        common_hf = (
            np.sin(2 * np.pi * freqs[0] * t + phases[0])
            + 0.7 * np.sin(2 * np.pi * freqs[1] * t + phases[1])
            + 0.5 * np.sin(2 * np.pi * freqs[2] * t + phases[2])
        )
        common_hf = (noise_common_spike_uV * common_hf).astype(np.float32)
        data += common_hf[:, None]
        gt["noise_common_spike"] = common_hf

    # Spikes: inhomogeneous Bernoulli approx to Poisson with rate modulated at 0.5 Hz.
    wf = make_spike_waveform(raw_sr_hz)
    wf_len = wf.shape[0]

    rate_mod = 0.5 + 0.5 * np.sin(2 * np.pi * 0.5 * t)  # in [0,1]
    base_rate_hz = 60.0
    rate_hz = base_rate_hz * (0.2 + 0.8 * rate_mod)  # in ~[12,60]
    gt["spike_rate_hz"] = rate_hz.astype(np.float32)

    p = rate_hz / float(raw_sr_hz)
    # Put the spike-encoded signal on channel 0 (others have no spikes) so LRR can
    # remove common noise without regressing away the spike signal itself.
    if n_ch > 0:
        events = rng.rand(n) < p
        idx = np.flatnonzero(events)
        idx = idx[idx < (n - wf_len)]
        for i in idx:
            data[i : i + wf_len, 0] += (spike_amp_uV * wf)

    # Add rare, very large spikes to test LFP blanking.
    rare_rate_hz = 0.5
    p2 = (rare_rate_hz / float(raw_sr_hz)) * np.ones_like(t)
    if n_ch > 0:
        events2 = rng.rand(n) < p2
        idx2 = np.flatnonzero(events2)
        idx2 = idx2[idx2 < (n - wf_len)]
        for i in idx2:
            data[i : i + wf_len, 0] += (6.0 * spike_amp_uV * wf)

    return data, gt


def evaluate_features(feats, gt: Dict[str, np.ndarray], band_to_ch: Dict[str, int]) -> Dict[str, float]:
    t = gt["t_s"]
    bin_t = (feats.bin_times_ms.astype(np.float64) / 1000.0) + (feats.bin_ms / 2000.0)
    bin_t = bin_t[bin_t <= t[-1]]

    def sample_gt(x: np.ndarray) -> np.ndarray:
        return np.interp(bin_t, t, x.astype(np.float64))

    metrics = {}

    for band, ch in band_to_ch.items():
        if band not in feats.lfp_env:
            continue
        if ch >= feats.lfp_env[band].shape[1]:
            continue
        x = feats.lfp_env[band][: bin_t.shape[0], ch]
        gt_key = "lfp_env_gt_uV_%s" % band
        if gt_key not in gt:
            continue
        metrics["corr_env_%s" % band] = _corr(x, sample_gt(gt[gt_key]))

    if "lmp_gt" in gt and "lmp_ch" in gt:
        ch = int(gt["lmp_ch"][0])
        x = feats.lmp[: bin_t.shape[0], ch]
        metrics["corr_lmp"] = _corr(x, sample_gt(gt["lmp_gt"]))

    # TX: use channel 0 (and threshold index 0).
    if feats.binned_tx and feats.binned_tx[0].size:
        tx = feats.binned_tx[0][: bin_t.shape[0], 0].astype(np.float64)
        expected = sample_gt(gt["spike_rate_hz"]) * (feats.bin_ms / 1000.0)
        metrics["corr_tx_rate"] = _corr(tx, expected)

    if feats.spike_band_power.size:
        sbp = feats.spike_band_power[: bin_t.shape[0], 0].astype(np.float64)
        expected = sample_gt(gt["spike_rate_hz"])
        metrics["corr_spikepow_rate"] = _corr(sbp, expected)

    return metrics


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration-s", type=float, default=20.0)
    ap.add_argument("--raw-sr", type=int, default=30000)
    ap.add_argument("--n-ch", type=int, default=8)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--bin-ms", type=float, default=20.0)
    ap.add_argument("--add-noise", action="store_true")
    ap.add_argument("--noise-common-lfp-uv", type=float, default=80.0)
    ap.add_argument("--noise-common-spike-uv", type=float, default=40.0)
    ap.add_argument("--blank-spike-thresh", type=float, default=4.0)
    ap.add_argument("--blank-radius-ms", type=float, default=2.0)
    args = ap.parse_args()

    data, gt = simulate_voltage(
        duration_s=args.duration_s,
        raw_sr_hz=args.raw_sr,
        n_ch=args.n_ch,
        seed=args.seed,
        add_noise=args.add_noise,
        noise_common_lfp_uV=args.noise_common_lfp_uv,
        noise_common_spike_uV=args.noise_common_spike_uv,
    )

    bands = ["delta", "theta", "beta", "gamma"]
    band_to_ch = {
        b: int(gt["lfp_ch_%s" % b][0]) for b in bands if ("lfp_ch_%s" % b) in gt
    }
    chan_sets = [list(range(args.n_ch))]

    runs = [
        ("no_filter", dict(apply_car_filter=False, apply_lrr_filter=False)),
        ("car_only", dict(apply_car_filter=True, apply_lrr_filter=False, car_sets=chan_sets)),
        ("lrr_only", dict(apply_car_filter=False, apply_lrr_filter=True, lrr_sets=chan_sets)),
        (
            "car+lrr",
            dict(
                apply_car_filter=True,
                apply_lrr_filter=True,
                car_sets=chan_sets,
                lrr_sets=chan_sets,
            ),
        ),
    ]

    all_metrics = {}
    for name, opts in runs:
        feats = extract_ns5_features(
            data=data,
            raw_sr=float(args.raw_sr),
            voltage_scale=1.0,
            bin_ms=args.bin_ms,
            tx_thresh=(-4.5,),
            lfp_target_sr=1000.0,
            blank_spike_thresh=args.blank_spike_thresh,
            blank_radius_ms=args.blank_radius_ms,
            **opts
        )
        all_metrics[name] = evaluate_features(feats, gt, band_to_ch)

    print("Simulated feature recovery metrics (higher is better):")
    keys = sorted({k for m in all_metrics.values() for k in m.keys()})
    header = "metric".ljust(22) + "  " + "  ".join(n.ljust(10) for n, _ in runs)
    print(header)
    for k in keys:
        row = k.ljust(22)
        for n, _ in runs:
            v = all_metrics.get(n, {}).get(k, float("nan"))
            row += "  " + ("%0.3f" % v).ljust(10)
        print(row)


if __name__ == "__main__":
    main()
