#!/usr/bin/env python3
"""
Submit `session_featurize_to_mat.py` runs to a SLURM cluster.

Two modes:
  - Per-session: one SLURM job per session
  - Per-block: one SLURM job per (session, block)

Pipeline arguments are passed through after `--` (recommended), e.g.:
  python3 submit_session_featurize_slurm.py \\
    --bucket exp_sessions_nearline \\
    --sessions t20.2026.01.05,t20.2026.01.06 \\
    --root-data /oak/.../t20 --root-derived /oak/.../Derived/t20/nspFeatures \\
    --per-block \\
    --partition hns --time 24:00:00 --mem 64G --cpus 8 \\
    -- --download --apply-lrr --lrr-approx --apply-car --align-by-timestamp
"""

import argparse
import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple


BLOCK_RE = re.compile(r"\((\d+\.?\d*)\)")
_THIS_DIR = Path(__file__).resolve().parent


def _sanitize_job_name(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    s = s.replace(".", "_")
    return s[:180]


def _read_sessions(sessions: Optional[str], sessions_file: Optional[str]) -> List[str]:
    out: List[str] = []
    if sessions:
        out.extend([x.strip() for x in sessions.split(",") if x.strip()])
    if sessions_file:
        p = Path(sessions_file)
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.append(line)
    # unique while preserving order
    seen = set()
    uniq = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _derive_subject(session: str, subject_override: Optional[str]) -> str:
    if subject_override:
        return subject_override
    # session strings are usually like "t20.2026.01.05"
    if "." in session:
        return session.split(".", 1)[0]
    raise ValueError("Could not infer subject from session '%s'; pass --subject." % session)


def _gsutil_ls(gsutil: str, url_glob: str) -> List[str]:
    out = subprocess.check_output([gsutil, "ls", url_glob], universal_newlines=True)
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def _list_blocks(gsutil: str, bucket: str, subject: str, session: str) -> List[str]:
    prefix = "gs://%s/%s/%s" % (bucket, subject, session)
    urls = _gsutil_ls(gsutil, prefix.rstrip("/") + "/**/*.ns5")
    ids: List[str] = []
    for u in urls:
        m = BLOCK_RE.search(u)
        if not m:
            continue
        ids.append(m.group(1))

    def _key(x: str) -> float:
        try:
            return float(x)
        except Exception:
            return 0.0

    return sorted(sorted(set(ids)), key=_key)


def _write_sbatch_script(
    script_path: Path,
    job_name: str,
    command_argv: Sequence[str],
    log_dir: Path,
    workdir: Optional[Path],
    partition: Optional[str],
    time_limit: Optional[str],
    mem: Optional[str],
    cpus: Optional[int],
    account: Optional[str],
    qos: Optional[str],
    constraint: Optional[str],
    extra_sbatch: Sequence[str],
) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    script_path.parent.mkdir(parents=True, exist_ok=True)

    cmd_str = " ".join(shlex.quote(x) for x in command_argv)

    lines = ["#!/usr/bin/env bash"]
    lines.append("#SBATCH --job-name=%s" % job_name)
    lines.append("#SBATCH --output=%s" % str(log_dir / "%x_%j.out"))
    lines.append("#SBATCH --error=%s" % str(log_dir / "%x_%j.err"))
    if partition:
        lines.append("#SBATCH --partition=%s" % partition)
    if time_limit:
        lines.append("#SBATCH --time=%s" % time_limit)
    if mem:
        lines.append("#SBATCH --mem=%s" % mem)
    if cpus:
        lines.append("#SBATCH --cpus-per-task=%d" % int(cpus))
    if account:
        lines.append("#SBATCH --account=%s" % account)
    if qos:
        lines.append("#SBATCH --qos=%s" % qos)
    if constraint:
        lines.append("#SBATCH --constraint=%s" % constraint)
    for x in extra_sbatch:
        x = x.strip()
        if not x:
            continue
        if x.startswith("#SBATCH"):
            lines.append(x)
        else:
            lines.append("#SBATCH " + x)

    lines.extend(
        [
            "",
            "set -euo pipefail",
            "module purge",
            "module load devel",
            "module load math",
            "module load python/3.12.1",
            "module load py-numpy/1.26.3_py312",
            "module load py-scipy/1.12.0_py312",
            "export PYTHONNOUSERSITE=1",
            "if [ -f /home/groups/henderj/fwillett/code/pyt_group_home/bin/activate ]; then",
            "  source /home/groups/henderj/fwillett/code/pyt_group_home/bin/activate",
            "fi",
            "cd %s" % shlex.quote(str(workdir)) if workdir else "",
            "export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK:-1}",
            'echo "Started: $(date)"',
            'echo "Host: $(hostname)"',
            'echo "Workdir: $(pwd)"',
            'echo "Python: $(which python3)"',
            'echo "Python version: $(python3 --version)"',
            'echo "Command: %s"' % cmd_str.replace("\"", "\\\""),
            cmd_str,
            'echo "Finished: $(date)"',
        ]
    )
    script_path.write_text("\n".join(lines) + "\n")


def _submit(script_path: Path) -> str:
    out = subprocess.check_output(["sbatch", str(script_path)], universal_newlines=True).strip()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sessions", type=str, default=None, help="Comma-separated sessions")
    ap.add_argument("--sessions-file", type=str, default=None, help="Text file with one session per line")
    ap.add_argument("--subject", type=str, default=None, help="Override subject (default derived from session prefix)")

    ap.add_argument("--bucket", type=str, default="exp_sessions_nearline")
    ap.add_argument("--gsutil", type=str, default=str(Path("~/google-cloud-sdk/bin/gsutil").expanduser()))

    ap.add_argument("--root-data", type=str, required=True)
    ap.add_argument("--root-derived", type=str, required=True)

    ap.add_argument("--per-block", action="store_true", help="Submit one job per block (requires gsutil ls)")
    ap.add_argument("--blocks", type=str, default="all", help="When not --per-block: 'all' or comma list")

    ap.add_argument(
        "--repo-dir",
        type=str,
        default=str(_THIS_DIR),
        help="Directory containing `session_featurize_to_mat.py` (default: this script's directory).",
    )
    ap.add_argument(
        "--pipeline-script",
        type=str,
        default="session_featurize_to_mat.py",
        help="Path to pipeline script (relative to --repo-dir unless absolute).",
    )
    ap.add_argument("--python", type=str, default="python3")

    ap.add_argument("--script-dir", type=str, default="slurm_scripts")
    ap.add_argument("--log-dir", type=str, default="slurm_logs")
    ap.add_argument("--no-submit", action="store_true", help="Only write scripts, do not call sbatch")

    ap.add_argument("--partition", type=str, default=None)
    ap.add_argument("--time", type=str, default=None)
    ap.add_argument("--mem", type=str, default=None)
    ap.add_argument("--cpus", type=int, default=None)
    ap.add_argument("--account", type=str, default=None)
    ap.add_argument("--qos", type=str, default=None)
    ap.add_argument("--constraint", type=str, default=None)
    ap.add_argument("--extra-sbatch", action="append", default=[], help="Extra SBATCH line (repeatable)")

    ap.add_argument(
        "pipeline_args",
        nargs=argparse.REMAINDER,
        help="Args passed to session_featurize_to_mat.py after `--`",
    )

    args = ap.parse_args()

    sessions = _read_sessions(args.sessions, args.sessions_file)
    if not sessions:
        raise SystemExit("No sessions provided. Use --sessions or --sessions-file.")

    if not os.path.exists(args.gsutil):
        raise SystemExit("gsutil not found at: %s" % args.gsutil)

    # Normalize pipeline_args: if user supplied leading '--', drop it.
    pipeline_args = list(args.pipeline_args)
    if pipeline_args and pipeline_args[0] == "--":
        pipeline_args = pipeline_args[1:]

    script_dir = Path(args.script_dir)
    log_dir = Path(args.log_dir)
    repo_dir = Path(args.repo_dir).expanduser().resolve()
    pipeline_script = Path(args.pipeline_script)
    if not pipeline_script.is_absolute():
        pipeline_script = (repo_dir / pipeline_script).resolve()

    if not pipeline_script.exists():
        raise SystemExit("pipeline script not found at: %s" % str(pipeline_script))

    jobs: List[Tuple[str, Path, List[str]]] = []
    for sess in sessions:
        subj = _derive_subject(sess, args.subject)

        if args.per_block:
            blocks = _list_blocks(args.gsutil, args.bucket, subj, sess)
            if not blocks:
                print("warning: no blocks found for session", sess)
                continue
            for b in blocks:
                job_name = _sanitize_job_name("ns5feat_%s_%s_b%s" % (subj, sess, b))
                cmd = [
                    args.python,
                    str(pipeline_script),
                    "--gsutil",
                    args.gsutil,
                    "--bucket",
                    args.bucket,
                    "--subject",
                    subj,
                    "--session",
                    sess,
                    "--root-data",
                    args.root_data,
                    "--root-derived",
                    args.root_derived,
                    "--blocks",
                    str(b),
                ] + pipeline_args
                sbatch_path = script_dir / (job_name + ".sbatch.sh")
                jobs.append((job_name, sbatch_path, cmd))
        else:
            blocks_arg = args.blocks
            job_name = _sanitize_job_name("ns5feat_%s_%s" % (subj, sess))
            cmd = [
                args.python,
                str(pipeline_script),
                "--gsutil",
                args.gsutil,
                "--bucket",
                args.bucket,
                "--subject",
                subj,
                "--session",
                sess,
                "--root-data",
                args.root_data,
                "--root-derived",
                args.root_derived,
                "--blocks",
                blocks_arg,
            ] + pipeline_args
            sbatch_path = script_dir / (job_name + ".sbatch.sh")
            jobs.append((job_name, sbatch_path, cmd))

    if not jobs:
        raise SystemExit("No jobs to submit.")

    for job_name, sbatch_path, cmd in jobs:
        _write_sbatch_script(
            sbatch_path,
            job_name=job_name,
            command_argv=cmd,
            log_dir=log_dir,
            workdir=repo_dir,
            partition=args.partition,
            time_limit=args.time,
            mem=args.mem,
            cpus=args.cpus,
            account=args.account,
            qos=args.qos,
            constraint=args.constraint,
            extra_sbatch=args.extra_sbatch,
        )

    print("Wrote %d sbatch scripts under %s" % (len(jobs), str(script_dir)))
    if args.no_submit:
        print("Not submitting (use --no-submit disabled to call sbatch).")
        return

    for job_name, sbatch_path, _ in jobs:
        out = _submit(sbatch_path)
        print("[%s] %s" % (job_name, out))


if __name__ == "__main__":
    main()
