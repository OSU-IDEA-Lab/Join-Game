#!/usr/bin/env python3
import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

PAT_MEAN = re.compile(
    "Mean calculation:\s*\(\s*([-+eE\d.]+)\s+([-+eE\d.]+)\s*\)")
PAT_VAR  = re.compile(r"Variance:\s*\(\s*(\d+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s*\)")
PAT_EXP2 = re.compile(r"Exploitation\(\s*([-+eE\d.]+)\s+([-+eE\d.]+)\s*\)")
PAT_EXP4 = re.compile(r"Exploitation\(\s*(\d+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s*\)")
PAT_RIPPLE_MEAN = re.compile(r"\(\s*([-+eE\d.]+)\s*,\s*([-+eE\d.]+)\s*\)")
PAT_RIPPLE_VARIANCE = re.compile(r"\(\s*(\d+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s+([-+eE\d.]+)\s*\)")

METHODS = ["simple", "middle"]
COLORS  = {"simple": "blue", "middle": "red"}
LABELS  = {"simple": "ROSL", "middle": "Ripple"}

def parse_means(filepath: Path):
    by_key = defaultdict(list)
    with filepath.open("r", errors="ignore") as f:
        for line in f:
            m = PAT_MEAN.search(line)
            if m:
                x, y = float(m.group(1)), float(m.group(2))
                if x <= 0.999988:
                    by_key[x].append(y)
                continue
            m2 = PAT_RIPPLE_MEAN.search(line)
            if m2:
                x, y = float(m2.group(1)), float(m2.group(2))
                if x <= 0.999988:
                    by_key[x].append(y)
    return by_key

def parse_ci(filepath: Path):
    by_key = defaultdict(lambda: {"low": [], "mean": [], "high": []})
    with filepath.open("r", errors="ignore") as f:
        for line in f:
            m = PAT_VAR.search(line)
            if m:
                it = float(m.group(1))
                low, mean, high = map(float, m.groups()[1:])
                by_key[it]["low"].append(low)
                by_key[it]["mean"].append(mean)
                by_key[it]["high"].append(high)
                continue
            m2 = PAT_EXP4.search(line)
            if m2:
                it = float(m2.group(1))
                low, mean, high = map(float, m2.groups()[1:])
                by_key[it]["low"].append(low)
                by_key[it]["mean"].append(mean)
                by_key[it]["high"].append(high)
    return by_key

def average_means_per_file(grouped):
    return {k: float(np.mean(v)) for k, v in grouped.items() if v}

def average_ci_per_file(grouped):
    out = {}
    for k, trip in grouped.items():
        out[k] = {
            "low": float(np.mean(trip["low"])) if trip["low"] else np.nan,
            "mean": float(np.mean(trip["mean"])) if trip["mean"] else np.nan,
            "high": float(np.mean(trip["high"])) if trip["high"] else np.nan,
        }
    return out

def average_across_shuffles_means(dicts):
    pool = defaultdict(list)
    for d in dicts:
        for k, v in d.items():
            pool[k].append(v)
    return {k: float(np.mean(v)) for k, v in pool.items()}

def average_across_shuffles_ci(dicts):
    pool = defaultdict(lambda: {"low": [], "mean": [], "high": []})
    for d in dicts:
        for k, v in d.items():
            pool[k]["low"].append(v["low"])
            pool[k]["mean"].append(v["mean"])
            pool[k]["high"].append(v["high"])
    out = {}
    for k, trip in pool.items():
        out[k] = {
            "low": float(np.mean(trip["low"])) if trip["low"] else np.nan,
            "mean": float(np.mean(trip["mean"])) if trip["mean"] else np.nan,
            "high": float(np.mean(trip["high"])) if trip["high"] else np.nan,
        }
    return out

def align_ci_to_output(ci_dict, means_dict):
    xs = sorted(means_dict.keys())
    ks = sorted(ci_dict.keys())
    n = min(len(xs), len(ks))
    aligned = []
    for i in range(n):
        k_ci = ks[i]
        k_x = xs[i]
        trip = ci_dict[k_ci]
        aligned.append((k_x, trip["low"], trip["mean"], trip["high"]))
    return aligned

def plot_means_combined(means_final, out_path, trim=0, title="Mean Error % for Cars Full"):
    plt.figure(figsize=(10, 6))
    for method in METHODS:
        items = sorted(means_final[method].items())
        xs = [k for k, _ in items]
        ys = [v for _, v in items]
        plt.plot(xs, ys, color=COLORS[method], marker="o", label=LABELS[method])
    plt.xlabel("Output %")
    plt.ylabel("Error Rate %")
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.xlim(left=-0.1)
    plt.ylim(bottom=0)
    # plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def plot_ci_dots(aligned_ci, method, out_path, title_suffix="Confidence Interval Triplets (dots only)"):
    xs, ys = [], []
    for x, lo, md, hi in aligned_ci[method]:
        xs.extend([x, x, x])
        ys.extend([lo, md, hi])
    plt.figure(figsize=(10, 5))
    plt.scatter(xs, ys, color=COLORS[method], s=12)
    plt.xlabel("Output %")
    plt.ylabel("Number of Tuples")
    plt.title(f"{LABELS[method]} — {title_suffix}")
    plt.grid(True)
    plt.xlim(left=-0.1)
    plt.ylim(bottom=0)
    # plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def write_csv_means(means_final, out_dir: Path, trim=0):
    for method in METHODS:
        items = sorted(means_final[method].items())
        if trim > 0 and len(items) > trim:
            items = items[trim:]
        p = out_dir / f"{method}_means.csv"
        with p.open("w") as w:
            w.write("output_percent,mean_error\n")
            for x, y in items:
                w.write(f"{x},{y}\n")

def write_csv_ci(aligned_ci, out_dir: Path):
    for method in METHODS:
        p = out_dir / f"{method}_ci_triplets.csv"
        with p.open("w") as w:
            w.write("output_percent,low,mean,high\n")
            for x, lo, md, hi in aligned_ci[method]:
                w.write(f"{x},{lo},{md},{hi}\n")

def main(argv=None):
    parser = argparse.ArgumentParser(description="Aggregate 3 methods (simple/middle/complete) × 3 shuffles of PostgreSQL logs.")
    parser.add_argument("files", nargs=6, help="Nine log files in order: simple1 simple2 simple3 middle1 middle2 middle3 complete1 complete2 complete3")
    parser.add_argument("--outdir", default=".", help="Output directory for PNG/CSV (default: current dir)")
    parser.add_argument("--trim", type=int, default=0, help="Trim first N output% points from mean plot (default: 0)")
    parser.add_argument("--csv", action="store_true", help="Also export CSVs for means and CI")
    args = parser.parse_args(argv)

    paths = [Path(p) for p in args.files]
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    mapping = {
        "simple":   paths[0:3],
        "middle":   paths[3:6]
    }

    per_file_means = {m: [] for m in METHODS}
    per_file_ci    = {m: [] for m in METHODS}
    for method in METHODS:
        for fp in mapping[method]:
            per_file_means[method].append(average_means_per_file(parse_means(fp)))
            per_file_ci[method].append(average_ci_per_file(parse_ci(fp)))

    means_final = {m: average_across_shuffles_means(per_file_means[m]) for m in METHODS}
    ci_final    = {m: average_across_shuffles_ci(per_file_ci[m])      for m in METHODS}
    aligned_ci  = {m: align_ci_to_output(ci_final[m], means_final[m]) for m in METHODS}

    plot_means_combined(means_final, outdir / "mean_error_combined.png", trim=args.trim,
                        title="Mean Error % for cars dataset")
    for m in METHODS:
        plot_ci_dots(aligned_ci, m, outdir / f"ci_triplets_{m}.png")

    if args.csv:
        write_csv_means(means_final, outdir, trim=args.trim)
        write_csv_ci(aligned_ci, outdir)

    print("Wrote:")
    print(outdir / "mean_error_combined.png")
    for m in METHODS:
        print(outdir / f"ci_triplets_{m}.png")
    if args.csv:
        for m in METHODS:
            print(outdir / f"{m}_means.csv")
            print(outdir / f"{m}_ci_triplets.csv")


if __name__ == "__main__":
    sys.exit(main())
