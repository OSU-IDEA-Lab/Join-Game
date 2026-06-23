#!/usr/bin/env python3
"""
Estimator accuracy plotter for ROSL runs.

Reads the merged trajectory.csv produced by tpch_manager.py and generates one
accuracy chart per (query, size, skew) configuration: estimator error vs.
fraction of the true join output sampled, one dotted line per shuffle plus a
solid averaged line.

This version reads the columns the worker actually writes and does NOT recompute
truth or error.  The worker's trajectory schema is:

    size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
    mean_per_pair, est_join, truth, ratio, rel_error, elapsed_sec, delta_sec,
    pct_of_truth_output

So:
  * X-axis  = pct_of_truth_output      (already a percentage, 0..100)
  * Y-axis  = |rel_error| * 100        (rel_error is a signed fraction)
  * truth   = the per-row `truth` column (no JSON lookup, no max-estimate guess)

The LIMIT is not stored per row; it is a label only, supplied via --limit.

Usage:
    python3 test/accuracy_charts.py --results_dir 6_18_floor_50 --epsilon_floor 0.5 --limit ON
"""

import argparse
import os
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


# Per-query output caps -- must stay in sync with tpch_manager.py / worker.py.
# Used only to LABEL charts (the limit is not stored per row in trajectory.csv).
# Each query gets its own cap, so a single global label would be wrong.
QUERY_LIMITS = {
    "Q9":  221700,
    "Q10": 13000,
    "Q11": 327624700,
    "Q12": 1000,
    "Q15": 43800,
}


def _limit_label(q_name, limit_flag):
    """Title/filename label for this query's LIMIT.

    If the sweep was run without limits (--limit OFF), say so.  Otherwise show
    this query's specific cap from QUERY_LIMITS, falling back to the raw flag if
    the query is not in the table.
    """
    if str(limit_flag).upper() in ("OFF", "0", "NONE", "FALSE"):
        return "OFF"
    if q_name in QUERY_LIMITS:
        return str(QUERY_LIMITS[q_name])
    return str(limit_flag)


def main():
    parser = argparse.ArgumentParser(
        description="Generate estimator accuracy charts from ROSL trajectory.csv")
    parser.add_argument('--results_dir', type=str, required=True,
                        help="Directory containing trajectory.csv")
    parser.add_argument('--epsilon_floor', type=str, required=True,
                        help="Epsilon floor value, for the chart title (e.g., 0.5)")
    parser.add_argument('--limit', type=str, default='ON',
                        help="Whether the sweep applied output limits: 'ON' (default) uses "
                             "each query's cap from QUERY_LIMITS for the title; 'OFF' labels "
                             "the run as unlimited. The limit is not stored per row.")
    args = parser.parse_args()
    results_dir = args.results_dir

    traj_path = os.path.join(results_dir, 'trajectory.csv')
    if not os.path.exists(traj_path):
        print(f"Error: {traj_path} not found. Run tpch_manager and merge first.")
        return

    print(f"Loading data from {traj_path}...")
    df = pd.read_csv(traj_path)
    df.columns = [c.strip().lower() for c in df.columns]

    required = ['query', 'zval', 'shuffle', 'pct_of_truth_output', 'rel_error', 'truth']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Error: trajectory.csv is missing required columns: {missing}")
        print(f"Found columns: {list(df.columns)}")
        return

    size_present = 'size' in df.columns

    # Drop rows with no usable truth (e.g. a failed run that wrote nothing useful).
    df = df[df['truth'] > 0].copy()
    if df.empty:
        print("No rows with a positive truth value; nothing to plot.")
        return

    # Y-axis: absolute relative error as a percentage. Floor tiny/zero values so
    # they remain visible on a log scale (a true 0% error would vanish).
    df['err_pct'] = np.maximum(df['rel_error'].abs() * 100.0, 1e-3)
    # X-axis: percent of true output already computed by the worker. Floor at a
    # tiny positive value so the first sample (often ~0%) survives a log axis.
    df['x_pct'] = np.maximum(df['pct_of_truth_output'], 1e-4)

    group_cols = ['query'] + (['size'] if size_present else []) + ['zval']
    grouped = df.groupby(group_cols)
    print(f"Found {len(grouped)} unique configurations to plot.")

    for name, group in grouped:
        gdict = dict(zip(group_cols, name if isinstance(name, tuple) else (name,)))
        q  = str(gdict.get('query', 'Unknown'))
        sz = str(gdict.get('size', '1')) if size_present else '1'
        z  = str(gdict.get('zval', 'Unknown'))
        lim = _limit_label(q, args.limit)  # per-query cap, or "OFF"

        plt.figure(figsize=(14, 8))
        plt.grid(True, which="major", ls="-", color='#E0E0E0', zorder=0)
        plt.grid(True, which="minor", ls="-", color='#F5F5F5', zorder=0)

        colors = ['r', 'g', 'b', 'c', 'm']
        shuffles = sorted(group['shuffle'].unique())

        # Shared X grid spanning the observed range, for the averaged line.
        xmin = max(group['x_pct'].min(), 1e-4)
        xmax = min(group['x_pct'].max(), 100.0)
        if xmax <= xmin:
            xmax = xmin * 10
        interp_x = np.logspace(np.log10(xmin), np.log10(xmax), 1000)
        interp_ys = []

        for i, shuff in enumerate(shuffles):
            s_df = group[group['shuffle'] == shuff].copy()
            s_df = s_df.sort_values('x_pct')
            x = s_df['x_pct'].to_numpy()
            err = s_df['err_pct'].to_numpy()

            # Final (most-converged) error: the error at the largest x, i.e. the
            # estimate after the most output has been sampled.  Shown in the
            # legend so each line's endpoint is readable despite log-log jitter.
            final_err = err[-1] if len(err) else float('nan')

            c = colors[i % len(colors)]
            plt.plot(x, err, f'{c}:', marker='.', markersize=5,
                     label=f'Shuffle {shuff} (final error of {final_err:.3g}%)',
                     alpha=0.5, zorder=2)

            # Interpolate onto the shared grid for averaging across shuffles.
            xu, idx = np.unique(x, return_index=True)
            if len(xu) > 1:
                erru = err[idx]
                y_interp = np.interp(interp_x, xu, erru, left=np.nan, right=np.nan)
                interp_ys.append(y_interp)

        if interp_ys:
            stack = np.vstack(interp_ys)
            # Average only where at least one shuffle has a value; columns that
            # are NaN for every shuffle (outside all shuffles' x-range) are left
            # out instead of triggering a mean-of-empty-slice warning.
            col_has_data = np.any(~np.isnan(stack), axis=0)
            y_avg = np.full(stack.shape[1], np.nan)
            if np.any(col_has_data):
                with np.errstate(invalid='ignore'):
                    y_avg[col_has_data] = np.nanmean(stack[:, col_has_data], axis=0)
            valid = ~np.isnan(y_avg)
            if np.any(valid):
                final_avg = y_avg[valid][-1]   # average error at the largest shared x
                lbl = f'Average ({len(shuffles)} Shuffles, final {final_avg:.3g}%)'
                plt.plot(interp_x[valid], y_avg[valid], 'k-',
                         linewidth=2.5, label=lbl, zorder=3)

        plt.xscale('log')
        plt.yscale('log')
        plt.xlabel('% of True Output Sampled', fontsize=14)
        plt.ylabel('Estimator Error %', fontsize=14)

        size_part = f" on TPC-H {sz}GB" if size_present else ""
        plt.title(f"{q} with limit {lim} {size_part}, z={z} / "
                  f"eps_floor={args.epsilon_floor}", fontsize=18)
        plt.legend(loc='lower left', fontsize=12, framealpha=1.0)
        plt.tight_layout()

        clean_q   = q.replace(' ', '_')
        clean_lim = str(lim).replace(' ', '_')
        size_tag  = f"_sz{sz}" if size_present else ""
        out_name  = f"accuracy_{clean_q}{size_tag}_z{z}_lim{clean_lim}.png"
        out_path  = os.path.join(results_dir, out_name)
        plt.savefig(out_path, dpi=300)
        plt.close()
        print(f"  -> Generated {out_path}")


if __name__ == '__main__':
    main()