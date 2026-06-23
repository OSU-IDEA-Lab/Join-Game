#!/usr/bin/env python3
"""
Estimator accuracy plotter for ROSL runs.

Reads the merged trajectory.csv produced by tpch_manager.py and generates one
accuracy chart per (query, size, skew) configuration: estimator error vs.
fraction of the true join output sampled.

Line style encodes the shuffle (no colour differentiation between shuffles):
    '--'   Shuffle 1  (dashed)
    ':'    Shuffle 2  (dotted)
    '-.'   Shuffle 3  (dash-dot)
    '-'    Average    (solid black, thicker)

Point colour encodes estimation direction:
    green  overestimate  (rel_error > 0,  est > truth)
    red    underestimate (rel_error <= 0, est <= truth)

The worker's trajectory schema is:

    size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
    mean_per_pair, est_join, truth, ratio, rel_error, elapsed_sec, delta_sec,
    pct_of_truth_output

Usage:
    python3 test/accuracy_charts.py --results_dir 6_18_floor_50 --epsilon_floor 0.5 --limit ON
"""

import argparse
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import pandas as pd
import numpy as np


# Per-query output caps -- must stay in sync with tpch_manager.py / worker.py.
# Used only to LABEL charts (the limit is not stored per row in trajectory.csv).
QUERY_LIMITS = {
    "Q9":  221700,
    "Q10": 13000,
    "Q11": 327624700,
    "Q12": 1000,
    "Q15": 43800,
}

# Shuffle index (0-based) → line style.  Cycles if there are more than 3 shuffles.
_SHUFFLE_STYLES = ['--', ':', '-.']

# Uniform colour for all per-shuffle lines; the style is the differentiator.
_LINE_COLOR = '#444444'

# Point colours for over- / under-estimates.
_OVER_COLOR  = 'green'
_UNDER_COLOR = 'red'


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
    # Read categorical key columns as strings to preserve leading zeros
    # (e.g. size "01" must not collapse to 1 and merge with "1").
    df = pd.read_csv(traj_path, dtype={'size': str, 'zval': str, 'shuffle': str})
    df.columns = [c.strip().lower() for c in df.columns]

    required = ['query', 'zval', 'shuffle', 'pct_of_truth_output', 'rel_error', 'truth']
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"Error: trajectory.csv is missing required columns: {missing}")
        print(f"Found columns: {list(df.columns)}")
        return

    size_present = 'size' in df.columns

    # Drop rows where truth was not recorded (e.g. a failed run).
    df = df[df['truth'] > 0].copy()
    if df.empty:
        print("No rows with a positive truth value; nothing to plot.")
        return

    # Y-axis: absolute relative error as a percentage.  Floor at a tiny positive
    # value so zero-error rows survive a log axis.
    df['err_pct'] = np.maximum(df['rel_error'].abs() * 100.0, 1e-3)
    # X-axis: percent of true output already computed by the worker.
    df['x_pct'] = np.maximum(df['pct_of_truth_output'], 1e-4)

    group_cols = ['query'] + (['size'] if size_present else []) + ['zval']
    grouped = df.groupby(group_cols)
    print(f"Found {len(grouped)} unique configurations to plot.")

    for name, group in grouped:
        gdict = dict(zip(group_cols, name if isinstance(name, tuple) else (name,)))
        q  = str(gdict.get('query', 'Unknown'))
        sz = str(gdict.get('size', '1')) if size_present else '1'
        z  = str(gdict.get('zval', 'Unknown'))
        lim = _limit_label(q, args.limit)

        fig, ax = plt.subplots(figsize=(14, 8))
        ax.grid(True, which="major", ls="-", color='#E0E0E0', zorder=0)
        ax.grid(True, which="minor", ls="-", color='#F5F5F5', zorder=0)

        shuffles = sorted(group['shuffle'].unique())

        # Shared X grid spanning the observed range, used for the averaged line.
        xmin = max(group['x_pct'].min(), 1e-4)
        xmax = min(group['x_pct'].max(), 100.0)
        if xmax <= xmin:
            xmax = xmin * 10
        interp_x  = np.logspace(np.log10(xmin), np.log10(xmax), 1000)
        interp_ys = []

        # ── per-shuffle lines + coloured scatter points ───────────────────
        for i, shuff in enumerate(shuffles):
            s_df = group[group['shuffle'] == shuff].copy().sort_values('x_pct')
            x   = s_df['x_pct'].to_numpy()
            err = s_df['err_pct'].to_numpy()
            rel = s_df['rel_error'].to_numpy()   # signed: >0 over, <=0 under

            lstyle    = _SHUFFLE_STYLES[i % len(_SHUFFLE_STYLES)]
            final_err = err[-1] if len(err) else float('nan')

            # Draw line without markers so the scatter colours read cleanly.
            ax.plot(x, err,
                    color=_LINE_COLOR, linestyle=lstyle, linewidth=1.2,
                    label=f'Shuffle {shuff} (final {final_err:.3g}%)',
                    alpha=0.65, zorder=2)

            # Colour-coded scatter: green = overestimate, red = underestimate.
            over  = rel > 0
            under = ~over
            if over.any():
                ax.scatter(x[over],  err[over],
                           color=_OVER_COLOR,  s=18, alpha=0.75,
                           linewidths=0, zorder=3)
            if under.any():
                ax.scatter(x[under], err[under],
                           color=_UNDER_COLOR, s=18, alpha=0.75,
                           linewidths=0, zorder=3)

            # Interpolate onto shared grid for cross-shuffle averaging.
            xu, idx = np.unique(x, return_index=True)
            if len(xu) > 1:
                erru     = err[idx]
                y_interp = np.interp(interp_x, xu, erru, left=np.nan, right=np.nan)
                interp_ys.append(y_interp)

        # ── averaged line (solid black) ───────────────────────────────────
        if interp_ys:
            stack        = np.vstack(interp_ys)
            col_has_data = np.any(~np.isnan(stack), axis=0)
            y_avg        = np.full(stack.shape[1], np.nan)
            if np.any(col_has_data):
                with np.errstate(invalid='ignore'):
                    y_avg[col_has_data] = np.nanmean(
                        stack[:, col_has_data], axis=0)
            valid = ~np.isnan(y_avg)
            if np.any(valid):
                final_avg = y_avg[valid][-1]
                lbl = f'Average ({len(shuffles)} Shuffles, final {final_avg:.3g}%)'
                ax.plot(interp_x[valid], y_avg[valid], 'k-',
                        linewidth=2.5, label=lbl, zorder=4)

        ax.set_xscale('log')
        ax.set_yscale('log')
        ax.set_xlabel('% of True Output Sampled', fontsize=14)
        ax.set_ylabel('Estimator Error %', fontsize=14)

        size_part = f" on TPC-H {sz}GB" if size_present else ""
        ax.set_title(f"{q} with limit {lim}{size_part}, z={z} / "
                     f"eps_floor={args.epsilon_floor}", fontsize=18)

        # ── legend: shuffle lines then point-colour key ───────────────────
        line_handles, _ = ax.get_legend_handles_labels()

        over_handle = mlines.Line2D(
            [], [], color=_OVER_COLOR,  marker='o', linestyle='None',
            markersize=6, label='Overestimate')
        under_handle = mlines.Line2D(
            [], [], color=_UNDER_COLOR, marker='o', linestyle='None',
            markersize=6, label='Underestimate')

        ax.legend(handles=line_handles + [over_handle, under_handle],
                  loc='lower left', fontsize=12, framealpha=1.0)

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