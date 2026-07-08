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

Marker shape encodes estimation direction (single neutral colour):
    'v'  downward triangle  overestimate  (rel_error > 0,  est > truth)
    '^'  upward triangle    underestimate (rel_error <= 0, est <= truth)

If the trajectory carries a ci_halfwidth column (the engine's self-normalized
95% CI, per round), each shuffle also gets a shaded band from the y-axis floor
up to ci_halfwidth/truth as a percentage: a point inside the band is a round
whose reported CI covers the truth; a point above it is a round whose CI
misses.  Older trajectory.csv files without the column plot without bands.

The worker's trajectory schema is:

    size, query, zval, shuffle, repeat, round, pairs_seen, sample_matches,
    mean_per_pair, est_join, ci_halfwidth, truth, ratio, rel_error,
    elapsed_sec, delta_sec, pct_of_truth_output

Usage Examples:
    python3 test/accuracy_charts.py --results_dir 6_18_floor_50 --epsilon_floor 0.5 --limit ON
    python3 test/accuracy_charts.py --results_dir 7_7_ProbNFailure_HowardCI_ --epsilon_floor 0.2 --limit ON
"""

import argparse
import os
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np


# Per-query output caps: imported from tpch_manager (the single source of
# truth) when it is importable, with a frozen fallback so the plotter still
# works when copied around on its own.
# Used only to LABEL charts (the limit is not stored per row in trajectory.csv).
try:
    import sys as _sys
    _sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from tpch_manager import QUERY_LIMITS
except ImportError:
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

# Markers for over- / under-estimates (shape is the differentiator, one colour).
_OVER_MARKER  = 'v'          # downward triangle: estimate sits above truth
_UNDER_MARKER = '^'          # upward triangle:   estimate sits below truth
_MARKER_COLOR = '#333333'

# Shaded confidence-interval band (per shuffle, from y-floor up to ci/truth %).
_CI_COLOR = '#4C72B0'
_CI_ALPHA = 0.12

# Log-axis floor shared by error points and CI band.
_Y_FLOOR = 1e-3


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
    parser.add_argument('--ci_col', type=str, default=None,
                        help="Trajectory column to use for the CI band "
                             "(default: ci_eb if present, else ci_halfwidth)")
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
    df['err_pct'] = np.maximum(df['rel_error'].abs() * 100.0, _Y_FLOOR)
    # X-axis: percent of true output already computed by the worker.
    df['x_pct'] = np.maximum(df['pct_of_truth_output'], 1e-4)

    # CI band: half-width as a % of truth, if the worker recorded it.  A round
    # whose err_pct falls inside this band is a round whose reported 95% CI
    # covers the truth.
    #
    # Column choice matters: the engine's self-normalized ci_halfwidth is
    # FIXED-HORIZON-ONLY -- per the comments in nodeNestloop.c it is not valid
    # at a data-dependent stop (output LIMIT), which is exactly how the capped
    # sweeps end.  Prefer the anytime-valid empirical-Bernstein interval
    # (ci_eb) when the worker recorded it; --ci_col overrides.
    ci_candidates = ([args.ci_col] if args.ci_col
                     else ['ci_eb', 'ci_halfwidth'])
    ci_col = next((c for c in ci_candidates if c in df.columns), None)
    has_ci = ci_col is not None
    if has_ci:
        df['ci_pct'] = np.maximum(
            df[ci_col] / df['truth'] * 100.0, _Y_FLOOR)
        if ci_col == 'ci_halfwidth':
            print("Note: using fixed-horizon ci_halfwidth for the CI band -- "
                  "this interval is NOT valid at an output-LIMIT stop, so "
                  "expect under-coverage on capped runs.  Re-run with a worker "
                  "that records ci_eb for an anytime-valid band.")
        else:
            print(f"Using {ci_col} for the CI band.")
    else:
        print("Note: no CI column in trajectory.csv -- plotting without "
              "confidence-interval bands (re-run with the updated worker "
              "to capture CIs).")

    n_shuffles_expected = df['shuffle'].nunique()
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
        if len(shuffles) < n_shuffles_expected:
            print(f"  WARNING: {q} z={z} sz={sz} has only {len(shuffles)} "
                  f"shuffle(s) {shuffles} of {n_shuffles_expected} in the file "
                  f"-- a job is missing (see missing_jobs.csv from the manager).")

        # Shared X grid spanning the observed range, used for the averaged line.
        xmin = max(group['x_pct'].min(), 1e-4)
        xmax = group['x_pct'].max()          # may exceed 100% (with-replacement
                                             # matches can outnumber truth, e.g. Q11)
        if xmax <= xmin:
            xmax = xmin * 10
        interp_x  = np.logspace(np.log10(xmin), np.log10(xmax), 1000)
        interp_ys = []

        # ── per-shuffle lines, triangle markers, and CI bands ─────────────
        for i, shuff in enumerate(shuffles):
            s_df = group[group['shuffle'] == shuff].copy().sort_values('x_pct')
            x   = s_df['x_pct'].to_numpy()
            err = s_df['err_pct'].to_numpy()
            rel = s_df['rel_error'].to_numpy()   # signed: >0 over, <=0 under

            lstyle    = _SHUFFLE_STYLES[i % len(_SHUFFLE_STYLES)]
            final_err = err[-1] if len(err) else float('nan')

            # Shaded CI region first (lowest zorder): floor -> ci/truth %.
            # Points inside the band are rounds whose reported CI covers truth.
            if has_ci:
                ci = s_df['ci_pct'].to_numpy()
                xu_ci, idx_ci = np.unique(x, return_index=True)
                if len(xu_ci) > 1:
                    ax.fill_between(xu_ci, _Y_FLOOR, ci[idx_ci],
                                    color=_CI_COLOR, alpha=_CI_ALPHA,
                                    linewidth=0, zorder=1)

            # Draw line without markers so the triangles read cleanly.
            ax.plot(x, err,
                    color=_LINE_COLOR, linestyle=lstyle, linewidth=1.2,
                    label=f'Shuffle {shuff} (final {final_err:.3g}%)',
                    alpha=0.65, zorder=2)

            # Direction-coded markers: 'v' = overestimate, '^' = underestimate.
            over  = rel > 0
            under = ~over
            if over.any():
                ax.scatter(x[over],  err[over],
                           marker=_OVER_MARKER, color=_MARKER_COLOR,
                           s=26, alpha=0.8, linewidths=0, zorder=3)
            if under.any():
                ax.scatter(x[under], err[under],
                           marker=_UNDER_MARKER, color=_MARKER_COLOR,
                           s=26, alpha=0.8, linewidths=0, zorder=3)

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

        # ── legend: shuffle lines, marker-shape key, CI band ──────────────
        line_handles, _ = ax.get_legend_handles_labels()

        over_handle = mlines.Line2D(
            [], [], color=_MARKER_COLOR, marker=_OVER_MARKER, linestyle='None',
            markersize=7, label='Overestimate')
        under_handle = mlines.Line2D(
            [], [], color=_MARKER_COLOR, marker=_UNDER_MARKER, linestyle='None',
            markersize=7, label='Underestimate')
        extra_handles = [over_handle, under_handle]

        if has_ci:
            extra_handles.append(mpatches.Patch(
                color=_CI_COLOR, alpha=_CI_ALPHA * 2.5,
                label=f'Reported 95% CI ({ci_col} / truth)'))

        ax.legend(handles=line_handles + extra_handles,
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