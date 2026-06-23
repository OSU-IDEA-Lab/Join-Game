#!/usr/bin/env bash
#
# rosl_probe.sh - diagnose why ROSL phase 1 doesn't finish.
#
# Forces a plain (fixed-inner) nested loop with ROSL on, runs the join with
# LIMIT 1 (which triggers ALL of phase 1 before the first row), splits the
# result row (stdout) from the ROSL INFO notices (stderr), and classifies the
# per-round behavior into the three cases from our discussion.
#
# Usage:
#   ./rosl_probe.sh                 # defaults: Q9 on tpch01g, z0_shuff1, 30s cap
#   DB=tpch1g TIMEOUT=60s ./rosl_probe.sh
#   SCHEMA=z1_shuff2 TA=customer TB=orders PRED='c_custkey = o_custkey' ./rosl_probe.sh
#
set -u

# ── connection / target (override via env) ───────────────────────────────────
PSQL="${PSQL:-/data/jinjo/alt/bin/psql}"
HOST="${HOST:-localhost}"
PORT="${PORT:-1533}"
DB="${DB:-tpch01g}"

# ── query under test (defaults = Q9: partsupp |x| lineitem) ──────────────────
SCHEMA="${SCHEMA:-z0_shuff1}"
TA="${TA:-partsupp}"
TB="${TB:-lineitem}"
PRED="${PRED:-ps_partkey = l_partkey}"

TIMEOUT="${TIMEOUT:-30s}"        # short cap so a stuck phase 1 returns quickly
OUTDIR="${OUTDIR:-/tmp/rosl_probe}"
mkdir -p "$OUTDIR"

PLAN="$OUTDIR/plan.out"
ROWS="$OUTDIR/rows.out"
NOTICES="$OUTDIR/notices.out"

# GUC block: force the plain, fixed-inner nested loop with ROSL on.
read -r -d '' SETS <<'SQL'
SET enable_rosl=on;
SET enable_hashjoin=off;
SET enable_mergejoin=off;
SET enable_indexscan=off;
SET enable_indexonlyscan=off;
SET enable_bitmapscan=off;
SET enable_material=off;
SET enable_block=off;
SET enable_fastjoin=off;
SET enable_fliporder=off;
SET enable_seqscan=off;
SET enable_nestloop=on;
SQL

JOIN="SELECT * FROM ${SCHEMA}.${TA}, ${SCHEMA}.${TB} WHERE ${PRED}"

echo "=================================================================="
echo " ROSL phase-1 probe"
echo "   db=${DB}  schema=${SCHEMA}  query=${TA} |x| ${TB}  on (${PRED})"
echo "   timeout=${TIMEOUT}   out=${OUTDIR}/"
echo "=================================================================="

# ── 1. confirm the plan is a plain nested loop over two seq scans ────────────
echo
echo "--- EXPLAIN (want: Nested Loop over two Seq Scans) ---"
"$PSQL" -h "$HOST" -p "$PORT" -d "$DB" >"$PLAN" 2>&1 <<SQL
${SETS}
EXPLAIN ${JOIN};
SQL
cat "$PLAN"

if grep -qi "Nested Loop" "$PLAN"; then
    nscans=$(grep -ci "Seq Scan" "$PLAN")
    echo ">> plan IS a nested loop (${nscans} seq scan(s))."
else
    echo ">> WARNING: plan is NOT a nested loop -- ROSL won't run. Fix the plan first."
fi

# ── 2. run the join (LIMIT 1) -> triggers all of phase 1; split the streams ──
echo
echo "--- executing (LIMIT 1, ${TIMEOUT} cap); notices -> ${NOTICES} ---"
t0=$(date +%s)
"$PSQL" -h "$HOST" -p "$PORT" -d "$DB" >"$ROWS" 2>"$NOTICES" <<SQL
${SETS}
SET statement_timeout='${TIMEOUT}';
${JOIN} LIMIT 1;
SQL
rc=$?
t1=$(date +%s)
elapsed=$(( t1 - t0 ))

# ── 3. classify ─────────────────────────────────────────────────────────────
rounds=$(grep -c 'ROSL estimate:' "$NOTICES")
maxround=$(grep -oE 'round=[0-9]+' "$NOTICES" | sed 's/round=//' | sort -n | tail -1)
maxround=${maxround:-0}
timedout=$(grep -ci 'statement timeout' "$NOTICES")

echo
echo "--- first rounds ---"
grep 'ROSL estimate:' "$NOTICES" | head -5
echo "--- last rounds ---"
grep 'ROSL estimate:' "$NOTICES" | tail -5

echo
echo "=================================================================="
echo " RESULT"
echo "   psql exit code : ${rc}   elapsed: ${elapsed}s   timed out: $([ "$timedout" -gt 0 ] && echo yes || echo no)"
echo "   rounds emitted : ${rounds}   (max round number = ${maxround})"
if [ "$elapsed" -gt 0 ] && [ "$rounds" -gt 0 ]; then
    echo "   rate           : ~$(( rounds / (elapsed>0?elapsed:1) )) rounds/sec"
fi
echo "------------------------------------------------------------------"

if ! grep -qi "Nested Loop" "$PLAN"; then
    echo " VERDICT: plan is not a plain nested loop -> ROSL never engaged."
    echo "          Re-check the EXPLAIN above before anything else."
elif [ "$rounds" -eq 0 ]; then
    echo " VERDICT: ZERO rounds — phase 1 never completed even one round."
    echo "          Logic bug or stuck before first finalize_round. (case 3)"
elif [ "$maxround" -ge 100000 ]; then
    echo " VERDICT: EXPLODING rounds (>=100k) — block sizing looks wrong;"
    echo "          relations larger than nominal or M_LIM/K_LIM mismatch. (case 2)"
else
    echo " VERDICT: STEADY rounds in the hundreds/low-thousands, each slow —"
    echo "          full-grid cost. Phase 1 walks the whole M x K grid before"
    echo "          emitting; this is the early-stopping-budget fix. (case 1)"
fi
echo "=================================================================="
echo "Files: plan=${PLAN}  rows=${ROWS}  notices=${NOTICES}"