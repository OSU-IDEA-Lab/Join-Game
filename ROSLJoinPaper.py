import math
import random

class ROSL:
    def __init__(self, exploration_size, exploitation_size, n_failure_constant,
                 file_r, key_r, file_s, key_s):
        self.exploration_size   = exploration_size
        self.exploitation_size  = exploitation_size # L in the paper
        self.n_failure_constant = n_failure_constant

        # round-specific info
        self.exploration_cache  = [None] * exploration_size
        self.reward             = [0]    * exploration_size
        self.failure            = [0]    * exploration_size
        self.curr_exploration_loaded = 0

        self.round_number = 0
        self.agg_exploration_loaded = 0

        self.size_r = 0
        self.size_s = 0

        self.file_r = file_r
        self.key_r  = key_r
        self.file_s = file_s
        self.key_s  = key_s

        self.T = 0
        self.ispw_num   = [0.0] * exploration_size
        self.ispw_den   = [0.0] * exploration_size
        self.tries      = [0]   * exploration_size
        self.global_num = 0.0
        self.global_den = 0.0

    def run_round(self, R_iter, probe_relation, results):
        # Create one iterator for the entire round to ensure no reconsumption
        S_iter = iter(probe_relation)

        self.exploration(R_iter, S_iter, results)
        self.update_estimate_exploration()

        # Exploitation picks up exactly where the S_iter was left by exploration
        self.exploitation(R_iter, S_iter, results)

        self.round_number += 1
        self.agg_exploration_loaded = self.curr_exploration_loaded
        self.reset_between_rounds()

    def reset_between_rounds(self):
        self.exploration_cache  = [None] * self.exploration_size
        self.reward             = [0]    * self.exploration_size
        self.failure            = [0]    * self.exploration_size
        self.curr_exploration_loaded = 0
        n = self.exploration_size
        self.ispw_num = [0.0] * n
        self.ispw_den = [0.0] * n
        self.tries    = [0]   * n

    def exploration(self, R_iter, S_iter, results):
        for i in range(self.exploration_size):
            try:
                self.exploration_cache[i] = next(R_iter)
                self.curr_exploration_loaded += 1
            except StopIteration:
                break

        # ── Duplicate removal ────────────────────────────────────────────────
        # After the cache fills, scan every pair of loaded slots for duplicate
        # rows (identical dict content). Any duplicate slot is replaced with
        # the next tuple from R_iter using the same next(R_iter) mechanism as
        # the original fill. Repeat until a full O(n²) pass finds no duplicates
        # or R is exhausted.
        r_exhausted = False
        while not r_exhausted:
            duplicate_found = False
            seen = {}  # maps frozenset(row.items()) -> first slot index
            for i in range(self.curr_exploration_loaded):
                row = self.exploration_cache[i]
                key = frozenset(row.items())
                if key in seen:
                    # Slot i is a duplicate of slot seen[key]; replace it
                    duplicate_found = True
                    try:
                        self.exploration_cache[i] = next(R_iter)
                        # New row is checked for duplicates in the next pass
                    except StopIteration:
                        r_exhausted = True
                        break
                else:
                    seen[key] = i
            if not duplicate_found:
                break  # Clean pass — no duplicates remain
        # ── End duplicate removal ────────────────────────────────────────────

        counter = self.curr_exploration_loaded
        while counter > 0:
            try:
                s_row = next(S_iter)
            except StopIteration:
                break  # End of S reached during exploration

            for i in range(self.curr_exploration_loaded):
                if self.failure[i] >= self.n_failure_constant:
                    continue
                r_row = self.exploration_cache[i]
                if str(r_row[self.key_r]) == str(s_row[self.key_s]):
                    results.append((r_row, s_row))
                    self.reward[i] += 1
                else:
                    self.failure[i] += 1
                    if self.failure[i] == self.n_failure_constant:
                        counter -= 1

    def exploitation(self, R_iter, S_iter, results):
        """
        ROSL exploitation: build a fixed exploitation cache upfront, then probe
        every remaining S-row against every arm in that cache.

        Phase 1 — Cache construction:
            Selection draws from a fixed distribution over all n exploration arms:
                w_i = reward_i  if reward_i > 0  else  1
                W   = sum of all w_i  (fixed for every draw)
                p_i = w_i / W

            Two cases:

            Case A — exploration cache exhausted R (n <= exploitation_size):
                Every exploration arm is used directly as the exploitation cache.
                This is the maximum possible set of unique arms and no draws are
                needed.

            Case B — n > exploitation_size:
                exploitation_size slots are filled by drawing WITH replacement
                from the fixed distribution (same W every draw, same p_i every
                draw). After filling, every slot is compared with every other
                slot for full-row equality. Any duplicate slot is replaced by
                drawing again from the same fixed distribution. The process
                repeats until a complete pass finds zero duplicates.

            e_t for every probe of arm i in Phase 2 is the per-draw selection
            probability from the fixed distribution: p_i = w_i / W.

        Phase 2 — S scan:
            Every remaining S-row is probed against every arm in the fixed
            exploitation cache. ISPW is updated per physical probe using the
            arm's fixed e_t from Phase 1.
        """
        if self.curr_exploration_loaded == 0:
            return

        n = self.curr_exploration_loaded

        # Fixed selection weights: reward_i if nonzero, else 1.
        # W is constant for every draw — the same distribution is used for
        # every slot fill and every replacement draw.
        weights     = [self.reward[i] if self.reward[i] > 0 else 1 for i in range(n)]
        W           = float(sum(weights))
        # e_t per arm — fixed, computed once from the static distribution
        e_t_by_arm  = [weights[i] / W for i in range(n)]

        # ── Phase 1: build exploitation cache ────────────────────────────────
        if n <= self.exploitation_size:
            # Case A: R was exhausted filling the exploration cache; every
            # exploration arm is unique by construction (dedup already ran),
            # so use all n arms directly.
            exploit_slots = list(range(n))
        else:
            # Case B: draw exploitation_size slots with replacement, then
            # iteratively replace duplicates until the cache is clean.
            def draw_one():
                """Draw one slot index from the fixed distribution."""
                rng = random.random() * W
                cumsum = 0.0
                for i in range(n):
                    cumsum += weights[i]
                    if rng <= cumsum:
                        return i
                return n - 1  # fallback

            exploit_slots = [draw_one() for _ in range(self.exploitation_size)]

            # Post-fill dedup: replace duplicate slots by redrawing from the
            # same fixed distribution. Repeat until a full pass is clean.
            while True:
                duplicate_found = False
                seen = {}
                for pos in range(len(exploit_slots)):
                    slot = exploit_slots[pos]
                    row_key = frozenset(self.exploration_cache[slot].items())
                    if row_key in seen:
                        duplicate_found = True
                        exploit_slots[pos] = draw_one()
                        # New slot checked in next pass
                    else:
                        seen[row_key] = pos
                if not duplicate_found:
                    break

        # ── Phase 2: scan remaining S against the fixed exploitation cache ──
        for s_row in S_iter:
            for slot_idx in exploit_slots:
                r_row = self.exploration_cache[slot_idx]
                e_t   = e_t_by_arm[slot_idx]
                if str(r_row[self.key_r]) == str(s_row[self.key_s]):
                    results.append((r_row, s_row))
                    self._update_ispw(slot_idx, 1, e_t)
                else:
                    self._update_ispw(slot_idx, 0, e_t)

        # Fold per-arm ISPW accumulators directly into the global accumulator
        self._fold_into_global()

    def update_estimate_exploration(self):
        """
        Replay each exploration probe as a discrete ISPW event.

        Exploration selection probability:
            p_R(r) = exploration_size / |R|

        For the first N probes of arm i (where N = n_failure_constant):
            e_t = p_R(r)   (arm was selected for exploration and will be probed)

        For probes beyond the first N (arm survived because it had ≥ 1 success):
            e_t = p_R(r) * [1 - (1 - p_su)^N]
            where p_su = reward_i / (reward_i + failure_i)
        """
        if self.size_r == 0 or self.curr_exploration_loaded == 0:
            return

        p_R = self.exploration_size / self.size_r
        N   = self.n_failure_constant

        for i in range(self.curr_exploration_loaded):
            trials = self.reward[i] + self.failure[i]
            if trials == 0:
                continue

            # Compute the survival probability for probes beyond N
            if trials > N:
                p_su = self.reward[i] / trials
                survival_factor = 1.0 - (1.0 - p_su) ** N
                # Guard against degenerate zero (e.g. p_su = 0 despite trials > N)
                if survival_factor <= 0.0:
                    survival_factor = 1e-9
                e_t_beyond = p_R * survival_factor
            else:
                e_t_beyond = None  # Not used if trials <= N

            matches_remaining = self.reward[i]
            for t_local in range(trials):
                Y_t = 1 if matches_remaining > 0 else 0
                if Y_t:
                    matches_remaining -= 1

                # First N probes use the base selection probability
                if t_local < N:
                    e_t = p_R
                else:
                    e_t = e_t_beyond

                self._update_ispw(i, Y_t, e_t)

        # Fold exploration estimates into global accumulator
        self._fold_into_global()

    def _update_ispw(self, arm_idx, Y_t, e_t):
        """Apply one ISPW update for a single probe event."""
        if e_t <= 0:
            return
        self.T += 1
        h_t     = math.sqrt(e_t / self.T)
        Gamma_t = Y_t / e_t
        self.ispw_num[arm_idx] += h_t * Gamma_t
        self.ispw_den[arm_idx] += h_t
        self.tries[arm_idx]    += 1

    def _fold_into_global(self):
        """
        Fold per-arm ISPW numerators and denominators directly into the
        global accumulators. This preserves the global weighted mean without
        double-averaging through per-arm q_hat values.
        """
        for i in range(self.curr_exploration_loaded):
            self.global_num += self.ispw_num[i]
            self.global_den += self.ispw_den[i]
            # Reset arm accumulators so they aren't folded again next call
            self.ispw_num[i] = 0.0
            self.ispw_den[i] = 0.0
            self.tries[i]    = 0

    def estimate_join_size(self):
        """
        Estimate join size using ISPW with normalization.

        q_hat = global_num / global_den  (estimated mean match probability)

        Join size estimate = q_hat * |R| * |S| / global_den
        """
        if self.global_den == 0 or self.size_r == 0 or self.size_s == 0:
            return 0
        q_hat = self.global_num / self.global_den
        return q_hat * self.size_r * self.size_s / self.global_den