import math
import random
import bisect

import math

# Compatibility patch for Python 3.6
if not hasattr(math, 'comb'):
    def comb(n, k):
        if k < 0 or k > n: return 0
        if k == 0 or k == n: return 1
        if k > n // 2: k = n - k
        
        numerator = 1
        for i in range(k):
            numerator = numerator * (n - i) // (i + 1)
        return numerator
    math.comb = comb

class ROSL:
    def __init__(self, exploration_size, exploitation_size, n_failure_constant,
                 file_r, key_r, file_s, key_s):
        self.exploration_size   = exploration_size
        self.exploitation_size  = exploitation_size  # L in the paper
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

        # Per-arm, per-phase accumulators
        self.num_explore = [0.0] * exploration_size
        self.den_explore = [0.0] * exploration_size
        self.num_exploit = [0.0] * exploration_size
        self.den_exploit = [0.0] * exploration_size

        # Round averaging
        self.accumulated_estimate = 0.0
        self.rounds_run           = 0

    # ------------------------------------------------------------------
    def run_round(self, R_iter, probe_relation, results):
        # [Shared Iterator Fix] Partitioning S rather than rescanning
        S_iter = iter(probe_relation)

        self.exploration(R_iter, S_iter, results)
        self.update_estimate_exploration()

        self.exploitation(R_iter, S_iter, results)

        self.round_number           += 1
        self.agg_exploration_loaded  = self.curr_exploration_loaded
        self.reset_between_rounds()

    # ------------------------------------------------------------------
    def reset_between_rounds(self):
        """Accumulate round estimate before clearing per-round state."""
        if self.curr_exploration_loaded > 0:
            self.accumulated_estimate += self._estimate_round()
            self.rounds_run += 1

        self.exploration_cache  = [None] * self.exploration_size
        self.reward             = [0]    * self.exploration_size
        self.failure            = [0]    * self.exploration_size
        self.curr_exploration_loaded = 0

        n = self.exploration_size
        self.num_explore = [0.0] * n
        self.den_explore = [0.0] * n
        self.num_exploit = [0.0] * n
        self.den_exploit = [0.0] * n

    # ------------------------------------------------------------------
    def exploration(self, R_iter, S_iter, results):
        """N-Failure exploration logic."""
        for i in range(self.exploration_size):
            try:
                self.exploration_cache[i] = next(R_iter)
                self.curr_exploration_loaded += 1
            except StopIteration:
                break

        counter = self.curr_exploration_loaded
        while counter > 0:
            try:
                s_row = next(S_iter)
            except StopIteration:
                break 

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

    # ------------------------------------------------------------------
    def exploitation(self, R_iter, S_iter, results):
        """
        Cache-per-S-row exploitation with fixed distribution.

        Phase 1 — Fix distribution (runs once after exploration):
            Calculate probabilities using water-level smoothing (1/exploration_size).
            Pre-calculate cumulative weights for O(log n) binary search.

        Phase 2 — Per-S-row cache construction and probing:
            For EACH S row:
                a) Draw L samples WITH replacement from the fixed distribution using binary search
                b) Keep unique slots only (discard duplicate draws)
                c) Compute marginal inclusion probability π_i for each unique slot
                d) Probe this S row against each unique slot with HT weight 1/π_i
        """
        if self.curr_exploration_loaded == 0:
            return

        n = self.curr_exploration_loaded
        
        # ── Phase 1: Fix the probability distribution (ONCE) ────────────────
        # Water-level smoothing (1/exploration_size)
        smoothing_constant = 1.0 / self.exploration_size
        arm_weights = [self.reward[i] + smoothing_constant for i in range(n)]
        total_weight = float(sum(arm_weights))
        
        # Fixed single-draw probabilities for entire exploitation phase
        p_draw = [w / total_weight for w in arm_weights]
        
        # Pre-compute cumulative distribution function (CDF) for O(log n) binary search
        cum_weights = []
        cumsum = 0.0
        for p in p_draw:
            cumsum += p
            cum_weights.append(cumsum)
        # Force the last element to 1.0 to avoid floating point precision edge cases
        cum_weights[-1] = 1.0
        
        num_draws = min(self.exploitation_size, n)

        # ── Phase 2: For each S row, build NEW cache and probe ──────────────
        for s_row in S_iter:
            # Draw L times WITH replacement from the fixed distribution
            drawn_slots = set()
            
            for _ in range(num_draws):
                rng = random.random()
                # O(log n) binary search to find the correct tuple index
                selected_idx = bisect.bisect_left(cum_weights, rng)
                
                # Safety fallback just in case of rounding errors
                if selected_idx >= n:
                    selected_idx = n - 1
                    
                drawn_slots.add(selected_idx)
            
            # Compute marginal inclusion probability π_i for each unique slot
            # π_i = P(slot i appears in at least one of L draws)
            #     = 1 - P(slot i not drawn in any of L draws)
            #     = 1 - (1 - p_draw[i])^L
            exploit_cache = []
            for idx in drawn_slots:
                pi_i = 1.0 - ((1.0 - p_draw[idx]) ** num_draws)
                exploit_cache.append((idx, pi_i))
            
            # Probe this S row against each unique slot in the cache
            for slot_idx, pi_i in exploit_cache:
                r_row = self.exploration_cache[slot_idx]
                matched = (str(r_row[self.key_r]) == str(s_row[self.key_s]))
                if matched:
                    results.append((r_row, s_row))
                self.num_exploit[slot_idx] += (1.0 if matched else 0.0) / pi_i
                self.den_exploit[slot_idx] += 1.0 / pi_i

    # ------------------------------------------------------------------
    @staticmethod
    def _beyond_n_survival(j, N, p_su):
        """Probability of surviving to the (N+j)-th probe."""
        total_probes = N + j - 1
        p_survive = 0.0
        for k in range(j, total_probes + 1):
            p_survive += (
                math.comb(total_probes, k)
                * (p_su ** k)
                * ((1.0 - p_su) ** (total_probes - k))
            )
        return max(p_survive, 1e-9)

    def update_estimate_exploration(self):
        """Rate-based IPW replay with Dynamic Binomial Survival."""
        if self.size_r == 0 or self.curr_exploration_loaded == 0:
            return

        N = self.n_failure_constant

        for i in range(self.curr_exploration_loaded):
            trials = self.reward[i] + self.failure[i]
            if trials == 0:
                continue

            p_su = self.reward[i] / trials if trials > 0 else 0.0

            matches_remaining = self.reward[i]
            for t_local in range(trials):
                Y_t = 1 if matches_remaining > 0 else 0
                if Y_t:
                    matches_remaining -= 1

                if t_local < N:
                    e_t = 1.0
                else:
                    j = t_local - N + 1
                    e_t = self._beyond_n_survival(j, N, p_su)

                self.num_explore[i] += Y_t / e_t
                self.den_explore[i] += 1.0 / e_t

    # ------------------------------------------------------------------
    def _estimate_round(self):
        """Pooled match rate estimator."""
        n = self.curr_exploration_loaded
        if n == 0:
            return 0.0

        total_rate = 0.0
        for i in range(n):
            den = self.den_explore[i] + self.den_exploit[i]
            if den > 0:
                total_rate += (self.num_explore[i] + self.num_exploit[i]) / den

        scale = (self.size_r / n) if n > 0 else 1.0
        return total_rate * self.size_s * scale

    # ------------------------------------------------------------------
    def estimate_join_size(self):
        """Return average of per-round estimates."""
        if self.rounds_run == 0:
            return self._estimate_round()
        return self.accumulated_estimate / self.rounds_run