import math
import random

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

        self.T = 0
        self.ispw_num   = [0.0] * exploration_size
        self.ispw_den   = [0.0] * exploration_size
        self.tries      = [0]   * exploration_size
        self.global_num = 0.0
        self.global_den = 0.0

    def run_round(self, R_iter, probe_relation, results):
        # [Shared Iterator Fix] A single S_iter is created once and passed
        # continuously through both phases, partitioning S rather than
        # rescanning it from the beginning for exploitation.
        S_iter = iter(probe_relation)

        self.exploration(R_iter, S_iter, results)
        self.update_estimate_exploration()

        # Exploitation picks up exactly where S_iter was left by exploration
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
        ROSL exploitation with probabilistic selection (Section 4.1.2).

        "at each step of the exploitation phase of Γ, ROSL picks tuples of R
        that are explored in Γ randomly proportional to their reward"

        [Double-Scaling Fix] Exploitation selection uses Laplace-style add-one
        smoothing instead of a piecewise zero-guard:

            e_t = (reward_i + 1) / (exploration_size + sum_rewards)

        This gives every arm a non-zero probability without a piecewise branch,
        and does NOT include p_R(r) in e_t. The blanket |R|/n extrapolation
        in estimate_join_size already accounts for selection from R, so
        folding p_R into e_t here would square that factor.
        """
        if self.curr_exploration_loaded == 0:
            return

        n = self.curr_exploration_loaded
        sum_rewards = sum(self.reward[i] for i in range(n))

        # Laplace-smoothed weights: reward_i + 1 for each of the n loaded arms.
        # Denominator = n + sum_rewards so that all n probabilities sum to 1:
        #   sum_i (reward_i + 1) / (n + sum_rewards)
        #   = (sum_rewards + n) / (n + sum_rewards) = 1
        # Uniqueness guarantee: once an arm is selected its weight is zeroed and
        # total_weight is decremented by that weight, so the same arm cannot be
        # drawn again for a different S tuple in the same round.
        arm_weights  = [self.reward[i] + 1 for i in range(n)]
        total_weight = float(sum(arm_weights))  # == n + sum_rewards

        for s_row in S_iter:
            if total_weight <= 0:
                break  # All arms exhausted; no unique selections remain

            # Probabilistically select ONE tuple proportional to current weights
            rng = random.random() * total_weight
            cumsum = 0.0
            selected_idx = n - 1  # fallback to last arm with non-zero weight
            for i in range(n):
                if arm_weights[i] == 0:
                    continue
                cumsum += arm_weights[i]
                if rng <= cumsum:
                    selected_idx = i
                    break

            # e_t = selection probability at the moment this arm was drawn.
            # total_weight reflects only arms still eligible at draw time.
            e_t = arm_weights[selected_idx] / total_weight

            # Zero out selected arm's weight and shrink the pool so it cannot
            # be chosen again for a subsequent S tuple this round.
            total_weight -= arm_weights[selected_idx]
            arm_weights[selected_idx] = 0

            r_row = self.exploration_cache[selected_idx]
            if str(r_row[self.key_r]) == str(s_row[self.key_s]):
                results.append((r_row, s_row))
                self._update_ispw(selected_idx, 1, e_t)
            else:
                self._update_ispw(selected_idx, 0, e_t)

        # Fold per-arm ISPW accumulators directly into the global accumulator
        self._fold_into_global()

    def update_estimate_exploration(self):
        """
        Replay each exploration probe as a discrete ISPW event.

        [Double-Scaling Fix] e_t no longer includes p_R(r). The blanket
        |R| / exploration_size extrapolation in estimate_join_size already
        covers the probability of selecting a tuple from R, so including
        p_R here would square that factor.

        For the first N probes of arm i (N = n_failure_constant):
            e_t = 1.0
            (arm is certain to be probed given it was loaded into the cache)

        For probes beyond the first N (arm survived because it had ≥ 1 success):
            e_t = 1 - (1 - p_su)^N
            where p_su = reward_i / (reward_i + failure_i)

        This is purely the survival/trial probability; p_R is left to the
        final extrapolation step.
        """
        if self.size_r == 0 or self.curr_exploration_loaded == 0:
            return

        N = self.n_failure_constant

        for i in range(self.curr_exploration_loaded):
            trials = self.reward[i] + self.failure[i]
            if trials == 0:
                continue

            # Survival factor for probes beyond N
            if trials > N:
                p_su = self.reward[i] / trials
                survival_factor = 1.0 - (1.0 - p_su) ** N
                # Guard against degenerate zero
                if survival_factor <= 0.0:
                    survival_factor = 1e-9
                e_t_beyond = survival_factor
            else:
                e_t_beyond = None  # Not used if trials <= N

            matches_remaining = self.reward[i]
            for t_local in range(trials):
                Y_t = 1 if matches_remaining > 0 else 0
                if Y_t:
                    matches_remaining -= 1

                # First N probes: arm is certain to be probed once loaded
                if t_local < N:
                    e_t = 1.0
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
        Estimate join size using ISPW extrapolation.

        q_hat = global_num / global_den  (estimated mean match probability
                                          over the observed sample)

        [Double-Division Fix] The join size estimate is simply:
            q_hat * |R| * |S|

        The previous code divided by global_den a second time, which shrinks
        the estimate incorrectly. q_hat is already a mean probability; the
        only extrapolation needed is scaling by the full Cartesian population.
        """
        if self.global_den == 0 or self.size_r == 0 or self.size_s == 0:
            return 0
        q_hat = self.global_num / self.global_den
        return q_hat * self.size_r * self.size_s