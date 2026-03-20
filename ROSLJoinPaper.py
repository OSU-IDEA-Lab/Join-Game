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

        e_t for each probe = smoothed_reward_i / total_smoothed_rewards,
        i.e. the probability the selected arm was chosen.
        """
        if self.curr_exploration_loaded == 0:
            return

        # Build smoothed reward distribution over explored tuples.
        # Zero rewards are smoothed to 0.01 so every arm has a non-zero
        # selection probability.
        smoothed = [(i, self.reward[i] if self.reward[i] > 0 else 0.01)
                    for i in range(self.curr_exploration_loaded)]
        total_smoothed = sum(r for _, r in smoothed)

        for s_row in S_iter:
            # Probabilistically select ONE tuple at each step
            rng = random.random() * total_smoothed
            cumsum = 0.0
            selected_idx = smoothed[-1][0]          # fallback to last arm
            selected_smoothed_reward = smoothed[-1][1]
            for idx, sr in smoothed:
                cumsum += sr
                if rng <= cumsum:
                    selected_idx = idx
                    selected_smoothed_reward = sr
                    break

            # e_t = probability this arm was selected for exploitation
            e_t = selected_smoothed_reward / total_smoothed

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