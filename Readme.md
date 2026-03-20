# ROSL Join Algorithm Variants: Probability Models and Estimation Methods

## Table of Contents
1. [Original Paper ISPW](#original-paper-ispw)
2. [Modified Version Following Paper's Intent](#modified-version-following-papers-intent)
3. [Half Random Exploitation Cache Approach](#half-random-exploitation-cache-approach)
   - [With Active State Conditioning](#with-exploration-probe-probabilities-conditioned-on-the-active-state)
   - [With Empirical Survival Weighting](#with-exploration-probe-probabilities-based-on-empirical-survival-weighting)

---

# Original Paper ISPW

## Probabilities by Case

### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_R(r)$.

$$p_R(r) = \frac{\text{exploration\_size}}{|R|}$$

### Exploration Probe (First N probes)
The probability that an exploration cache tuple is probed against an opposite relation tuple in the first N probes.

$$
\begin{aligned}
e_t = p_R(r) = \frac{\text{exploration\_size}}{|R|}
\end{aligned}
$$

### Exploration Probe (Additional probes beyond N)
An exploration cache tuple is only probed against an additional opposite relation tuple after the first N probes if:
- the tuple was chosen for exploration, with probability $p_R(r) = \frac{\text{exploration\_size}}{|R|}$
- the tuple had at least one success in the first N probes, with probability (where $p_{su}(r)$ denotes the empirical success rate):

$$
\begin{aligned}
&1 - (1 - p_{su}(r))^N\\
= &1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N
\end{aligned}
$$

Thereby, the probability that an exploration cache tuple is probed against an additional opposite relation tuple after the first N probes is:

$$
\begin{aligned}
e_t &= p_R(r) \times \left[1 - (1 - p_{su}(r))^N\right] \\
&= \frac{\text{exploration\_size}}{|R|} \times \left[1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N\right]
\end{aligned}
$$

### Exploitation Selection
The probability of selecting a tuple from the exploration cache for exploitation, randomly proportional to their exploration reward, is:

$$\frac{\text{reward}_i}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

### Exploitation Probe
All exploitation cache tuples will be probed against all remaining opposite relation tuples beyond the last opposite relation tuple tried by any exploration cache tuple in exploration. The probability that an exploitation cache tuple is probed against an opposite relation tuple in exploitation is the same probability that it was selected for exploitation:

$$\frac{\text{reward}_i}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

## Exploration Cache Duplicate Removal

After the cache fills via sequential `next(R_iter)` draws, a post-fill deduplication pass runs before any probing begins. Every pair of loaded slots is compared for full row equality (identical dict content). Any duplicate slot is replaced by drawing the next tuple from `R_iter` using the same mechanism as the original fill. The replacement is not checked in the current pass; it is deferred to the next iteration. The loop repeats until a complete pass finds zero duplicates or `R` is exhausted. If `R` is exhausted before the cache is clean, the cache is used as-is with whatever duplicates remain.

## Estimate Update Procedure's Discrete Event

The discrete event is a **single probe** (a join attempt between an $R$ tuple and an $S$ tuple).

For each probe, Inverse-Selection-Probability-Weighting (ISPW) is applied using an adaptive variance-stabilizing multiplier ($h_t = \sqrt{e_t / T}$). The outcome ($\Gamma_t = Y_t / e_t$) is weighted by $h_t$ and added to the arm's local numerator, while $h_t$ is added to the denominator.

At the end of each phase, per-arm accumulators (`ispw_num[i]`, `ispw_den[i]`) are folded **directly** into a global estimate accumulator (`global_num`, `global_den`) and then zeroed. The final join size is computed entirely from this global mean probability:

$$\hat{J} = \frac{\text{global\_num}}{\text{global\_den}} \times |R| \times |S| \mathbin{/} \text{global\_den}$$

---

# Modified Version Following Paper's Intent

## Description of Fixes

### The "Double Division" Fix
The original code divided the final estimated count by the sample size (`global_den`) a second time. The fix removes this division, returning `q_hat * size_r * size_s`. This deviates from the literal instruction in Section 4.2.4 of the paper, which states to multiply by $\frac{|R||S|}{|J|}$. However, because `q_hat` is already a mean probability, dividing by the sample size again shrinks the estimate incorrectly. The fix aligns with the paper's true statistical intent to use ISPW to find a sample mean and extrapolate it by the pure Cartesian population size.

### The "Shared Iterator" Fix
The fix instantiates a single `S_iter` at the start of the round and passes it continuously through both exploration and exploitation, effectively partitioning table $S$. This deviates from Section 3.2.4 of the paper, which explicitly instructs to sequentially scan $S$ "from the beginning" during exploitation. The literal instruction causes the algorithm to evaluate the exact same pairs twice, manufacturing duplicate rows that do not exist in the base tables. The fix aligns with the paper's intent to balance exploration and exploitation without violating fundamental relational database data integrity.

### The "Double Scaling" Fix
This fix removes the exploration selection probability ($p_R$) from the `e_t` calculation in both exploration and exploitation, leaving only the survival or trial probability. This deviates from the literal interpretation of ISPW, which suggests dividing by the product of *all* probabilistic selection factors immediately. The fix aligns with the paper's intent by recognizing the codebase's architecture: a blanket extrapolation multiplier ($|R| \times |S|$) is already applied at the end of every round. Removing the early scaling prevents the exploration selection factor from being applied twice and distorting the estimate.

## Probabilities by Case

### Exploration Selection
Same as Original. $p_R(r) = \frac{\text{exploration\_size}}{|R|}$.

### Exploration Probe (First N probes)
With $p_R$ removed from $e_t$ per the Double Scaling Fix, each arm is treated as certain to be probed once loaded:

$$e_t = 1.0$$

### Exploration Probe (Additional probes beyond N)
With $p_R$ removed per the Double Scaling Fix, only the bare survival factor remains:

$$e_t = 1 - (1 - p_{su}(r))^N = 1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N$$

### Exploitation Selection
Exploitation selection uses Laplace add-one smoothing instead of the piecewise zero-guard of the Original Paper version. The denominator is $n + \sum \text{rewards}$, where $n$ is the number of arms actually loaded into the cache in the current round (not `exploration_size`), ensuring all $n$ probabilities sum to exactly 1:

$$\frac{\text{reward}_i + 1}{n + \sum \text{rewards}}$$

### Exploitation Probe
Each exploitation arm is probed against at most one S tuple per round (uniqueness guarantee — see below). The probability that a given arm is probed against a given S tuple is its Laplace-smoothed selection probability at the moment of the draw, using the `total_weight` that reflects only arms still eligible at that point:

$$e_t = \frac{\text{reward}_i + 1}{\text{total\_weight at draw time}}$$

## Exploration Cache Duplicate Removal

Duplicates in the exploitation phase are **prevented** rather than removed. Each arm is held in a mutable `arm_weights` array initialised to `reward_i + 1`. After an arm is selected for a given S tuple, its weight is set to 0 and `total_weight` is decremented by that weight. Subsequent draws use the reduced `total_weight`, making it impossible for the same arm to be selected for a second S tuple in the same round. Exploitation stops when `total_weight` reaches 0 (all arms consumed) or S is exhausted, whichever comes first.

## Corrected Version's Estimate Update Procedure's Discrete Event

The discrete event remains a **single probe**. The join size updates are still computed using the variance-stabilized ISPW formula ($\Gamma_t$ and $h_t$), and the local metadata is still folded directly into the **global estimate accumulator**. The critical changes are:

1. `e_t` values no longer include $p_R$, preventing the exploration selection factor from being squared.
2. The global estimate is correctly extrapolated by multiplying the global mean probability (`global_num / global_den`) by the Cartesian product ($|R| \times |S|$), eliminating the double-division bug:

$$\hat{J} = \frac{\text{global\_num}}{\text{global\_den}} \times |R| \times |S|$$

---

# ROSL Join Algorithm Variants: Probability Models and Estimation Methods

## Table of Contents
1. [Original Paper ISPW](#original-paper-ispw)
2. [Modified Version Following Paper's Intent](#modified-version-following-papers-intent)
3. [Half Random Exploitation Cache Approach](#half-random-exploitation-cache-approach)
   - [With Active State Conditioning](#with-exploration-probe-probabilities-conditioned-on-the-active-state)
   - [With Empirical Survival Weighting](#with-exploration-probe-probabilities-based-on-empirical-survival-weighting)

---

# Original Paper ISPW

## Probabilities by Case

### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_R(r)$.

$$p_R(r) = \frac{\text{exploration\_size}}{|R|}$$

### Exploration Probe (First N probes)
The probability that an exploration cache tuple is probed against an opposite relation tuple in the first N probes.

$$
\begin{aligned}
e_t = p_R(r) = \frac{\text{exploration\_size}}{|R|}
\end{aligned}
$$

### Exploration Probe (Additional probes beyond N)
An exploration cache tuple is only probed against an additional opposite relation tuple after the first N probes if:
- the tuple was chosen for exploration, with probability $p_R(r) = \frac{\text{exploration\_size}}{|R|}$
- the tuple had at least one success in the first N probes, with probability (where $p_{su}(r)$ denotes the empirical success rate):

$$
\begin{aligned}
&1 - (1 - p_{su}(r))^N\\
= &1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N
\end{aligned}
$$

Thereby, the probability that an exploration cache tuple is probed against an additional opposite relation tuple after the first N probes is:

$$
\begin{aligned}
e_t &= p_R(r) \times \left[1 - (1 - p_{su}(r))^N\right] \\
&= \frac{\text{exploration\_size}}{|R|} \times \left[1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N\right]
\end{aligned}
$$

### Exploitation Selection
The probability of selecting a tuple from the exploration cache for exploitation, randomly proportional to their exploration reward, is:

$$\frac{\text{reward}_i}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

### Exploitation Probe
All exploitation cache tuples will be probed against all remaining opposite relation tuples beyond the last opposite relation tuple tried by any exploration cache tuple in exploration. The probability that an exploitation cache tuple is probed against an opposite relation tuple in exploitation is the same probability that it was selected for exploitation:

$$\frac{\text{reward}_i}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

## Exploration Cache Duplicate Removal

After the cache fills via sequential `next(R_iter)` draws, a post-fill deduplication pass runs before any probing begins. Every pair of loaded slots is compared for full row equality (identical dict content). Any duplicate slot is replaced by drawing the next tuple from `R_iter` using the same mechanism as the original fill. The replacement is not checked in the current pass; it is deferred to the next iteration. The loop repeats until a complete pass finds zero duplicates or `R` is exhausted. If `R` is exhausted before the cache is clean, the cache is used as-is with whatever duplicates remain.

## Discrete Event

The discrete event is a **single probe** (a join attempt between an $R$ tuple and an $S$ tuple).

For each probe, Inverse-Selection-Probability-Weighting (ISPW) is applied using an adaptive variance-stabilizing multiplier ($h_t = \sqrt{e_t / T}$). The outcome ($\Gamma_t = Y_t / e_t$) is weighted by $h_t$ and added to the arm's local numerator, while $h_t$ is added to the denominator.

At the end of each phase, per-arm accumulators (`ispw_num[i]`, `ispw_den[i]`) are folded **directly** into a global estimate accumulator (`global_num`, `global_den`) and then zeroed. The final join size is computed entirely from this global mean probability:

$$\hat{J} = \frac{\text{global\_num}}{\text{global\_den}} \times |R| \times |S| \mathbin{/} \text{global\_den}$$

---

# Modified Version Following Paper's Intent

## Description of Fixes

### The "Double Division" Fix
The original code divided the final estimated count by the sample size (`global_den`) a second time. The fix removes this division, returning `q_hat * size_r * size_s`. This deviates from the literal instruction in Section 4.2.4 of the paper, which states to multiply by $\frac{|R||S|}{|J|}$. However, because `q_hat` is already a mean probability, dividing by the sample size again shrinks the estimate incorrectly. The fix aligns with the paper's true statistical intent to use ISPW to find a sample mean and extrapolate it by the pure Cartesian population size.

### The "Shared Iterator" Fix
The fix instantiates a single `S_iter` at the start of the round and passes it continuously through both exploration and exploitation, effectively partitioning table $S$. This deviates from Section 3.2.4 of the paper, which explicitly instructs to sequentially scan $S$ "from the beginning" during exploitation. The literal instruction causes the algorithm to evaluate the exact same pairs twice, manufacturing duplicate rows that do not exist in the base tables. The fix aligns with the paper's intent to balance exploration and exploitation without violating fundamental relational database data integrity.

### The "Double Scaling" Fix
This fix removes the exploration selection probability ($p_R$) from the `e_t` calculation in both exploration and exploitation, leaving only the survival or trial probability. This deviates from the literal interpretation of ISPW, which suggests dividing by the product of *all* probabilistic selection factors immediately. The fix aligns with the paper's intent by recognizing the codebase's architecture: a blanket extrapolation multiplier ($|R| \times |S|$) is already applied at the end of every round. Removing the early scaling prevents the exploration selection factor from being applied twice and distorting the estimate.

## Probabilities by Case

### Exploration Selection
Same as Original. $p_R(r) = \frac{\text{exploration\_size}}{|R|}$.

### Exploration Probe (First N probes)
With $p_R$ removed from $e_t$ per the Double Scaling Fix, each arm is treated as certain to be probed once loaded:

$$e_t = 1.0$$

### Exploration Probe (Additional probes beyond N)
With $p_R$ removed per the Double Scaling Fix, only the bare survival factor remains:

$$e_t = 1 - (1 - p_{su}(r))^N = 1 - \left(1 - \frac{\text{reward}_i}{\text{reward}_i + \text{failure}_i}\right)^N$$

### Exploitation Selection
Exploitation selection uses Laplace add-one smoothing instead of the piecewise zero-guard of the Original Paper version. The denominator is $n + \sum \text{rewards}$, where $n$ is the number of arms actually loaded into the cache in the current round (not `exploration_size`), ensuring all $n$ probabilities sum to exactly 1:

$$\frac{\text{reward}_i + 1}{n + \sum \text{rewards}}$$

### Exploitation Probe
Each exploitation arm is probed against at most one S tuple per round (uniqueness guarantee — see below). The probability that a given arm is probed against a given S tuple is its Laplace-smoothed selection probability at the moment of the draw, using the `total_weight` that reflects only arms still eligible at that point:

$$e_t = \frac{\text{reward}_i + 1}{\text{total\_weight at draw time}}$$

## Exploration Cache Duplicate Removal

Duplicates in the exploitation phase are **prevented** rather than removed. Each arm is held in a mutable `arm_weights` array initialised to `reward_i + 1`. After an arm is selected for a given S tuple, its weight is set to 0 and `total_weight` is decremented by that weight. Subsequent draws use the reduced `total_weight`, making it impossible for the same arm to be selected for a second S tuple in the same round. Exploitation stops when `total_weight` reaches 0 (all arms consumed) or S is exhausted, whichever comes first.

## Discrete Event

The discrete event remains a **single probe**. The join size updates are still computed using the variance-stabilized ISPW formula ($\Gamma_t$ and $h_t$), and the local metadata is still folded directly into the **global estimate accumulator**. The critical changes are:

1. `e_t` values no longer include $p_R$, preventing the exploration selection factor from being squared.
2. The global estimate is correctly extrapolated by multiplying the global mean probability (`global_num / global_den`) by the Cartesian product ($|R| \times |S|$), eliminating the double-division bug:

$$\hat{J} = \frac{\text{global\_num}}{\text{global\_den}} \times |R| \times |S|$$