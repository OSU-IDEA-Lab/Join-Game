# ROSL Join Algorithm Variants: Probability Models and Estimation Methods

## Table of Contents
1. [Summary of Changes](#probability-comparison)
2. [Version Comparison](#version-comparison)
3. [Test Results](#test-results)
4. [Ablation Test Results](#test-results)
5. [Pooled Rate ISPW](#pooled-rate-ispw)
6. [Original Paper ISPW](#original-paper-ispw)

---

# Summary of Changes
Different event probabilities are used as described in the below version comparison section. The exploitation cache is rechosen each for each opposite relation tuple, to avoid poor performance on sparse joins.
# Version Comparison

In the **row labels** of the below table, labels of **discrete events** are in **bold**.
| | Global Pooling (Paper Ver) ISPW  | Localized Pooling ISPW |
|---|---|---|
| Exploitation Cache Duplicate Prevention | remove and replace | remove without replacement |
| Discrete Event Definition | a single probe | *same* |
| Exploration Selection Probability | $\frac{1}{\|R\|}$ | $\frac{\text{explorationSize}}{\|R\|}$ |
| **Exploration Probe Probability (First N probes)** | $\frac{1}{\|R\|}$ | $\frac{\text{explorationSize}}{\|R\|}$ |
| **Exploration Probe Probability (Additional probes beyond N)** | $\frac{1}{\|R\|} \times \left[1 - \left(1 - \frac{\text{totalRewards}_{i}}{\text{totalRewards}_{i} + \text{totalFailures}_{i}}\right)^N\right]$ | $\frac{\text{explorationSize}}{\|R\|} \times \sum_{k = j}^{N+j - 1} \binom{N+j-1}{k} (p_{su}(r))^k (1 - p_{su}(r))^{(N+j-1)-k} $ <br><br>where $j$ is how many probes beyond N the probe is |
| Exploitation Selection Probability | $\text{max}(0.01, \frac{\text{totalRewards}_{i}}{\sum_{\forall i} \text{totalRewards}_{i}})$ <br> normalized such that all probabilities sum to 1$| $\frac{\text{explorationSize}}{\|R\|} \times \left[ 1 - \left( 1 - \frac{\text{explorationRewards}_{i} + 1 / (explorationSize)}{ 1+ \sum_{\forall i} \text{explorationRewards}_{i}} \right)^\text{exploitationSize} \right]$ |
| **Exploitation Probe Probability** | $\text{max}(0.01, \frac{\text{totalRewards}_{i}}{\sum_{\forall i} \text{totalRewards}_{i}})$ <br>normalized such that all probabilities sum to 1$ | $\frac{\text{explorationSize}}{\|R\|} \times \left[ 1 - \left( 1 - \frac{\text{explorationRewards}_{i} + 1 / (explorationSize)}{ 1+ \sum_{\forall i} \text{explorationRewards}_{i}} \right)^\text{exploitationSize} \right]$ |
| Join Estimate Calculation | For each probe, the reward multiplied by $\frac{1}{e_{t}} \times \sqrt{e_{t} / T}$ is added to a global numerator and $\sqrt{e_{t} / T}$ is added to a global denominator. The estimated join size is:<br>$\frac{\text{globalNum}}{\text{globalden}} \times \|R\|  \times \|S\| \mathbin{/} \text{globalden}$ | Let $\hat{e}_{t}$ be the conditional probability of each probe event, given that the $R$ tuple was selected for exploration. For each probe, the reward multiplied by $\frac{1}{\hat{e}_{t}}$ is added to a per-arm per-round numerator and $\frac{1}{\hat{e}_{t}}$ is added to a per-arm per-round denominator. The estimated join size based on each round is: <br> $\sum_{i=1}^{n} \frac{\text{numexplore}[i] + \text{numexploit}[i]}{\text{denexplore}[i] + \text{denexploit}[i]} \times \|S\| \times \frac{\|R\|}{\text{explorationSize}}$ <br> The final estimate is the average of the join estimates from all rounds.|

# Test Results

# Ablation Test Results

# Pooled Rate ISPW

## Exploitation Cache Duplicate Prevention

If the the number of tuples left in `R` limited the exploration cache size such that the exploration cache size is $\leq$ the exploitation cache size limit, then fill the exploitation cache with all exploration tuples. This must the maximum set of unique explotation tuples.

Otherwise, it must be possible to fill the exploitation cache as follows:

Each exploitation cache tuple is drawn randomly from the same distribution, which means it is possible to draw duplicates. After the cache is filled, every slot is compared with every other slot to check for duplicates. Duplicates are removed without replacement.

## Distribution Exploration Cache Tuples are Selected from

Each tuple starts with a weight equal to the sum of exploration phase rewards and (1 /explorationSize). 
In one iteration, weights are converted to cumulative weights seen from begining of exploration cache; this is naturally non-decreasing. To select tuples, the algorithm:
- generates a random treshold in the range of 1 to the sum of all weights
- finds the first tuple greater than or equal to the random number with binary search

## Discrete Event
 
The discrete event is a **single probe** (one $(r_{i}, s_{j})$ join attempt).
 
## Probabilities by Case
 
### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_{R}(r)$.
 
$$p_{R}(r) = \frac{\text{explorationSize}}{|R|}$$
 
### *Discrete Event Case*: Exploration Probe (in First N probes per tuple)
The dependent probability that a tuple is probed each of the first N times in exploration, given that it was selected for exploration, is 1. 

Thereby, the overall probability of that a tuple is probed the first N times in exploration is
$$e_{t} = p_{R}(r) \times 1 = \frac{\text{explorationSize}}{|R|}$$
 
Since all discrete events are dependent on tuple $r$ being chosen for exploration, we will define $\hat{e_{t}}$ as conditional probability for discrete events given that the $r$ tuple has already been selected for exploration.
 
$$\hat{e_{t}} = 1.0$$
 

### *Discrete Event Case*: Exploration Probe (Additional probes beyond N)
*Note: Untested correction, suspected to correct probability for probes beyond N+1th)*

For $j > 0$, probability for N+$j\text{th}$ probe in exploration is dependent on the following three conditions:
- the tuple was chosen for exploration, with probability $p_{R}(r)$
- the tuple was probed the first N times, with probability 1
- the tuple was accumulated less than $N$ failures, with probability that will be derived below 

Since rewards per probe are in {0,1} for success or failure: $$\text{numfailures} = \text{numprobes} - \text{numrewards}$$
Thereby the **conditional** probability that the N+$j\text{th}$ exploration probe passes the $N$-Failure condition, given the tuple was chosen for exploration and the tuple was probed the first N times, is: 

$$
\begin{aligned}
\hat{e_{t}} &= P(\text{totalFailures}_{i} < N \text{, in } (N+j-1) \text{ probes}) \\
    &= P(\text{totalrewards}_{i} > (N+j-1) - N \text{, in }(N+j-1) \text{ probes}) \\
    &= P(\text{totalrewards}_{i} > (j-1)  \text{, in }(N+j-1) \text{ probes}) \\
    &= P(\text{totalrewards}_{i} \ge j \text{, in }(N+j-1) \text{ probes}) \\
    &= \sum_{k = j}^{N+j - 1} P(\text{totalrewards}_{i} = k \text{, in } (N+j-1) \text{ probes})\\
    &= \sum_{k = j}^{N+j - 1} \binom{N+j-1}{k} (p_{su}(r))^k (1 - p_{su}(r))^{(N+j-1)-k}
\end{aligned}
$$


Finally, given the arm's empirical match rate $p_{su}(r)$, the probability of this discrete event is as follows:
 
$$
\begin{aligned}
e_{t} &= p_{R}(r) \times 1 \times  P(\text{totalFailures}_{i} < N \text{, in } (N+j-1) \text{ probes}) \\
&=  \frac{\text{explorationSize}}{|R|}*\sum_{k = j}^{N+j - 1} \binom{N+j-1}{k} (p_{su}(r))^k (1 - p_{su}(r))^{(N+j-1)-k}
\end{aligned}
$$

### Exploitation Selection
An exploitation cache of up to `exploitation_size` $R$ tuples is built as described above for each $S$ tuple consumed in exploitation. 

The **conditional** probability that a $R$ tuple is selected for any single exploitation cache slot for a single $S$ tuple is as follows, given selection for exploration:
 
$$
\hat{p}_{draw, i} = \frac{\text{explorationRewards}_{i} + 1 / (explorationSize)}{ 1+ \sum_{\forall i} \text{explorationRewards}_{i}}
$$

The **conditional** probability that a $R$ tuple is selected for at least one exploitation cache slot for a single $S$ tuple is as follows, given selection for exploration:
 
$$
\begin{aligned}
\hat{p}_{\text{exploit},i} &= \left[ 1 - \left( 1 - \hat{p}_{draw, i} \right)^\text{exploitationSize} \right]\\
&= \left[ 1 - \left( 1 - \frac{\text{explorationRewards}_{i} + 1 / (explorationSize)}{ 1+ \sum_{\forall i} \text{explorationRewards}_{i}} \right)^\text{exploitationSize} \right]\\
\end{aligned}
$$

Without the condition of selection for exploration each $R$ tuple's selection probability for a probe is:

$$
p_{\text{exploit},i} = \frac{\text{explorationSize}}{|R|} \times \left[ 1 - \left( 1 - \frac{\text{explorationRewards}_{i} + 1 / (explorationSize)}{ 1+ \sum_{\forall i} \text{explorationRewards}_{i}} \right)^\text{exploitationSize} \right]
$$
 
### *Discrete Event Case*: Exploitation Probe
Every tuple in the fixed exploitation cache is probed against every remaining opposite relation tuple following the last probed in by any exploration cache tuple in exploration. Given that a tuple was selected for exploration, the **conditional** probability that a the tuple is probed against any opposite relation tuple in exploitation is the probability that the tuple was selected for exploitation; this probability is the tuple specific probability that was fixed at selection time.
 
$$\hat{e_{t}} = \hat{p}_{\text{exploit},i} $$

The probability of this discrete event is as follows:

$$e_{t} = p_{\text{exploit},i}$$

 
## Estimate Update Procedure
For every probe in both phases, the arm's per-phase accumulators are updated using a Horvitz-Thompson weighting without any variance-stabilizing multiplier. Here $\text{reward}_{t} \in \{0, 1\}$ is the binary match outcome of the probe, and $\hat{e_{t}}$ is the probability of the probe given selection for exploration as defined for each case above:
 
$$\text{num}[i] \mathrel{+}= \frac{\text{reward}_t}{\hat{e_{t}}} \qquad \text{den}[i] \mathrel{+}= \frac{1}{\hat{e_{t}}}$$
 
At the end of the round, exploration and exploitation accumulators are pooled per arm to compute an unbiased match rate. These rates are summed across all $n$ arms, multiplied by $|S|$ to project onto the full S relation, and scaled by $\frac{|R|}{\text{explorationSize}}$ to extrapolate from the $n$ sampled arms to the full R relation. Here $n$ is the number of arms actually loaded into the exploration cache in the current round — the lesser of explorationSize and the number of tuples remaining in $R$ — which may be smaller than explorationSize in the final round when $R$ is exhausted early:
 
$$\hat{J}_{\text{round}} = \sum_{i=1}^{n} \frac{\text{numexplore}[i] + \text{numexploit}[i]}{\text{denexplore}[i] + \text{denexploit}[i]} \times |S| \times \frac{|R|}{\text{explorationSize}}$$

**Why we use $\hat{e_{t}}$ and multiply by $p_{R}(r)$, instead of using $e_{t}$:**

Every probe event across phases is dependent on the selection of a tuple from $R$ for exploration with probability $p_{R}(r)$. Because this probability is identical for every probe, it multiplies every term in both `num[i]` and `den[i]` by the same constant. It therefore cancels exactly in the ratio `num[i] / den[i]`, leaving the pooled rate unchanged regardless of whether $p_{R}(r)$ is included in $e_{t}$ or not. The pooled ratio estimates the match probability *given* the tuple was selected for exploration if the first place with probability $p_{R}(r)$. The multiplier $\frac{|R|}{\text{explorationSize}}$ scales the estimate for tuples in the round(exploration+exploitation) up to represent the full relation $R$. 
 
Across rounds, the join size estimate is the simple average of per-round estimates across all rounds:
 
$$\hat{J} = \frac{1}{K} \sum_{k=1}^{K} \hat{J}_{\text{round},k}$$

# Original Paper ISPW


## Exploitation Cache Duplicate Removal

If the the number of tuples left in `R` limited the exploration cache size such that the exploration cache size is $\leq$ the exploitation cache size limit, then fill the exploitation cache with all exploration tuples. This must the maximum set of unique explotation tuples.

Otherwise, it must be possible to fill the exploitation cache as follows:

Each exploitation cache tuple is drawn randomly from the same distribution, which means it is possible to draw duplicates. After the cache is filled, every slot is compared with every other slot to check for duplicates. Duplicates are replaced by drawing another tuple from the same distrubution, which means duplicates in redraw is still possible. The proccess repeats until a complete pass finds zero duplicates.

## Discrete Event

The discrete event is a **single probe** (a join attempt between an $R$ tuple and an $S$ tuple).

## Probabilities by Case

### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_{R}(r)$.

$$p_{R}(r) = \frac{1}{|R|}$$

### *Discrete Event Case*: Exploration Probe (First N probes)
The probability that an exploration cache tuple is probed against an opposite relation tuple in the first N probes.

$$
\begin{aligned}
e_{t} = p_{R}(r) = \frac{1}{|R|}
\end{aligned}
$$

### *Discrete Event Case*: Exploration Probe (Additional probes beyond N)
An exploration cache tuple is only probed against an additional opposite relation tuple after the first N probes if:
- the tuple was chosen for exploration, with probability $p_{R}(r) = \frac{1}{|R|}$
- the tuple had at least one success in the first N probes, with probability (where $p_{su}(r)$ denotes the empirical success rate):

$$
\begin{aligned}
&1 - (1 - p_{su}(r))^N\\
= &1 - \left(1 - \frac{\text{totalRewards}_{i}}{\text{totalprobes}_{i}}\right)^N\\
= &1 - \left(1 - \frac{\text{totalRewards}_{i}}{\text{totalRewards}_{i} + \text{totalFailures}_{i}}\right)^N
\end{aligned}
$$

Thereby, the probability that an exploration cache tuple is probed against an additional opposite relation tuple after the first N probes is:

$$
\begin{aligned}
e_{t} &= p_{R}(r) \times \left[1 - (1 - p_{su}(r))^N\right] \\
&= \frac{1}{|R|} \times \left[1 - \left(1 - \frac{\text{totalRewards}_{i}}{\text{totalRewards}_{i} + \text{totalFailures}_{i}}\right)^N\right]
\end{aligned}
$$

### Exploitation Selection

The initial weight given to each exploration cache tuple is:

$$\text{max}(0.01, \frac{\text{totalRewards}_{i}}{\sum_{\forall i} \text{totalRewards}_{i}})$$

The probability of selecting a tuple from the exploration cache for exploitation, randomly proportional to their exploration reward, is the result when the above described weights are normalized such that they add up to 1. 

### *Discrete Event Case*: Exploitation Probe
All exploitation cache tuples will be probed against all remaining opposite relation tuples beyond the last opposite relation tuple tried by any exploration cache tuple in exploration. The probability that an exploitation cache tuple is probed against an opposite relation tuple in exploitation is the same probability that it was selected for exploitation.

## Estimation Method

For each probe, Inverse-Selection-Probability-Weighting (ISPW) is applied using an adaptive variance-stabilizing multiplier ($h_t = \sqrt{e_{t} / T}$). The outcome ($\Gamma_t = Y_t / e_{t}$) is weighted by $h_t$ and added to the arm's local numerator, while $h_t$ is added to the denominator.

At the end of each phase, per-arm accumulators (`ispw_num[i]`, `ispw_den[i]`) are folded **directly** into a global estimate accumulator (`globalNum`, `globalDen`) and then zeroed. The final join size is computed entirely from this global mean probability:

$$\hat{J} = \frac{\text{globalNum}}{\text{globalden}} \times |R| \times |S| \mathbin{/} \text{globalden}$$

---
