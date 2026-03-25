# ROSL Join Algorithm Variants: Probability Models and Estimation Methods

## Table of Contents
1. [Original Paper ISPW](#original-paper-ispw)
2. [Pooled Rate Estimator](#pooled-rate-estimator)

---

# Original Paper ISPW


## Exploration Cache Duplicate Removal

If the the number of tuples left in `R` limited the exploration cache size such that the exploration cache size is $\leq$ the exploitation cache size limit, then fill the exploitation cache with all exploration tuples. This must the maximum set of unique explotation tuples.

Otherwise, it must be possible to fill the exploitation cache as follows:

Each exploitation cache tuple is drawn randomly from the same distribution, which means it is possible to draw duplicates. After the cache is filled, every slot is compared with every other slot to check for duplicates. Duplicates are replaced by drawing another tuple from the same distrubution, which means duplicates in redraw is still possible. The proccess repeats until a complete pass finds zero duplicates.

## Discrete Event

The discrete event is a **single probe** (a join attempt between an $R$ tuple and an $S$ tuple).

## Probabilities by Case

### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_{R}(r)$.

$$p_{R}(r) = \frac{\text{explorationsize}}{|R|}$$

### *Discrete Event Case*: Exploration Probe (First N probes)
The probability that an exploration cache tuple is probed against an opposite relation tuple in the first N probes.

$$
\begin{aligned}
e_{t} = p_{R}(r) = \frac{\text{explorationsize}}{|R|}
\end{aligned}
$$

### *Discrete Event Case*: Exploration Probe (Additional probes beyond N)
An exploration cache tuple is only probed against an additional opposite relation tuple after the first N probes if:
- the tuple was chosen for exploration, with probability $p_{R}(r) = \frac{\text{explorationsize}}{|R|}$
- the tuple had at least one success in the first N probes, with probability (where $p_{su}(r)$ denotes the empirical success rate):

$$
\begin{aligned}
&1 - (1 - p_{su}(r))^N\\
= &1 - \left(1 - \frac{\text{totalrewards}_{i}}{\text{totalprobes}_{i}}\right)^N\\
= &1 - \left(1 - \frac{\text{totalrewards}_{i}}{\text{totalrewards}_{i} + \text{totalfailures}_{i}}\right)^N
\end{aligned}
$$

Thereby, the probability that an exploration cache tuple is probed against an additional opposite relation tuple after the first N probes is:

$$
\begin{aligned}
e_{t} &= p_{R}(r) \times \left[1 - (1 - p_{su}(r))^N\right] \\
&= \frac{\text{explorationsize}}{|R|} \times \left[1 - \left(1 - \frac{\text{totalrewards}_{i}}{\text{totalrewards}_{i} + \text{totalfailures}_{i}}\right)^N\right]
\end{aligned}
$$

### Exploitation Selection
The probability of selecting a tuple from the exploration cache for exploitation, randomly proportional to their exploration reward, is:

$$\frac{\text{totalrewards}_{i}}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

### *Discrete Event Case*: Exploitation Probe
All exploitation cache tuples will be probed against all remaining opposite relation tuples beyond the last opposite relation tuple tried by any exploration cache tuple in exploration. The probability that an exploitation cache tuple is probed against an opposite relation tuple in exploitation is the same probability that it was selected for exploitation:

$$\frac{\text{totalrewards}_{i}}{\sum \text{rewards}}$$

with zero rewards smoothed to $0.01$ so that all exploration tuples have a non-zero chance of being selected.

## Estimation Method

For each probe, Inverse-Selection-Probability-Weighting (ISPW) is applied using an adaptive variance-stabilizing multiplier ($h_t = \sqrt{e_{t} / T}$). The outcome ($\Gamma_t = Y_t / e_{t}$) is weighted by $h_t$ and added to the arm's local numerator, while $h_t$ is added to the denominator.

At the end of each phase, per-arm accumulators (`ispw_num[i]`, `ispw_den[i]`) are folded **directly** into a global estimate accumulator (`global_num`, `global_den`) and then zeroed. The final join size is computed entirely from this global mean probability:

$$\hat{J} = \frac{\text{globalnum}}{\text{globalden}} \times |R| \times |S| \mathbin{/} \text{globalden}$$

---

# Pooled Rate Estimator

## Exploration Cache Duplicate Prevention

If the the number of tuples left in `R` limited the exploration cache size such that the exploration cache size is $\leq$ the exploitation cache size limit, then fill the exploitation cache with all exploration tuples. This must the maximum set of unique explotation tuples.

Otherwise, it must be possible to fill the exploitation cache without duplicates as follows:

The exploitation cache is constructed duplicate-free, rather than being duplicate-free by post fill inspection and replacement. 

Each tuple starts with a weight equal to its incremented exploration phase rewards. To select tuples, the algorithm:
- generates a random treshold in the range of 1 to the sum of all weights of unchosen tuples
- iterates through tuples and accumulates their weights, selecting the first tuple whose weight brings the cumulatitive weight to the treshold

After an tuple is selected, its weight is zeroed. This limits subsequent selections to unchosen tuples. 

## Discrete Event
 
The discrete event is a **single probe** (one $(r_{i}, s_{j})$ join attempt).
 
## Probabilities by Case
 
### Exploration Selection
The probability of selecting a random tuple for exploration is denoted by $p_{R}(r)$.
 
$$p_{R}(r) = \frac{\text{explorationsize}}{|R|}$$
 
### *Discrete Event Case*: Exploration Probe (in First N probes per tuple)
The dependent probability that a tuple is probed each of the first N times in exploration, given that it was selected for exploration, is 1. 

Thereby, the overall probability of that a tuple is probed the first N times in exploration is
$$e_{t} = p_{R}(r) \cdot 1 = \frac{\text{explorationsize}}{|R|}$$
 
Since all discrete events are dependent on tuple $r$ being chosen for exploration, we will define $\hat{e_{t}}$ as conditional probability for discrete events given that the $r$ tuple has already been selected for exploration.
 
$$\hat{e_{t}} = 1.0$$
 
### *Discrete Event Case*: Exploration Probe (Additional probes beyond N) (Curr Version)
*Note: Tested in current implementation, but suspected to only be correct for N+1th probe and not further probes. See following section for planned correction.*

Probability for each probe beyond N times in exploration is dependent on the probability that the tuple was chosen for exploration and the probability probed the first N times. 

Given these preconditions, an arm continues to be probed beyond N only if it accumulated at least one success within its first N probes, meaning it was not retired by the N-failure condition. Given the arm's empirical match rate $p_{su}(r)$, the probability of this discrete event is as follows:
 
$$
\begin{aligned}
e_{t} &= p_{R}(r) \cdot 1 \cdot (1 - (1 - p_{su}(r))^N) \\
&=  \frac{\text{explorationsize}}{|R|}*(1 - (1 - \frac{\text{totalrewards}_{i}}{\text{totalprobes}_{i}})^N )\\
&=  \frac{\text{explorationsize}}{|R|}*(1 - (1 - \frac{\text{totalrewards}_{i}}{\text{totalrewards}_{i} + \text{totalfailures}_{i}})^N )
\end{aligned}
$$
 
Thereby, the conditional probability for a $r$ tuple being probed each time beyond the first N probes, given that the $r$ tuple has already been selected for exploration, is just dependent on having been probed the first N times with probability 1 as follows:
 
$$
\begin{aligned}
\hat{e_{t}} &= 1 \cdot  (1 - (1 - p_{su}(r))^N) \\
&=  1 - (1 - \frac{\text{totalrewards}_{i}}{\text{totalrewards}_{i} + \text{totalfailures}_{i}})^N 
\end{aligned}
$$

### *Discrete Event Case*: Exploration Probe (Additional probes beyond N) (To be implemented)
*Note: Untested correction, suspected to correct probability for probes beyond N+1th)*

For $j > 0$, probability for N+$j\text{th}$ probe in exploration is dependent on the following three conditions:
- the tuple was chosen for exploration, with probability $p_{R}(r)$
- the tuple was probed the first N times, with probability 1
- the tuple was accumulated less than $N$ failures, with probability that will be derived below 

Since rewards per probe are in {0,1} for success or failure: $$\text{numfailures} = \text{numprobes} - \text{numrewards}$$
Thereby the **conditional** probability that the N+$j\text{th}$ exploration probe passes the $N$-Failure condition, given the tuple was chosen for exploration and the tuple was probed the first N times, is: 

$$
\begin{aligned}
\hat{e_{t}} &= P(\text{totalfailures}_{i} < N \text{, in } (N+j-1) \text{ probes}) \\
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
e_{t} &= p_{R}(r) \cdot 1 \cdot  P(\text{totalfailures}_{i} < N \text{, in } (N+j-1) \text{ probes}) \\
&=  \frac{\text{explorationsize}}{|R|}*\sum_{k = j}^{N+j - 1} \binom{N+j-1}{k} (p_{su}(r))^k (1 - p_{su}(r))^{(N+j-1)-k}
\end{aligned}
$$

### Exploitation Selection
An exploitation cache of up to `exploitation_size` tuples is built as described above. Each tuple's selection probability is **conditional** given selection for exploration, influenced by previously selected tuples, and fixed at selection time as follows:
 
$$
p_{\text{exploit},i} = \frac{\text{explorationRewards}_{i} + 1}{ \text{explorationSize} - \text{numPrevChosen} + \sum_{j \in \text{Unchosen}} \text{explorationRewards}_{j}}
$$
 
### *Discrete Event Case*: Exploitation Probe
Every tuple in the fixed exploitation cache is probed against every remaining opposite relation tuple following the last probed in by any exploration cache tuple in exploration. Given that a tuple was selected for exploration, the **conditional** probability that a the tuple is probed against any opposite relation tuple in exploitation is the probability that the tuple was selected for exploitation; this probability is the tuple specific probability that was fixed at selection time.
 
$$\hat{e_{t}} = p_{\text{exploit},i} $$

The probability of this discrete event is as follows:

$$e_{t} = p_{R}(r) \cdot p_{\text{exploit},i}= \frac{\text{explorationsize}}{|R|}* p_{\text{exploit},i}$$

 
## Estimate Update Procedure
For every probe in both phases, the arm's per-phase accumulators are updated using a Horvitz-Thompson weighting without any variance-stabilizing multiplier. Here $\text{reward}_t \in \{0, 1\}$ is the binary match outcome of the probe, and $\hat{e_{t}}$ is the probability of the probe given selection for exploration as defined for each case above:
 
$$\text{num}[i] \mathrel{+}= \frac{\text{reward}_t}{\hat{e_{t}}} \qquad \text{den}[i] \mathrel{+}= \frac{1}{\hat{e_{t}}}$$
 
At the end of the round, exploration and exploitation accumulators are pooled per arm to compute an unbiased match rate. These rates are summed across all $n$ arms, multiplied by $|S|$ to project onto the full S relation, and scaled by $\frac{|R|}{\text{explorationsize}}$ to extrapolate from the $n$ sampled arms to the full R relation. Here $n$ is the number of arms actually loaded into the exploration cache in the current round — the lesser of `exploration_size` and the number of tuples remaining in $R$ — which may be smaller than `exploration_size` in the final round when $R$ is exhausted early:
 
$$\hat{J}_{\text{round}} = \sum_{i=1}^{n} \frac{\text{numexplore}[i] + \text{numexploit}[i]}{\text{denexplore}[i] + \text{denexploit}[i]} \times |S| \times \frac{|R|}{\text{explorationsize}}$$

**Why we use $\hat{e_{t}}$ and multiply by $p_{R}(r)$, instead of using $e_{t}$:**

Every probe event across phases is dependent on the selection of a tuple from $R$ for exploration with probability $p_{R}(r)$. Because this probability is identical for every probe, it multiplies every term in both `num[i]` and `den[i]` by the same constant. It therefore cancels exactly in the ratio `num[i] / den[i]`, leaving the pooled rate unchanged regardless of whether $p_{R}(r)$ is included in $e_{t}$ or not. The pooled ratio estimates the match probability *given* the tuple was selected for exploration if the first place with probability $p_{R}(r)$. The multiplier $\frac{|R|}{\text{explorationsize}}$ scales the estimate for tuples in the round(exploration+exploitation) up to represent the full relation $R$. 
 
Across rounds, the join size estimate is the simple average of per-round estimates across all rounds:
 
$$\hat{J} = \frac{1}{K} \sum_{k=1}^{K} \hat{J}_{\text{round},k}$$
