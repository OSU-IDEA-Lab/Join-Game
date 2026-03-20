from collections import defaultdict


class BasicJoin:
    def __init__(self, file_r, key_r, file_s, key_s):
        self.file_r = file_r
        self.key_r  = key_r
        self.file_s = file_s
        self.key_s  = key_s

        self.hash_table = defaultdict(list)  # built from R during join
        self.results    = []
        self.r_count    = 0                  # rows indexed from R
        self.s_count    = 0                  # rows probed from S

    def join(self, R, S):
        """
        Classic two-phase hash join.
        Phase 1: build hash table over R keyed on key_r.
        Phase 2: probe with every row of S on key_s.
        Returns list of (r_row, s_row) match pairs.
        """
        self.hash_table.clear()
        self.results = []
        self.r_count = 0
        self.s_count = 0

        # Phase 1: build
        for row in R:
            self.hash_table[row[self.key_r]].append(row)
            self.r_count += 1

        # Phase 2: probe
        for s_row in S:
            self.s_count += 1
            for r_row in self.hash_table[s_row[self.key_s]]:
                self.results.append((r_row, s_row))

        return self.results
