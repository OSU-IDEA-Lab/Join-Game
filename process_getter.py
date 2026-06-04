import re

input_file = "/data/saketh_temp/PSQL1506/Join-Game/DemoDir/log/postgresql-2025-12-14_201421.log"
regex = re.compile(r"INFO:  This is ([a-zA-Z_]*)")
functions = set()

with open(input_file, "r", errors="ignore") as f:
    for line in f:
        m = regex.search(line)
        if m:
            functions.add(m.group(1))

for f in functions:
    print(f)