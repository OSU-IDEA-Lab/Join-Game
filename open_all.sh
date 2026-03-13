#!/bin/bash

# Executor implementations
code src/backend/executor/nodeHash.c
code src/backend/executor/nodeHashjoin.c

# Optimizer implementation
code src/backend/optimizer/plan/createplan.c

# Headers
code src/include/executor/nodeHash.h
code src/include/executor/hashjoin.h
code src/include/nodes/execnodes.h