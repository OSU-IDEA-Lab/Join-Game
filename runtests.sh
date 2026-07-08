#!/bin/bash
 
# Generates the current date in M_D format (e.g., 6_16)
SUFFIX=$(date +"%-m_%-d")
 
# VERSION="ProbNFailure_HowardCI"
VERSION="SingleM_AdaptiveWeights"
 
# If you prefer to include the time (e.g., 6_16_13_45 for 1:45 PM), 
# comment out the line above and uncomment the line below:
# SUFFIX=$(date +"%-m_%-d_%H_%M")
 
# Define the command using the dynamic suffix
nohup python3 test/tpch_manager.py "${SUFFIX}_${VERSION}_" --workers 10 --limit > "tpch_manager_${SUFFIX}.log" 2>&1 &
 
# Print a confirmation message with the log file name
echo "Process started in the background!"
echo "Outputting to: tpch_manager_${SUFFIX}.log"