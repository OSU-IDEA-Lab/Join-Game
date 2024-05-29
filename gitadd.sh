#!/bin/bash

# This bash script adds all files to staging.

# Loop through each item in the current directory
for item in * .*; do
    # Check if the item is not the DemoDir and not the special directories '.' and '..'
    if [[ "$item" != "DemoDir" && "$item" != "." && "$item" != ".." ]]; then
        # Add it to git
        git add "$item"
    fi
done

# Check if the branch exists in the local repository
if git show-ref --quiet refs/heads/ORJ-BI; then
    # Checkout the RippleJoin branch if it exists
    git checkout ORJ-BI
else
    # Create the RippleJoin branch and switch to it
    git checkout -b ORJ-BI
fi

# Commit the changes
git commit -m "ORJ-BI"

# Push the changes to the RippleJoin branch on the remote
# git push IDEA -u ORJ-BI
