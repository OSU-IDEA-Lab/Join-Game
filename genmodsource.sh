#!/bin/bash

# finds all modified files of original commit, for readme updating purposes
# generates shell script to save all to my engr filespace

# 1. Clean up any previous runs to prevent duplicate appends
rm -f modsource.sh

# 2. Get modified files, prepend 'cp' source path, append destination path, 
# and pipe it all directly into mod_source.txt
git diff --name-only --diff-filter=M 76fd6c1 HEAD | \
sed 's|^|cp /data/jinjo/alt/Join-Game/|' | \
sed 's|$| /nfs/stak/users/jinjo/research/|' > mod_source.txt

# 3. Run your awk command to fix the destination filenames and output to the final script
awk '{ match($2, /\/[^\/]*$/); if (RSTART > 0) $NF = $NF substr($2, RSTART + 1); print }' mod_source.txt >> modsource.sh

# 4. Make the newly created script executable
chmod +x modsource.sh

echo "Success! modsource.sh has been generated and is ready to run."