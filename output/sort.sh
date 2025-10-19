#!/bin/bash

# Create "sorted" subdirectory if it doesn't exist
mkdir -p sorted

# Loop through all .txt files in the current directory
for file in *.txt; do
    # Check if any .txt files exist
    [ -e "$file" ] || continue
    
    # Sort the file and save to sorted/ directory
    sort "$file" > "sorted/$file"
done

echo "All .txt files have been sorted and saved in the 'sorted' directory."
