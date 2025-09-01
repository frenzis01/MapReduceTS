#!/bin/bash

# Enable nullglob so *.txt expands to nothing if no files exist
shopt -s nullglob

# Delete all .txt files in the current directory
rm -f  -- output/*.txt
rm -rf output/sorted

echo "All .txt files in the output directory have been deleted."
