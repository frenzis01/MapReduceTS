#!/bin/bash

# Define output directory
OUTPUT_DIR="output"
SORTED_DIR="output/sorted"

# Execute sort.sh first
echo "Sorting files..."
cd "$OUTPUT_DIR"
./sort.sh
cd ..

echo ""
echo "========================================="
echo "Comparing sorted files..."
echo "========================================="
echo ""

# Find the worker prefix (assuming all files have the same prefix)
worker_prefix=$(ls "$SORTED_DIR"/*_seq-word-count-lorem.txt_result.txt 2>/dev/null | head -1 | xargs basename | sed 's/_seq-word-count-lorem.txt_result.txt//')

if [ -z "$worker_prefix" ]; then
    echo "Error: Could not find files with expected pattern in $SORTED_DIR"
    exit 1
fi

echo "Using worker prefix: $worker_prefix"
echo ""

# Define file pairs to compare
declare -A file_pairs=(
    ["lorem"]="${worker_prefix}_seq-word-count-lorem.txt_result.txt ${worker_prefix}_word-count-lorem.txt*_result.txt"
    ["loremshort"]="${worker_prefix}_seq-word-count-loremshort.txt_result.txt ${worker_prefix}_word-count-loremshort.txt*_result.txt"
)

# Counter for differences
differences_found=0

# Compare each pair
for key in "${!file_pairs[@]}"; do
    read -r seq_file word_file <<< "${file_pairs[$key]}"
    
    # Prepend sorted directory to file paths
    seq_file="$SORTED_DIR/$seq_file"
    
    # Expand wildcard for word_file
    word_file_expanded=$(ls "$SORTED_DIR"/$word_file 2>/dev/null | head -1)
    
    echo "-------------------------------------------"
    echo "Comparing: $key"
    echo "  Sequential: $seq_file"
    echo "  Parallel:   $word_file_expanded"
    echo ""
    
    if [ ! -f "$seq_file" ]; then
        echo "  ❌ ERROR: File not found: $seq_file"
        differences_found=$((differences_found + 1))
        continue
    fi
    
    if [ ! -f "$word_file_expanded" ]; then
        echo "  ❌ ERROR: File not found: $word_file_expanded"
        differences_found=$((differences_found + 1))
        continue
    fi
    
    # Compare the files
    if diff -q "$seq_file" "$word_file_expanded" > /dev/null; then
        echo "  ✅ Files are IDENTICAL"
    else
        echo "  ❌ Files are DIFFERENT"
        echo ""
        echo "  Differences:"
        diff "$seq_file" "$word_file_expanded" | head -20
        differences_found=$((differences_found + 1))
    fi
    echo ""
done

echo "========================================="
echo "Summary"
echo "========================================="
if [ $differences_found -eq 0 ]; then
    echo "✅ All file pairs are identical!"
    exit 0
else
    echo "❌ Found differences in $differences_found file pair(s)"
    exit 1
fi
