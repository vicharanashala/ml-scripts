#!/bin/bash
# Deduplication script for all state Paddy datasets

echo "======================================================================="
echo "MULTI-STATE PADDY DEDUPLICATION PIPELINE"
echo "======================================================================="

# Array of states (excluding Tamil Nadu - no data)
states=("WestBengal" "Odisha" "Haryana" "Bihar" "MadhyaPradesh" "Chhattisgarh" "AndhraPradesh" "Telangana" "Uttarakhand" "Karnataka" "Maharashtra")

# Process each state
for state in "${states[@]}"; do
    echo ""
    echo "======================================================================="
    echo "Processing: $state"
    echo "======================================================================="
    
    input_file="Data/State_Paddy/${state}_Paddy_Raw.csv"
    output_file="outputs/final/${state}_Paddy_Unique.csv"
    
    if [ -f "$input_file" ]; then
        PYTHONPATH=. python scripts/data_processing/deduplicate_questions.py \
            --input "$input_file" \
            --output "$output_file" \
            --column QueryText
        
        if [ $? -eq 0 ]; then
            echo "✓ $state deduplication complete"
        else
            echo "✗ $state deduplication failed"
        fi
    else
        echo "✗ Input file not found: $input_file"
    fi
done

echo ""
echo "======================================================================="
echo "DEDUPLICATION COMPLETE"
echo "======================================================================="
