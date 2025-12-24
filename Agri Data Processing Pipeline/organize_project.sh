#!/bin/bash
# Project Organization Script
# Organizes files into proper directory structure for GitHub

echo "=========================================="
echo "Organizing Project Structure"
echo "=========================================="

# Create organized directory structure
mkdir -p logs
mkdir -p outputs/intermediate
mkdir -p outputs/final
mkdir -p outputs/reports
mkdir -p outputs/batch_files
mkdir -p scripts/pipelines
mkdir -p docs/reports

# Move log files to logs directory
echo "Moving log files..."
mv -f *.log logs/ 2>/dev/null || true

# Move intermediate output files
echo "Organizing output files..."
mv -f outputs/step1_filtered_punjab_paddy.csv outputs/intermediate/ 2>/dev/null || true
mv -f outputs/step2_deduplicated.csv outputs/intermediate/ 2>/dev/null || true
mv -f outputs/step3_with_qa.csv outputs/intermediate/ 2>/dev/null || true
mv -f outputs/step4_merged.csv outputs/intermediate/ 2>/dev/null || true

# Move final output files
mv -f outputs/final_deduplicated_punjab_paddy.csv outputs/final/ 2>/dev/null || true
mv -f outputs/final_paddy_relevant_only.csv outputs/final/ 2>/dev/null || true
mv -f outputs/final_paddy_relevant_only_non_paddy_removed.csv outputs/final/ 2>/dev/null || true

# Move report files
mv -f outputs/*.report.txt outputs/reports/ 2>/dev/null || true
mv -f outputs/PIPELINE_EXECUTION_SUMMARY.md outputs/reports/ 2>/dev/null || true

# Move batch files
mv -f outputs/*.jsonl outputs/batch_files/ 2>/dev/null || true

# Move pipeline scripts
echo "Organizing scripts..."
mv -f process_pipeline.py scripts/pipelines/ 2>/dev/null || true
mv -f filter_paddy_relevant.py scripts/pipelines/ 2>/dev/null || true
mv -f populate_season.py scripts/pipelines/ 2>/dev/null || true

echo ""
echo "=========================================="
echo "Organization Complete!"
echo "=========================================="
echo ""
echo "New directory structure:"
tree -L 2 -I 'venv|__pycache__|*.pyc|Data' .
