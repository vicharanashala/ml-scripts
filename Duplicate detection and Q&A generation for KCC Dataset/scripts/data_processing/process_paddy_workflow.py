#!/usr/bin/env python3
"""
Complete Paddy Processing Workflow

Steps:
1. Filter Paddy Crop from PB_Combined_Cleaned.csv
2. Deduplicate filtered data
3. Generate Q&A for unique questions
4. Merge with existing PUNJAB_Paddy_Dhan.csv
5. Final deduplication of merged data
"""

import pandas as pd
import subprocess
import sys
import os

print("="*70)
print("PADDY DATA PROCESSING WORKFLOW")
print("="*70)

# Step 1: Filter Paddy Crop
print("\nStep 1: Filtering Paddy (Dhan) crop...")
df_all = pd.read_csv('Data/PB_Combined_Cleaned.csv')
print(f"  Total rows: {len(df_all)}")

df_paddy = df_all[df_all['Crop'] == 'Paddy (Dhan)']
print(f"  Paddy rows: {len(df_paddy)}")

df_paddy.to_csv('Data/PB_Paddy_Filtered_New.csv', index=False)
print(f"  Saved to: Data/PB_Paddy_Filtered_New.csv")

# Step 2: Deduplication
print("\nStep 2: Deduplicating Paddy data...")
print("  Running deduplication pipeline...")

result = subprocess.run([
    'python', 'scripts/data_processing/deduplicate_questions.py',
    '--input', 'Data/PB_Paddy_Filtered_New.csv',
    '--output', 'outputs/deduplicated/PB_Paddy_Unique_New.csv',
    '--column', 'QueryText'
], env={**os.environ, 'PYTHONPATH': '/home/ubuntu/Kshitij/unique-qs'}, 
   capture_output=True, text=True)

if result.returncode != 0:
    print("  ERROR in deduplication:")
    print(result.stderr)
    sys.exit(1)

# Parse deduplication results
df_unique = pd.read_csv('outputs/deduplicated/PB_Paddy_Unique_New.csv')
print(f"  Unique questions: {len(df_unique)}")

# Step 3: Q&A Generation
print("\nStep 3: Generating Q&A pairs...")
print(f"  Processing {len(df_unique)} unique questions...")
print("  NOTE: This step requires API key and will be done separately")
print("  Skipping for now - will use existing PB_Paddy_QA_Final.csv")

# Check if Q&A file exists
qa_file = 'outputs/qa_results/PB_Paddy_QA_Final.csv'
if os.path.exists(qa_file):
    df_qa = pd.read_csv(qa_file)
    print(f"  Using existing Q&A file: {len(df_qa)} rows")
else:
    print(f"  WARNING: Q&A file not found: {qa_file}")
    print(f"  Please run Q&A generation manually")
    df_qa = df_unique.copy()
    df_qa['Question'] = ''
    df_qa['Answer'] = ''

# Step 4: Merge with existing PUNJAB_Paddy_Dhan.csv
print("\nStep 4: Merging with existing PUNJAB_Paddy_Dhan.csv...")
df_existing = pd.read_csv('Data/PUNJAB_Paddy_Dhan.csv')
print(f"  Existing data rows: {len(df_existing)}")
print(f"  New Q&A rows: {len(df_qa)}")

# Ensure both have same columns
common_cols = list(set(df_existing.columns) & set(df_qa.columns))
print(f"  Common columns: {len(common_cols)}")

df_merged = pd.concat([df_existing[common_cols], df_qa[common_cols]], ignore_index=True)
print(f"  Merged rows: {len(df_merged)}")

df_merged.to_csv('outputs/cleaned_data/PB_Paddy_Merged.csv', index=False)
print(f"  Saved to: outputs/cleaned_data/PB_Paddy_Merged.csv")

# Step 5: Final deduplication
print("\nStep 5: Final deduplication of merged data...")
print("  Running deduplication pipeline on merged data...")

# Check which column to use for deduplication
if 'Question' in df_merged.columns and df_merged['Question'].notna().any():
    dedup_column = 'Question'
    print(f"  Using 'Question' column for deduplication")
else:
    dedup_column = 'QueryText'
    print(f"  Using 'QueryText' column for deduplication")

result = subprocess.run([
    'python', 'scripts/data_processing/deduplicate_questions.py',
    '--input', 'outputs/cleaned_data/PB_Paddy_Merged.csv',
    '--output', 'outputs/deduplicated/PB_Paddy_Final_Unique.csv',
    '--column', dedup_column
], env={**os.environ, 'PYTHONPATH': '/home/ubuntu/Kshitij/unique-qs'},
   capture_output=True, text=True)

if result.returncode != 0:
    print("  ERROR in final deduplication:")
    print(result.stderr)
    sys.exit(1)

df_final = pd.read_csv('outputs/deduplicated/PB_Paddy_Final_Unique.csv')
print(f"  Final unique rows: {len(df_final)}")

print("\n" + "="*70)
print("WORKFLOW COMPLETE!")
print("="*70)
print(f"\nSummary:")
print(f"  Step 1 - Filtered: {len(df_paddy)} Paddy rows")
print(f"  Step 2 - Deduplicated: {len(df_unique)} unique questions")
print(f"  Step 3 - Q&A Generated: {len(df_qa)} rows")
print(f"  Step 4 - Merged: {len(df_merged)} total rows")
print(f"  Step 5 - Final Unique: {len(df_final)} rows")
print(f"\nFinal output: outputs/deduplicated/PB_Paddy_Final_Unique.csv")
print("="*70)
