import pandas as pd
from difflib import SequenceMatcher
import re
import os

input_file = r"D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_wheat_questions_consolidated_multipart.csv"
output_file = r"D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_wheat_questions_consolidated_SIMILARITY_MERGED.csv"

print("="*80)
print("CONSOLIDATING SIMILAR QUESTIONS & MERGING METADATA")
print("="*80)

# Read file
df = pd.read_csv(input_file)
print(f"\nLoaded {len(df)} questions")

# Add duplicate_count column if it doesn't exist (count occurrences)
if 'duplicate_count' not in df.columns:
    df['duplicate_count'] = 1
    print("Created duplicate_count column")

# Add Topic column if it doesn't exist - use keyword_group as Topic
if 'Topic' not in df.columns:
    df['Topic'] = df.get('keyword_group', 'OTHER')
    print("Created Topic column from keyword_group")

# No need to filter - already filtered in previous steps
print(f"Processing {len(df)} questions with {df['Topic'].nunique()} topic groups")

# For performance: Use drop_duplicates first to reduce rows
print("\nRemoving exact duplicates first...")
df_unique = df.drop_duplicates(subset=['QueryText'], keep='first')
print(f"After removing exact duplicates: {len(df_unique)} questions")

# Use the unique dataframe for processing
df = df_unique.reset_index(drop=True)

# Quick normalization using vectorized str operations
print("Normalizing text...")
df['norm_text'] = (df['QueryText'].fillna('')
    .str.lower()
    .str.replace(r'information regarding', '', regex=True)
    .str.replace(r'control of', '', regex=True)
    .str.replace(r'management of', '', regex=True)
    .str.replace(r'in wheat', '', regex=True)
    .str.replace(r'of wheat', '', regex=True)
    .str.replace(r'how to', '', regex=True)
    .str.replace(r'what is', '', regex=True)
    .str.replace(r'dose of', '', regex=True)
    .str.replace(r'spray', '', regex=True)
    .str.replace(r'crop', '', regex=True)
    .str.replace(r'[^a-z0-9\s]', '', regex=True)
    .str.strip()
)

# -----------------------------------------------------------------------------
# 2. CLUSTERING & CONSOLIDATION LOGIC
# -----------------------------------------------------------------------------
def consolidate_group(cluster_indices, full_df):
    """
    Merges multiple rows into one, combining their metadata.
    """
    # Get the sub-dataframe for this cluster
    group_df = full_df.loc[cluster_indices]
    
    # Representative question: The one with the highest duplicate_count (most frequent)
    # or the shortest one if counts are equal (usually cleaner)
    representative_row = group_df.sort_values(['duplicate_count', 'QueryText'], ascending=[False, True]).iloc[0]
    
    consolidated = {}
    consolidated['Topic'] = representative_row['Topic']
    consolidated['QueryText'] = representative_row['QueryText']
    
    # Sum duplicate counts
    consolidated['duplicate_count'] = group_df['duplicate_count'].sum()
    
    # Consolidate Metadata Columns
    # We will join unique values with a comma
    metadata_cols = [
        'BlockName', 'Category', 'Year', 'Month', 'Day', 
        'DistrictName', 'QueryType', 'Season', 'Sector', 
        'StateName', 'keyword_group', 'Crop'
    ]
    
    for col in metadata_cols:
        if col in group_df.columns:
            all_values = []
            for val in group_df[col].dropna().astype(str):
                # Split existing comma-separated values to avoid double nesting
                parts = [p.strip() for p in val.split(',')]
                all_values.extend(parts)
            
            # Get unique values and sort them
            unique_values = sorted(list(set(all_values)))
            # Join with comma
            consolidated[col] = ', '.join(unique_values)
            
    return consolidated

final_rows = []
processed_indices = set()

# We process topic by topic
topics = df['Topic'].unique()

print(f"Processing {len(topics)} topics...")

for topic in topics:
    # Get all rows for this topic
    topic_df = df[df['Topic'] == topic]
    
    # Sort by frequency so we start with the most common questions as cluster centers
    topic_df = topic_df.sort_values('duplicate_count', ascending=False)
    
    indices = topic_df.index.tolist()
    local_processed = set()
    
    # Dynamic Thresholds based on Topic
    # Aphids/Weeds/Rust are very specific, so we can be aggressive (lower threshold)
    # Uncategorized needs to be careful (higher threshold)
    if topic in ['Aphids', 'Weeds', 'Rust', 'Varieties', 'Sowing', 'Fertilizer']:
        THRESHOLD = 0.55  # Aggressive merging for known topics
    else:
        THRESHOLD = 0.75  # Conservative for others
        
    for i in indices:
        if i in local_processed:
            continue
            
        current_cluster = [i]
        local_processed.add(i)
        base_text = df.loc[i, 'norm_text']
        
        for j in indices:
            if j in local_processed:
                continue
                
            compare_text = df.loc[j, 'norm_text']
            
            # Calculate similarity
            if not base_text and not compare_text:
                sim = 1.0
            else:
                sim = SequenceMatcher(None, base_text, compare_text).ratio()
            
            if sim >= THRESHOLD:
                current_cluster.append(j)
                local_processed.add(j)
        
        # Consolidate this cluster
        consolidated_row = consolidate_group(current_cluster, df)
        final_rows.append(consolidated_row)

# Create final DataFrame
final_df = pd.DataFrame(final_rows)

# Sort final result by Topic (to group similar questions) and then by frequency
print("Sorting by Topic and Frequency...")
final_df = final_df.sort_values(by=['Topic', 'duplicate_count'], ascending=[True, False])

# Save
final_df.to_csv(output_file, index=False)

print(f"\n✅ Consolidation Complete!")
print(f"Original Rows: {len(df)}")
print(f"Consolidated Rows: {len(final_df)}")
print(f"Reduction: {len(df) - len(final_df)} rows merged")
print(f"Saved to: {output_file}")

print("\nTop 5 Consolidated Questions:")
for i, row in final_df.head(5).iterrows():
    print(f"[{row['Topic']}] {row['QueryText']} (Count: {row['duplicate_count']})")
