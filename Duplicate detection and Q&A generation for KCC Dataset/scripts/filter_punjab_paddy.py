#!/usr/bin/env python3
"""
Filter CSV file for StateName = PUNJAB and Crop = Paddy Dhan
"""
import sys
import os

# Add parent directory to path to import from utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def filter_punjab_paddy():
    """Filter AI_ANS_25K.csv for PUNJAB state and Paddy Dhan crop"""
    try:
        import pandas as pd
    except ImportError:
        print("Error: pandas not installed. Please run: pip install pandas")
        sys.exit(1)
    
    # File paths
    input_file = '/home/ubuntu/Kshitij/unique-qs/Data/AI_ANS_25K.csv'
    output_file = '/home/ubuntu/Kshitij/unique-qs/Data/PUNJAB_Paddy_Dhan.csv'
    
    print("=" * 70)
    print("FILTERING PUNJAB PADDY DHAN DATA")
    print("=" * 70)
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print()
    
    # Read CSV
    print("Reading CSV file...")
    df = pd.read_csv(input_file)
    print(f"Total rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    print()
    
    # Show unique values for StateName and Crop
    print(f"Unique states: {df['StateName'].nunique()}")
    print(f"Unique crops: {df['Crop'].nunique()}")
    print()
    
    # Filter for PUNJAB and Paddy Dhan
    print("Applying filters:")
    print("  - StateName = 'PUNJAB'")
    print("  - Crop = 'Paddy Dhan'")
    print()
    
    filtered_df = df[(df['StateName'] == 'PUNJAB') & (df['Crop'] == 'Paddy Dhan')]
    
    print(f"Filtered rows: {len(filtered_df):,}")
    print(f"Reduction: {(1 - len(filtered_df)/len(df)) * 100:.1f}%")
    print()
    
    if len(filtered_df) == 0:
        print("WARNING: No rows match the filter criteria!")
        print("\nChecking for similar values...")
        print(f"States containing 'PUNJAB': {df[df['StateName'].str.contains('PUNJAB', case=False, na=False)]['StateName'].unique()}")
        print(f"Crops containing 'Paddy': {df[df['Crop'].str.contains('Paddy', case=False, na=False)]['Crop'].unique()}")
        sys.exit(1)
    
    # Save filtered data
    print(f"Saving to: {output_file}")
    filtered_df.to_csv(output_file, index=False)
    print("✓ File saved successfully!")
    print()
    
    # Show sample of filtered data
    print("Sample of filtered data (first 5 rows):")
    print("-" * 70)
    for idx, row in filtered_df.head(5).iterrows():
        print(f"Row {idx + 1}:")
        print(f"  District: {row.get('DistrictName', 'N/A')}")
        print(f"  State: {row['StateName']}")
        print(f"  Crop: {row['Crop']}")
        print(f"  Query: {row.get('QueryText', 'N/A')[:80]}...")
        print()
    
    print("=" * 70)
    print("FILTERING COMPLETE")
    print("=" * 70)
    
    return output_file

if __name__ == '__main__':
    filter_punjab_paddy()
