#!/usr/bin/env python3
"""
Multi-State Paddy Data Filtering Script

Filters KCC datasets for specific states and Paddy (Dhan) crop,
creating separate CSV files for each state.
"""

import pandas as pd
from pathlib import Path
import sys

# Target states (uppercase as they appear in data)
STATES = [
    'TAMIL NADU',
    'WEST BENGAL',
    'ODISHA',
    'HARYANA',
    'BIHAR',
    'MADHYA PRADESH',
    'CHHATTISGARH',
    'ANDHRA PRADESH',
    'TELANGANA',
    'UTTARAKHAND',
    'KARNATAKA',
    'MAHARASHTRA'
]

# State name mapping for filenames (remove spaces, title case)
STATE_FILENAME_MAP = {
    'TAMIL NADU': 'TamilNadu',
    'WEST BENGAL': 'WestBengal',
    'ODISHA': 'Odisha',
    'HARYANA': 'Haryana',
    'BIHAR': 'Bihar',
    'MADHYA PRADESH': 'MadhyaPradesh',
    'CHHATTISGARH': 'Chhattisgarh',
    'ANDHRA PRADESH': 'AndhraPradesh',
    'TELANGANA': 'Telangana',
    'UTTARAKHAND': 'Uttarakhand',
    'KARNATAKA': 'Karnataka',
    'MAHARASHTRA': 'Maharashtra'
}

# Paddy crop variations to check
PADDY_VARIATIONS = ['Paddy (Dhan)', 'Paddy Dhan', 'PADDY (DHAN)', 'PADDY DHAN', 'Paddy(Dhan)']

# Input files
INPUT_FILES = [
    'Data/All Datasets/kcc_dataset_part_1.csv',
    'Data/All Datasets/kcc_dataset_part_2.csv',
    'Data/All Datasets/kcc_dataset_part_3.csv',
    'Data/All Datasets/kcc_dataset_part_4.csv',
    'Data/All Datasets/kcc_dataset_part_5.csv'
]

# Output directory
OUTPUT_DIR = Path('Data/State_Paddy')

def main():
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Created output directory: {OUTPUT_DIR}")
    
    # Initialize state data collectors
    state_data = {state: [] for state in STATES}
    
    # Process each input file
    for file_idx, input_file in enumerate(INPUT_FILES, 1):
        print(f"\n{'='*70}")
        print(f"Processing file {file_idx}/5: {input_file}")
        print(f"{'='*70}")
        
        try:
            # Read file in chunks to handle large size
            chunk_size = 500000
            chunk_count = 0
            
            for chunk in pd.read_csv(input_file, chunksize=chunk_size):
                chunk_count += 1
                
                # Filter for target states and Paddy crop
                mask = (chunk['StateName'].isin(STATES)) & (chunk['Crop'].isin(PADDY_VARIATIONS))
                filtered = chunk[mask]
                
                # Group by state
                for state in STATES:
                    state_chunk = filtered[filtered['StateName'] == state]
                    if len(state_chunk) > 0:
                        state_data[state].append(state_chunk)
                
                # Progress update
                if chunk_count % 10 == 0:
                    total_filtered = sum(len(data) for data_list in state_data.values() for data in data_list)
                    print(f"  Processed {chunk_count * chunk_size:,} rows, filtered: {total_filtered:,}")
            
            print(f"✓ Completed {input_file}")
            
        except Exception as e:
            print(f"✗ Error processing {input_file}: {e}")
            continue
    
    # Save state files
    print(f"\n{'='*70}")
    print("Saving state files...")
    print(f"{'='*70}")
    
    summary = []
    for state in STATES:
        if state_data[state]:
            # Concatenate all chunks for this state
            state_df = pd.concat(state_data[state], ignore_index=True)
            
            # Save to file
            filename = STATE_FILENAME_MAP[state]
            output_file = OUTPUT_DIR / f"{filename}_Paddy_Raw.csv"
            state_df.to_csv(output_file, index=False)
            
            print(f"✓ {state}: {len(state_df):,} rows -> {output_file}")
            summary.append({'State': state, 'Rows': len(state_df), 'File': str(output_file)})
        else:
            print(f"✗ {state}: No data found")
            summary.append({'State': state, 'Rows': 0, 'File': 'N/A'})
    
    # Print summary
    print(f"\n{'='*70}")
    print("FILTERING SUMMARY")
    print(f"{'='*70}")
    total_rows = sum(s['Rows'] for s in summary)
    print(f"Total rows filtered: {total_rows:,}")
    print(f"States with data: {sum(1 for s in summary if s['Rows'] > 0)}/12")
    print(f"\nPer-state breakdown:")
    for s in summary:
        if s['Rows'] > 0:
            print(f"  {s['State']}: {s['Rows']:,} rows")
    
    print(f"\n✓ Filtering complete!")

if __name__ == "__main__":
    main()
