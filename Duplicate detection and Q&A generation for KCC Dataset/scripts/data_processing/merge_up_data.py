import pandas as pd
import glob
import os
import argparse

def merge_up_datasets(input_dir, output_file):
    """
    Merges all CSV files in the input directory into a single CSV file.
    """
    print(f"Searching for CSV files in {input_dir}...")
    
    # Handle spaces in directory path correctly
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    if not csv_files:
        print("No CSV files found!")
        return
    
    print(f"Found {len(csv_files)} files:")
    for f in csv_files:
        print(f"  - {os.path.basename(f)}")
        
    dfs = []
    total_rows = 0
    
    for file in csv_files:
        try:
            # Read CSV - try utf-8 first, then latin1 if that fails
            try:
                df = pd.read_csv(file)
            except UnicodeDecodeError:
                print(f"  ⚠️ Warning: UTF-8 decode failed for {file}, trying latin1...")
                df = pd.read_csv(file, encoding='latin1')
                
            print(f"  Reading {os.path.basename(file)}: {len(df)} rows")
            dfs.append(df)
            total_rows += len(df)
        except Exception as e:
            print(f"  ❌ Error reading {file}: {e}")
            
    if not dfs:
        print("No data loaded.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)
    
    print(f"\nTotal rows combined: {len(combined_df)}")
    
    # Save to output file
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    combined_df.to_csv(output_file, index=False)
    print(f"✅ Saved merged dataset to: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge UP datasets")
    parser.add_argument("--input-dir", required=True, help="Directory containing UP CSV files")
    parser.add_argument("--output", required=True, help="Path for the combined output CSV")
    
    args = parser.parse_args()
    
    merge_up_datasets(args.input_dir, args.output)
