import pandas as pd
import argparse
import re

def filter_dynamic_questions(input_file, output_file, column='QueryText'):
    """
    Filters out dynamic questions (weather, market prices) from the dataset.
    """
    print(f"Loading data from {input_file}...")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return

    total_rows = len(df)
    print(f"Total rows: {total_rows}")
    
    # Ensure column exists
    if column not in df.columns:
        print(f"Error: Column '{column}' not found in dataset!")
        return

    # Define keywords/patterns to remove
    # 1. Weather related
    weather_keywords = [
        r'\bweather\b', r'\bforecast\b', r'\brain\b', r'\bmonsoon\b', 
        r'\btemperature\b', r'\bclouds\?\b', r'\bwind\b', r'\bclimate\b',
        r'\bmausam\b', r'\bbarish\b', r'\bpani kab\b', r'\bbarasat\b',
        r'\bwill it rain\b', r'\bwhen.*\brain\b'
    ]
    
    # 2. Market/Price related
    price_keywords = [
        r'\bmarket price\b', r'\bmandi price\b', r'\bmandi rate\b', 
        r'\bcurrent rate\b', r'\today\'?s rate\b', r'\btoday\'?s price\b',
        r'\baaj ka bhav\b', r'\bmandi bhav\b', r'\bwhat is the price\b',
        r'\bcost of.*\b', r'\bkimat\b', r'\bdam\b', r'\brate of.*\b'
    ]
    
    # Combine patterns
    remove_patterns = weather_keywords + price_keywords
    combined_pattern = '|'.join(remove_patterns)
    
    print("\nFiltering criteria:")
    print(f"  - Weather keywords: {len(weather_keywords)}")
    print(f"  - Price/Market keywords: {len(price_keywords)}")
    
    # Filter
    # We want to KEEP rows that DO NOT match the pattern
    # Use fillna(False) to handle existing NaNs (keep them or drop them? usually drop empty text)
    
    # First drop empty questions
    df_clean = df.dropna(subset=[column])
    dropped_empty = total_rows - len(df_clean)
    print(f"  - Dropped {dropped_empty} rows with empty {column}")
    
    # Apply regex filter
    # case=False for case insensitive
    mask = df_clean[column].astype(str).str.contains(combined_pattern, case=True, regex=True, flags=re.IGNORECASE)
    
    # Rows to remove
    df_removed = df_clean[mask]
    
    # Rows to keep
    df_final = df_clean[~mask]
    
    removed_count = len(df_removed)
    kept_count = len(df_final)
    
    print(f"\nFiltering Results:")
    print(f"  - Rows Removed (Dynamic): {removed_count}")
    print(f"  - Rows Kept: {kept_count}")
    print(f"  - Reduction: {(removed_count/total_rows)*100:.2f}%")
    
    # Save excluded rows for inspection (optional but good for debugging)
    removed_file = output_file.replace('.csv', '_removed_dynamic.csv')
    df_removed.to_csv(removed_file, index=False)
    print(f"\nSaved removed questions to: {removed_file}")
    
    # Save final result
    df_final.to_csv(output_file, index=False)
    print(f"✅ Saved clean dataset to: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter dynamic questions (weather, prices)")
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output filtered CSV file")
    
    args = parser.parse_args()
    
    filter_dynamic_questions(args.input, args.output)
