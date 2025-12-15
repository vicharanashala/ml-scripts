import pandas as pd
import os
from collections import defaultdict

# File paths - Yahan apni files ke paths daalein
FILE_PATHS = [
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_1.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_2.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_3.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_4.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_5.csv"
]

# Excluded states
EXCLUDED_STATES = [
    "PUNJAB",
    "KARNATAKA",
    "MAHARASHTRA",
    "TAMIL NADU",
    "TAMILNADU",
    "UTTAR PRADESH",
    "WEST BENGAL"
]

def analyze_crop_queries(file_paths, excluded_states):
    """
    Saari files ko read karke crop queries analyze karta hai (excluding specific states)
    """
    # Results store karne ke liye (Dynamic dictionary)
    state_crop_counts = defaultdict(lambda: defaultdict(int))
    
    print("Files process ho rahi hain...\n")
    
    for file_path in file_paths:
        if not os.path.exists(file_path):
            print(f"⚠️ File nahi mili: {file_path}")
            continue
            
        print(f"Processing: {os.path.basename(file_path)}")
        
        try:
            # Pehle sirf columns check karne ke liye thoda sa read karein
            df_preview = pd.read_csv(file_path, nrows=5)
            
            # Column names print karein (har file ke liye)
            print(f"\nAvailable columns in {os.path.basename(file_path)}: {list(df_preview.columns)}\n")
            
            # Yahan column names ko apni dataset ke hisaab se adjust karein
            # Common possibilities:
            state_col = None
            crop_col = None
            
            # State column dhoondhein
            for col in df_preview.columns:
                col_lower = col.lower().strip()
                if any(word in col_lower for word in ['state', 'region', 'area']):
                    state_col = col
                    break
            
            # Crop column dhoondhein
            for col in df_preview.columns:
                col_lower = col.lower().strip()
                if any(word in col_lower for word in ['crop', 'commodity', 'product']):
                    crop_col = col
                    break
            
            if state_col is None or crop_col is None:
                print(f"❌ State ya Crop column nahi mila in {os.path.basename(file_path)}")
                print(f"   Available columns: {list(df_preview.columns)}")
                continue
                
            # Ab chunking ke saath poori file process karein
            chunk_size = 100000
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False, on_bad_lines='skip'):
                if state_col not in chunk.columns or crop_col not in chunk.columns:
                    continue

                # Normalize state names for comparison
                chunk[state_col] = chunk[state_col].astype(str).str.upper().str.strip()
                
                # Filter out excluded states
                mask = ~chunk[state_col].isin([s.upper() for s in excluded_states])
                filtered_chunk = chunk[mask]
                
                if filtered_chunk.empty:
                    continue

                # Group by State and Crop
                grouped = filtered_chunk.groupby([state_col, crop_col]).size()
                
                for (state, crop), count in grouped.items():
                    state_crop_counts[state][crop] += count
            
        except Exception as e:
            print(f"❌ Error in {os.path.basename(file_path)}: {str(e)}\n")
            continue
    
    return state_crop_counts

def display_results(state_crop_counts):
    """
    Results ko display karta hai
    """
    print("\n" + "="*80)
    print("CROP QUERY ANALYSIS RESULTS")
    print("="*80 + "\n")
    
    # Sort states alphabetically
    sorted_states = sorted(state_crop_counts.keys())
    
    for state in sorted_states:
        print(f"\n{'='*60}")
        print(f"STATE: {state.upper()}")
        print(f"{'='*60}")
        
        if not state_crop_counts[state]:
            print("❌ Koi data nahi mila is state ke liye\n")
            continue
        
        # Sort by count (descending)
        sorted_crops = sorted(
            state_crop_counts[state].items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        print(f"\nAll Crops and their Query Counts:\n")
        print(f"{'Rank':<6} {'Crop Name':<40} {'Total Queries':<15}")
        print("-" * 60)
        
        for rank, (crop, count) in enumerate(sorted_crops, 1):
            print(f"{rank:<6} {crop:<40} {count:<15,}")
        
        # Top crop highlight karein
        top_crop, top_count = sorted_crops[0]
        print(f"\n🏆 SABSE ZYADA QUERIED CROP: {top_crop}")
        print(f"   Total Queries: {top_count:,}")
        
        # Total queries
        total_queries = sum(state_crop_counts[state].values())
        print(f"\n📊 Total Queries (All Crops): {total_queries:,}\n")

def save_results_to_csv(state_crop_counts, output_file="crop_analysis_results.csv"):
    """
    Results ko CSV mein save karta hai
    """
    results = []
    
    for state in state_crop_counts.keys():
        for crop, count in state_crop_counts[state].items():
            results.append({
                'State': state,
                'Crop': crop,
                'Query_Count': count
            })
    
    df_results = pd.DataFrame(results)
    df_results = df_results.sort_values(['State', 'Query_Count'], ascending=[True, False])
    df_results.to_csv(output_file, index=False)
    print(f"\n✅ Results saved to: {output_file}")

if __name__ == "__main__":
    print("🌾 CROP QUERY ANALYSIS SCRIPT (OTHER STATES) 🌾\n")
    
    # Analysis run karein
    results = analyze_crop_queries(FILE_PATHS, EXCLUDED_STATES)
    
    # Results display karein
    display_results(results)
    
    # Results CSV mein save karein
    save_results_to_csv(results, "Other_States_Crops.csv")
    
    print("\n" + "="*80)
    print("Analysis Complete! ✅")
    print("="*80)