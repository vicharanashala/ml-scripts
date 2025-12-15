import pandas as pd
import os

input_file = r"D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\statistics\Other_States_Crops.csv"
output_dir = r"D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\statistics"

print("="*80)
print("REMOVING NUMERIC CODES FROM CROP ANALYSIS & CREATING STATE-WISE FILES")
print("="*80)

# Read file
df = pd.read_csv(input_file)
print(f"\nOriginal rows: {len(df)}")

# Remove numeric crop codes
# Check if Crop column is purely numeric (code)
def is_numeric_code(val):
    if pd.isna(val):
        return False
    try:
        float(str(val))
        return True
    except:
        return False

df_clean = df[~df['Crop'].apply(is_numeric_code)].copy()
print(f"Rows after removing codes: {len(df_clean)}")
print(f"Codes removed: {len(df) - len(df_clean)}")

# Get unique states
states = df_clean['State'].unique()
print(f"\nCreating {len(states)} state-wise files...\n")

# Create state-wise files
for state in sorted(states):
    state_data = df_clean[df_clean['State'] == state].copy()
    
    # Create filename (replace spaces with underscores)
    state_name = state.replace(" ", "_").upper()
    filename = f"{state_name}_crops.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Save file
    state_data.to_csv(filepath, index=False)
    
    print(f"✅ {state}:")
    print(f"   File: {filename}")
    print(f"   Rows: {len(state_data)}")
    print(f"   Unique crops: {state_data['Crop'].nunique()}")
    print(f"   Top 3 crops: {state_data.nlargest(3, 'Query_Count')['Crop'].tolist()}")
    print()

print("="*80)
print("✅ DONE! All state-wise CSV files created.")
print("="*80)
