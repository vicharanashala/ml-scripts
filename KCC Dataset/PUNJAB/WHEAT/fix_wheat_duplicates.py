import pandas as pd
import re
import os

# File path
input_file = r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_wheat_questions_CLEANED.csv"

print("="*80)
print("FIXING DUPLICATE 'IN WHEAT' IN QUESTIONS")
print("="*80)

# Read file
df = pd.read_csv(input_file)
print(f"\nLoaded {len(df)} questions")

def fix_duplicate_wheat(text):
    """Remove duplicate 'in wheat' occurrences"""
    if not isinstance(text, str):
        return text
    
    # Remove "in wheat" duplicates - keep only one at the end
    text = re.sub(r'\s+in\s+wheat(\s+in\s+wheat)+\?', ' in wheat?', text, flags=re.IGNORECASE)
    text = re.sub(r'\s+in\s+wheat(\s+in\s+wheat)+$', ' in wheat', text, flags=re.IGNORECASE)
    
    # Also fix cases where it's "in wheat in wheat?" without proper spacing
    text = re.sub(r'in\s+wheat\s+in\s+wheat', 'in wheat', text, flags=re.IGNORECASE)
    
    return text

print("\nFixing duplicate 'in wheat'...")
df['QueryText'] = df['QueryText'].apply(fix_duplicate_wheat)

# Display sample
print("\n" + "="*80)
print("SAMPLE OF FIXED QUESTIONS:")
print("="*80)
for i, row in df.head(15).iterrows():
    print(f"{i+1}. {row['QueryText']}")

# Save back to same file
df.to_csv(input_file, index=False)
print(f"\n{'='*80}")
print(f"✅ File updated: PUNJAB_wheat_questions_CLEANED.csv")
print(f"   Total questions: {len(df)}")
print(f"{'='*80}")
