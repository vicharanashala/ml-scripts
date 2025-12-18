import pandas as pd
import os

# File path
input_file = r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_wheat_questions_CLEANED.csv"
output_dir = os.path.dirname(input_file)

print("="*80)
print("REMOVING EXACT DUPLICATE QUESTIONS")
print("="*80)

# Read file
df = pd.read_csv(input_file)
print(f"\nLoaded {len(df)} questions\n")

# Store original count and duplicate info
original_count = len(df)
print(f"Original count: {original_count}")

# Find duplicates based on QueryText column
duplicates = df[df.duplicated(subset=['QueryText'], keep=False)]
print(f"Questions with exact duplicates: {len(duplicates)}")

# Get unique duplicated questions
unique_duplicated = df[df.duplicated(subset=['QueryText'], keep=False)]['QueryText'].unique()
print(f"Unique duplicated question text entries: {len(unique_duplicated)}\n")

# Show which questions have duplicates
print("Duplicate Analysis:")
print("-" * 80)
for q in unique_duplicated[:10]:  # Show first 10
    count = len(df[df['QueryText'] == q])
    print(f"  • {q[:70]}")
    print(f"    → Appears {count} times\n")

# Remove duplicates, keeping first occurrence
df_unique = df.drop_duplicates(subset=['QueryText'], keep='first')

print(f"\nAfter removing duplicates: {len(df_unique)} unique questions")
print(f"Duplicates removed: {original_count - len(df_unique)}")
print(f"Reduction: {((original_count - len(df_unique)) / original_count * 100):.1f}%\n")

# Save deduplicated file
output_file = os.path.join(output_dir, "PUNJAB_wheat_questions_DEDUPLICATED.csv")
df_unique.to_csv(output_file, index=False)

print("="*80)
print(f"✅ Deduplicated file saved: PUNJAB_wheat_questions_DEDUPLICATED.csv")
print(f"   Original: {original_count} questions")
print(f"   Unique: {len(df_unique)} questions")
print(f"   Duplicates removed: {original_count - len(df_unique)}")
print("="*80)
