import pandas as pd

print("=" * 80)
print("FILTERING QUESTIONS WITH 'WHEAT' IN QUERY TEXT")
print("=" * 80)

# Load the grouped dataset
df = pd.read_csv('PUNJAB_wheat_questions_grouped_part_2.csv')
print(f"\nTotal questions: {len(df):,}")

# Filter questions that contain 'wheat' in QueryText (case-insensitive)
df['has_wheat'] = df['QueryText'].str.lower().str.contains('wheat', na=False)

# Keep only questions with 'wheat'
df_with_wheat = df[df['has_wheat']].copy()
df_without_wheat = df[~df['has_wheat']].copy()

# Remove the helper column
df_with_wheat = df_with_wheat.drop('has_wheat', axis=1)

print(f"\n📊 Filtering Results:")
print(f"   Questions WITH 'wheat' in text: {len(df_with_wheat):,}")
print(f"   Questions WITHOUT 'wheat' in text: {len(df_without_wheat):,}")
print(f"   Removed: {len(df_without_wheat):,} ({100*len(df_without_wheat)/len(df):.1f}%)")

# Save filtered dataset
output_file = 'PUNJAB_wheat_questions_filtered.csv'
df_with_wheat.to_csv(output_file, index=False)

print(f"\n✅ Saved filtered dataset: {output_file}")
print(f"   Total questions: {len(df_with_wheat):,}")
print(f"   All contain 'wheat' in QueryText")

# Show keyword group distribution
print("\n📊 Keyword Group Distribution (after filtering):")
print(df_with_wheat['keyword_group'].value_counts().head(15).to_string())

# Show examples of removed questions
if len(df_without_wheat) > 0:
    print("\n" + "=" * 80)
    print(f"📋 EXAMPLES OF REMOVED QUESTIONS (no 'wheat' in text):")
    print("=" * 80)
    for idx, (i, row) in enumerate(df_without_wheat.head(10).iterrows(), 1):
        print(f"\n{idx}. Group: {row['keyword_group']}")
        print(f"   Query: {row['QueryText'][:100]}")

# Show examples of kept questions
print("\n" + "=" * 80)
print(f"📋 EXAMPLES OF KEPT QUESTIONS (with 'wheat'):")
print("=" * 80)
for idx, (i, row) in enumerate(df_with_wheat.head(10).iterrows(), 1):
    print(f"\n{idx}. Group: {row['keyword_group']}")
    print(f"   Query: {row['QueryText'][:100]}")

print("\n" + "=" * 80)
print("✅ FILTERING COMPLETE!")
print("=" * 80)
print(f"🌾 {len(df_with_wheat):,} questions with 'wheat' in text")
print(f"❌ {len(df_without_wheat):,} questions removed (no 'wheat')")
