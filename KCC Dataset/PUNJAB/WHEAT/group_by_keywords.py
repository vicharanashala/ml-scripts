import pandas as pd
import re
from collections import defaultdict

# Configuration
State = "PUNJAB"
part = 2

# Load the dataset
print("=" * 80)
print(f"GROUPING WHEAT QUESTIONS BY KEYWORDS - PART {part}")
print("=" * 80)

df = pd.read_csv('PUNJAB_PURE_WHEAT_ONLY.csv')
print(f"\nTotal questions: {len(df)}")

# Define keyword groups for different topics
keyword_groups = {
    'APHIDS': ['aphid', 'chupa', 'mahua'],
    'YELLOW_RUST': ['yellow rust', 'peela rog', 'yellow disease'],
    'BROWN_RUST': ['brown rust', 'leaf rust', 'bhuri rog'],
    'BLACK_RUST': ['black rust', 'stem rust', 'kala rog'],
    'TERMITE': ['termite', 'deemak', 'dimak'],
    'PINK_STEM_BORER': ['pink', 'stem borer', 'sundi'],
    'ARMYWORM': ['armyworm', 'army worm', 'fauji keeda'],
    'WEEDS_GRASS': ['phalaris', 'grass weed', 'narrow leaf', 'ghaas'],
    'WEEDS_BROADLEAF': ['broadleaf', 'broad leaf', 'chaudi patti'],
    'WEEDS_GENERAL': ['weed', 'kharpat'],
    'POWDERY_MILDEW': ['powdery mildew', 'safedफफूंदी'],
    'KARNAL_BUNT': ['karnal bunt', 'tundu', 'covered smut'],
    'LOOSE_SMUT': ['loose smut', 'gola rog'],
    'FLAG_SMUT': ['flag smut'],
    'VARIETIES': ['variety', 'varieties', 'kism', 'hd-', 'pbw-', 'dbw-', 'wh-'],
    'SEED_RATE': ['seed rate', 'beej dar', 'sowing rate'],
    'SEED_TREATMENT': ['seed treatment', 'beej upchar'],
    'FERTILIZER_NPK': ['npk', 'urea', 'dap', 'fertilizer'],
    'NITROGEN': ['nitrogen', 'n dose', 'नाइट्रोजन'],
    'PHOSPHORUS': ['phosphorus', 'phosphate'],
    'POTASH': ['potash', 'potassium'],
    'MICRONUTRIENTS': ['zinc', 'iron', 'manganese', 'boron', 'sulphur'],
    'IRRIGATION': ['irrigation', 'water', 'sinchay'],
    'SOWING_TIME': ['sowing time', 'planting time', 'बुवाई'],
    'FIELD_PREPARATION': ['field preparation', 'खेत तैयारी', 'ploughing'],
    'HARVESTING': ['harvest', 'katai', 'कटाई'],
    'YIELD': ['yield', 'उपज', 'production'],
    'STORAGE': ['storage', 'भंडारण'],
}

def extract_keywords_from_text(text):
    """Extract all matching keyword groups from text."""
    text_lower = str(text).lower()
    matched_groups = []
    
    for group_name, keywords in keyword_groups.items():
        for keyword in keywords:
            if keyword.lower() in text_lower:
                matched_groups.append(group_name)
                break  # One match per group is enough
    
    return matched_groups if matched_groups else ['OTHER']

# Group questions by keywords
print("\n" + "=" * 80)
print("EXTRACTING KEYWORDS AND GROUPING QUESTIONS")
print("=" * 80)

df['keyword_groups'] = df['QueryText'].apply(extract_keywords_from_text)

# Create a dictionary to store groups
grouped_questions = defaultdict(list)

for idx, row in df.iterrows():
    groups = row['keyword_groups']
    for group in groups:
        grouped_questions[group].append({
            'index': idx,
            'QueryText': row['QueryText'],
            'QueryType': row['QueryType'],
            'KccAns': row.get('KccAns', '')
        })

# Sort groups by size
sorted_groups = sorted(grouped_questions.items(), key=lambda x: len(x[1]), reverse=True)

print(f"\n✅ Found {len(sorted_groups)} keyword groups")
print("\n📊 Group Distribution:")
print("-" * 80)

for group_name, questions in sorted_groups[:20]:
    print(f"{group_name:30s} : {len(questions):4d} questions")

# Display sample from each major group
print("\n" + "=" * 80)
print("SAMPLE QUESTIONS FROM EACH GROUP")
print("=" * 80)

for group_name, questions in sorted_groups[:10]:
    print(f"\n{'=' * 80}")
    print(f"📌 GROUP: {group_name} ({len(questions)} questions)")
    print(f"{'=' * 80}")
    
    # Show first 5 questions from this group
    for i, q in enumerate(questions[:5], 1):
        print(f"\n{i}. {q['QueryText'][:100]}...")

# Save all grouped data in ONE CSV file with row-wise grouping
print("\n" + "=" * 80)
print("SAVING ALL GROUPS IN ONE CSV FILE")
print("=" * 80)

# Sort dataframe by primary group for row-wise grouping
df['primary_group'] = df['keyword_groups'].apply(lambda x: x[0] if x else 'OTHER')
df['all_groups'] = df['keyword_groups'].apply(lambda x: ', '.join(x))

# Sort by primary group so all similar questions are together
df_sorted = df.sort_values(['primary_group', 'QueryText']).reset_index(drop=True)

# Save single CSV file with all metadata preserved
output_file = f'{State}_wheat_questions_grouped_part_{part}.csv'
df_sorted.to_csv(output_file, index=False)

print(f"\n✅ Saved single grouped file: {output_file}")
print(f"   Total questions: {len(df_sorted):,}")
print(f"   All metadata preserved")
print(f"   Questions sorted by group (row-wise)")

# Print group counts
print(f"\n📊 Questions per group in the file:")
print(df_sorted['primary_group'].value_counts().to_string())

print("\n" + "=" * 80)
print("✅ GROUPING COMPLETE!")
print("=" * 80)
print(f"\n📊 Total groups: {len(sorted_groups)}")
print(f"📁 Files created: {len([g for g in sorted_groups if len(g[1]) >= 5]) + 2}")
print(f"🎯 All questions organized by keywords!")
