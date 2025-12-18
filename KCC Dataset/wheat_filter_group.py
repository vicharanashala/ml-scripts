import pandas as pd
import re
from collections import defaultdict

# Configuration
State = "PUNJAB"
part = 2

print("=" * 80)
print(f"WHEAT QUESTIONS FILTER AND GROUPING - PART {part}")
print("=" * 80)

# Load the dataset
df = pd.read_csv(f'kcc_dataset_part_{part}.csv', on_bad_lines='skip')
print(f"\nOriginal rows: {len(df):,}")

# ============================================================================
# STEP 1: FILTER WHEAT QUESTIONS
# ============================================================================
print("\n" + "=" * 80)
print("STEP 1: FILTERING WHEAT QUESTIONS")
print("=" * 80)

# Filter by State
df = df[df['StateName'] == State]
print(f"After state filter ({State}): {len(df):,}")

# Filter by Crop - WHEAT only
df = df[df['Crop'].str.upper() == 'WHEAT']
print(f"After crop filter (WHEAT): {len(df):,}")

# Remove weather-related QueryTypes
weather_types = ['Weather', 'Government Schemes', 'Market Information', 'Credit', 
                 'Training and Exposure Visits', 'Power Roads etc', '0']
df = df[~df['QueryType'].isin(weather_types)]
print(f"After removing non-agricultural queries: {len(df):,}")

# Remove rows with weather keywords in QueryText
weather_keywords = ['weather', 'rain', 'rainfall', 'temperature', 'climate', 'frost', 
                    'monsoon', 'sunny', 'cloudy', 'storm']
weather_mask = df['QueryText'].str.lower().str.contains('|'.join(weather_keywords), na=False)
df = df[~weather_mask]
print(f"After removing weather-related questions: {len(df):,}")

# ============================================================================
# STEP 2: GROUP SIMILAR QUESTIONS
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: GROUPING SIMILAR QUESTIONS")
print("=" * 80)

def normalize_text(text):
    """Normalize text for similarity comparison."""
    if pd.isna(text):
        return ""
    
    text = str(text).upper()
    
    # Remove noise
    text = re.sub(r'^INFORMATION REGARDING FOR THE\s+', '', text)
    text = re.sub(r'^INFORMATION REGARDING FOR\s+', '', text)
    text = re.sub(r'^INFORMATION REGARDING\s+', '', text)
    text = re.sub(r'^INFORMATION\s+', '', text)
    text = re.sub(r'^[:\.\-\s]+', '', text)
    
    # Normalize common patterns
    text = re.sub(r'\bCONTROL OF\b', 'CONTROL', text)
    text = re.sub(r'\bFOR THE CONTROL OF\b', 'CONTROL', text)
    text = re.sub(r'\bIN WHEAT CROP\b', 'IN WHEAT', text)
    text = re.sub(r'\bWHEAT CROP\b', 'WHEAT', text)
    text = re.sub(r'\bCROP\b', '', text)
    
    # Remove punctuation
    text = re.sub(r'[.,?!:;()\[\]{}"\']+', ' ', text)
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

# Create normalized column for grouping
df['normalized_query'] = df['QueryText'].apply(normalize_text)

# Group by normalized query
print("Grouping questions by similarity...")
grouped = df.groupby('normalized_query', sort=False)

# Create keyword groups
def extract_keywords(text):
    """Extract main keywords from query."""
    text_lower = str(text).lower()
    
    keywords = []
    
    # Diseases/Pests
    if 'aphid' in text_lower or 'chupa' in text_lower:
        keywords.append('APHIDS')
    if 'yellow rust' in text_lower or 'peela rog' in text_lower:
        keywords.append('YELLOW_RUST')
    if 'brown rust' in text_lower or 'leaf rust' in text_lower:
        keywords.append('BROWN_RUST')
    if 'termite' in text_lower or 'deemak' in text_lower:
        keywords.append('TERMITES')
    if 'weed' in text_lower or 'kharpat' in text_lower:
        keywords.append('WEEDS')
    
    # Agronomic practices
    if 'variety' in text_lower or 'varieties' in text_lower:
        keywords.append('VARIETIES')
    if 'fertilizer' in text_lower or 'urea' in text_lower or 'dap' in text_lower:
        keywords.append('FERTILIZER')
    if 'seed' in text_lower:
        keywords.append('SEED')
    if 'irrigation' in text_lower or 'water' in text_lower:
        keywords.append('IRRIGATION')
    if 'sowing' in text_lower:
        keywords.append('SOWING')
    
    return ', '.join(keywords) if keywords else 'OTHER'

df['keyword_group'] = df['QueryText'].apply(extract_keywords)

# Sort by keyword_group, then by normalized_query (to keep similar questions together)
df_sorted = df.sort_values(['keyword_group', 'normalized_query', 'QueryText'])

# Select columns to keep (remove normalized_query as it's just for grouping)
columns_to_keep = ['BlockName', 'Category', 'Year', 'Month', 'Day', 'Crop', 
                   'DistrictName', 'QueryType', 'Season', 'Sector', 'StateName', 
                   'QueryText', 'KccAns', 'keyword_group']

df_final = df_sorted[columns_to_keep].reset_index(drop=True)

# ============================================================================
# STEP 3: SAVE RESULTS
# ============================================================================
print("\n" + "=" * 80)
print("SAVING RESULTS")
print("=" * 80)

output_file = f'{State}_wheat_questions_grouped_part_{part}.csv'
df_final.to_csv(output_file, index=False)

print(f"\n✅ Saved: {output_file}")
print(f"   Total wheat questions: {len(df_final):,}")
print(f"   Questions grouped row-wise by similarity")
print(f"   All metadata preserved")

# Display statistics
print("\n📊 Keyword Group Distribution:")
print(df_final['keyword_group'].value_counts().to_string())

print("\n📊 Query Type Distribution:")
print(df_final['QueryType'].value_counts().head(10).to_string())

# Show sample
print("\n📋 Sample Questions (first 10):")
for i, row in df_final.head(10).iterrows():
    print(f"\n{i+1}. Group: {row['keyword_group']}")
    print(f"   Query: {row['QueryText'][:100]}")

print("\n" + "=" * 80)
print("✅ PROCESSING COMPLETE!")
print("=" * 80)
print(f"🌾 {len(df_final):,} wheat questions")
print(f"📁 Grouped by similarity (row-wise)")
print(f"🏷️  {len(df_final['keyword_group'].unique())} keyword groups")
