import pandas as pd

# ==========================
# CONFIG
# ==========================
INPUT_FILE = "PUNJAB_COTTON_KAPAS.csv"
OUTPUT_FILE = "PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv"

# ==========================
# TOPIC & SUBTOPIC RULES
# ==========================
TOPIC_SUBTOPIC_KEYWORDS = {
    'Pest Control': {
        'Whitefly': ['whitefly', 'white fly'],
        'Aphid': ['aphid', 'jassid'],
        'Thrips': ['thrips'],
        'Borer': ['borer', 'bollworm'],
        'Mite': ['mite', 'spider'],
        'Scale': ['scale'],
        'Other Pests': ['pest', 'insect']
    },
    
    'Disease Management': {
        'Rust': ['rust'],
        'Blight': ['blight'],
        'Wilt': ['wilt', 'fusarium'],
        'Leaf Spot': ['leaf spot', 'spot'],
        'Powdery Mildew': ['powdery', 'downy'],
        'Rot': ['rot', 'rotting'],
        'Other Diseases': ['disease', 'fungal', 'bacterial']
    },
    
    'Fertilizer & Nutrition': {
        'Nitrogen': ['nitrogen', 'urea'],
        'Phosphorus': ['phosphorus', 'dap'],
        'Potassium': ['potassium', 'potash'],
        'Micronutrients': ['micronutrient', 'zinc', 'iron', 'boron'],
        'NPK': ['npm', 'npk'],
        'Fertilizer Dose': ['dose', 'fertilizer']
    },
    
    'Weed Management': {
        'Herbicide': ['herbicide', 'spray', 'chemical'],
        'Weed Control': ['weed', 'kharpatwar']
    },
    
    'Irrigation': {
        'Irrigation Schedule': ['irrigation', 'watering'],
        'Water Management': ['water', 'moisture'],
        'Drainage': ['drain', 'rainfall']
    },
    
    'Seed & Sowing': {
        'Variety': ['variety', 'hybrid'],
        'Seed Quality': ['seed'],
        'Sowing Method': ['sowing', 'planting', 'germination']
    },
    
    'Harvesting & Post-Harvest': {
        'Harvesting': ['harvest', 'picking'],
        'Yield': ['yield'],
        'Storage': ['storage', 'drying'],
        'Quality': ['quality', 'grading']
    },
    
    'General Practice': {
        'Advisory': ['information', 'advisory'],
        'Techniques': ['method', 'technique', 'practice'],
        'Timing': ['timing', 'schedule']
    }
}

# ==========================
# FUNCTIONS
# ==========================
def classify_topic_subtopic(query_text):
    """
    Classify query into Topic and Subtopic based on keywords.
    Returns (Topic, Subtopic)
    If no match found, returns ('OTHER', 'OTHER')
    """
    query_lower = str(query_text).lower()
    
    # Check each topic and its subtopics
    for topic, subtopics_dict in TOPIC_SUBTOPIC_KEYWORDS.items():
        for subtopic, keywords in subtopics_dict.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return (topic, subtopic)
    
    # If no keywords matched, classify as OTHER
    return ('OTHER', 'OTHER')

# ==========================
# LOAD DATA
# ==========================
print("Reading input file...")
df = pd.read_csv(INPUT_FILE)
print(f"Loaded {len(df)} questions")

# ==========================
# CLASSIFY INTO TOPICS & SUBTOPICS
# ==========================
print("Classifying into Topics and Subtopics...")
df[['Topic', 'Subtopic']] = df['QueryText'].apply(
    lambda x: pd.Series(classify_topic_subtopic(x))
)

# Show distribution
print(f"\nTopic Distribution:")
print(df['Topic'].value_counts())

print(f"\nSubtopic Distribution (by Topic):")
for topic in df['Topic'].unique():
    print(f"\n{topic}:")
    subtopic_counts = df[df['Topic'] == topic]['Subtopic'].value_counts()
    for subtopic, count in subtopic_counts.items():
        print(f"  - {subtopic}: {count}")

# ==========================
# SORT BY TOPIC -> SUBTOPIC -> QUESTION
# ==========================
print("\nSorting by Topic > Subtopic > Question...")
df = df.sort_values(by=['Topic', 'Subtopic', 'QueryText'], ascending=[True, True, True])

# ==========================
# VALIDATION: Ensure no queries are missed
# ==========================
unclassified = df[(df['Topic'] == 'OTHER') & (df['Subtopic'] == 'OTHER')]
print(f"\nUnclassified Queries (OTHER/OTHER): {len(unclassified)}")

if len(unclassified) > 0:
    print("Sample unclassified queries:")
    for i, query in enumerate(unclassified['QueryText'].head(3).values):
        print(f"  {i+1}. {query}")

# ==========================
# SELECT COLUMNS FOR OUTPUT
# ==========================
output_cols = ['Topic', 'Subtopic', 'QueryText'] + [col for col in df.columns if col not in ['Topic', 'Subtopic', 'QueryText']]
df_output = df[output_cols]

# ==========================
# SAVE OUTPUT
# ==========================
df_output.to_csv(OUTPUT_FILE, index=False)

print("\n" + "=" * 80)
print("SUCCESS! Topic & Subtopic Classification Complete")
print(f"Total Rows: {len(df_output)}")
print(f"Topics: {df['Topic'].nunique()}")
print(f"Subtopics: {df['Subtopic'].nunique()}")
print(f"Saved to: {OUTPUT_FILE}")
print("=" * 80)

# ==========================
# VERIFICATION
# ==========================
print("\nVerification:")
print(f"Input rows: {len(df)}")
print(f"Output rows: {len(df_output)}")
print(f"Match: {'YES' if len(df) == len(df_output) else 'NO'}")

print("\nSample Output (First 10 rows):")
print(df_output[['Topic', 'Subtopic', 'QueryText']].head(10).to_string())
