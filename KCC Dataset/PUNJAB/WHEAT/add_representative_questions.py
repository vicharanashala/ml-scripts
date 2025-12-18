import pandas as pd

# ==========================
# CONFIG
# ==========================
INPUT_FILE = "D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv"
OUTPUT_FILE = "PUNJAB_COTTON_KAPAS_GROUPED_WITH_SAMPLE_QUESTION.csv"

QUESTION_COL = "QueryText"
SUBTOPIC_COL = "keyword_group"
KCC_ANSWER_COL = "KCC_Answer"   # remove if exists

# ==========================
# TOPIC INFERENCE RULES
# ==========================
TOPIC_RULES = {
    "Disease": [
        "virus", "blight", "rot", "wilt", "spot", "curl",
        "mosaic", "disease", "fungal", "bacterial"
    ],
    "Pest": [
        "bollworm", "whitefly", "aphid", "thrips",
        "jassid", "mite", "pest", "insect"
    ],
    "Fertilizer": [
        "nitrogen", "urea", "dap", "npk", "fertilizer",
        "nutrient", "zinc", "potash"
    ],
    "Irrigation": [
        "irrigation", "watering", "water", "drip"
    ],
    "Seed": [
        "seed", "variety", "hybrid", "sowing"
    ]
}

def infer_topic(text):
    text = str(text).lower()
    for topic, keywords in TOPIC_RULES.items():
        for kw in keywords:
            if kw in text:
                return topic
    return "Other"

# ==========================
# LOAD DATA
# ==========================
print("Reading input file...")
df = pd.read_csv(INPUT_FILE)
print(f"Loaded {len(df)} questions")

# Remove KCC answer column
if KCC_ANSWER_COL in df.columns:
    df = df.drop(columns=[KCC_ANSWER_COL])

# ==========================
# CREATE TOPIC COLUMN
# ==========================
print("Creating topics...")
if SUBTOPIC_COL not in df.columns:
    df[SUBTOPIC_COL] = ""
    
df["Topic"] = (
    df[SUBTOPIC_COL].fillna("") + " " + df[QUESTION_COL].fillna("")
).apply(infer_topic)

# ==========================
# SORT DATA
# ==========================
df = df.sort_values(
    by=["Topic", SUBTOPIC_COL, QUESTION_COL],
    ascending=[True, True, True]
).reset_index(drop=True)

# ==========================
# ADD REPRESENTATIVE QUESTION COLUMN
# ==========================
df["Representative_Question"] = ""

current_group = None

for idx, row in df.iterrows():
    group_key = (row["Topic"], row[SUBTOPIC_COL])

    # New group starts
    if group_key != current_group:
        df.at[idx, "Representative_Question"] = row[QUESTION_COL]
        current_group = group_key
    # else: leave blank

# ==========================
# SAVE OUTPUT
# ==========================
df.to_csv(OUTPUT_FILE, index=False)

print("=" * 80)
print("SUCCESS! GROUPED OUTPUT WITH ONE QUESTION PER GROUP CREATED")
print(f"Total rows: {len(df)}")
print(f"Saved to: {OUTPUT_FILE}")
print("=" * 80)
