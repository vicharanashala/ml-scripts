import pandas as pd
import os

# Get full paths
BASE_PATH = r"D:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1"
INPUT_FILE = os.path.join(BASE_PATH, "PUNJAB_wheat_questions_DEDUPLICATED.csv")
OUTPUT_FILE = os.path.join(BASE_PATH, "PUNJAB_wheat_questions_NEW(1).csv")

print("📥 Reading input file...")
df = pd.read_csv(INPUT_FILE)

print(f"Initial rows: {len(df)}")

# Columns used to define semantic group
GROUP_COLS = ["Topic", "keyword_group", "Crop"]

def merge_unique(series):
    """Merge unique non-null values into comma-separated string"""
    return ", ".join(
        sorted(set(series.dropna().astype(str)))
    )

print("🔄 Deduplicating based on semantic groups...")

dedup_rows = []

for _, group in df.groupby(GROUP_COLS):
    # Pick canonical question (highest frequency)
    canonical_row = group.sort_values(
        "duplicate_count", ascending=False
    ).iloc[0].copy()

    # Aggregate duplicate count
    canonical_row["duplicate_count"] = group["duplicate_count"].sum()

    # Merge multi-valued fields
    canonical_row["BlockName"] = merge_unique(group["BlockName"])
    canonical_row["QueryType"] = merge_unique(group["QueryType"])
    canonical_row["Season"] = merge_unique(group["Season"])

    dedup_rows.append(canonical_row)

df_dedup = pd.DataFrame(dedup_rows)

# Optional: sort for readability
df_dedup = df_dedup.sort_values(
    "duplicate_count", ascending=False
)

df_dedup.to_csv(OUTPUT_FILE, index=False)

print("=" * 80)
print("✅ DEDUPLICATION COMPLETE")
print(f"Final rows: {len(df_dedup)}")
print(f"Saved to: {OUTPUT_FILE}")
print("=" * 80)
