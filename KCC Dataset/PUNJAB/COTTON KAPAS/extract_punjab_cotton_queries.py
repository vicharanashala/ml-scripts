import pandas as pd
import os

# ==============================
# INPUT / OUTPUT CONFIG
# ==============================

INPUT_FILES = [
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_1.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_2.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_3.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_4.csv",
    r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc_dataset_part_5.csv"
]

OUTPUT_FILE = "PUNJAB_COTTON_KAPAS_QUERIES.csv"

STATE_FILTER = "PUNJAB"

# Cotton keywords (English + Hindi + variations)
COTTON_KEYWORDS = [
    "cotton",
    "kapas",
    "kapas",
    "cotten",   # common typo
    "raw cotton"
]

CHUNK_SIZE = 100_000

# ==============================
# UTILITY FUNCTIONS
# ==============================

def find_column(columns, keywords):
    """
    Automatically find column based on keyword match
    """
    for col in columns:
        col_l = col.lower()
        if any(k in col_l for k in keywords):
            return col
    return None


# ==============================
# MAIN EXTRACTION LOGIC
# ==============================

print("🌾 Extracting Punjab Cotton / Kapas Queries\n")

all_results = []

for file in INPUT_FILES:
    if not os.path.exists(file):
        print(f"⚠️ File not found: {file}")
        continue

    print(f"📂 Processing: {os.path.basename(file)}")

    # Preview columns
    preview = pd.read_csv(file, nrows=5)
    columns = list(preview.columns)

    state_col = find_column(columns, ["state"])
    crop_col = find_column(columns, ["crop", "commodity", "product"])
    question_col = find_column(columns, ["query", "question", "text"])

    if not state_col or not crop_col:
        print("❌ Required columns not found, skipping file\n")
        continue

    for chunk in pd.read_csv(file, chunksize=CHUNK_SIZE, low_memory=False, on_bad_lines="skip"):
        # Normalize
        chunk[state_col] = chunk[state_col].astype(str).str.upper().str.strip()
        chunk[crop_col] = chunk[crop_col].astype(str).str.lower().str.strip()

        # State filter
        state_filtered = chunk[chunk[state_col] == STATE_FILTER]

        if state_filtered.empty:
            continue

        # Cotton / Kapas filter
        mask = state_filtered[crop_col].apply(
            lambda x: any(k in x for k in COTTON_KEYWORDS)
        )

        cotton_data = state_filtered[mask]

        if not cotton_data.empty:
            all_results.append(cotton_data)

print("\n🔄 Consolidating results...")

if not all_results:
    print("❌ No Punjab Cotton/Kapas queries found.")
    exit()

final_df = pd.concat(all_results, ignore_index=True)

# Optional: remove exact duplicates
final_df = final_df.drop_duplicates()

final_df.to_csv(OUTPUT_FILE, index=False)

print("=" * 80)
print("✅ EXTRACTION COMPLETE")
print(f"Total rows: {len(final_df)}")
print(f"Saved to: {OUTPUT_FILE}")
print("=" * 80)
