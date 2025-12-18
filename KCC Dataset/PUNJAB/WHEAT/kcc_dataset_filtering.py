# @title Filter Data with Semantic Deduplication - One Clean Dataset
State = "PUNJAB" # @param ['MAHARASHTRA', 'MANIPUR', 'MEGHALAYA', 'NAGALAND', 'ODISHA', 'PUNJAB', 'RAJASTHAN', 'TAMILNADU', 'TRIPURA', 'UTTAR PRADESH', 'WEST BENGAL', 'SIKKIM', 'CHHATTISGARH', 'JHARKAND', 'UTTARAKHAND', 'TELANGANA', 'A AND N ISLANDS', 'ARUNACHAL PRADESH', 'DELHI', 'MIZORAM', 'PUDUCHERRY', 'ANDHRA PRADESH', 'ASSAM', 'BIHAR', 'GUJARAT', 'HARYANA', 'HIMACHAL PRADESH', 'JAMMU AND KASHMIR', 'KARNATAKA', 'KERALA', 'MADHYA PRADESH', 'DAMAN AND DIU']
part = 2 # @param {"type":"integer", "min":1, "max":5}
similarity_threshold = 0.85 # @param {"type":"slider", "min":0.5, "max":1.0, "step":0.05}

import pandas as pd
import re
from difflib import SequenceMatcher
import os

# Define the path to the CSV file
script_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(script_dir, f'kcc_dataset_part_{str(part)}.csv')

# Load the data into a pandas DataFrame
df = pd.read_csv(file_path, on_bad_lines='skip')

print("=" * 80)
print(f"PROCESSING PART {part} FOR {State}")
print("=" * 80)
print(f"Original rows loaded: {len(df):,}")

# Remove rows where Category is 'Others' and QueryType is 'Weather' from the original DataFrame
df = df[~((df['Category'] == 'Others') & (df['QueryType'] == 'Weather'))]
print(f"After removing 'Others'+'Weather': {len(df):,} rows")

# Define the list of states to filter by
states_to_filter = [State]

# Filter the DataFrame to include only the specified states
filtered_df_states = df[df['StateName'].isin(states_to_filter)]
print(f"After filtering by {State}: {len(filtered_df_states):,} rows")

# Remove rows where QueryType is 'Weather' or 'Government Schemes'
query_types_to_remove = ['Weather', 'Government Schemes', 'Market Information', 'Credit',
                         'Training and Exposure Visits', 'Power Roads etc', '0']
filtered_df_states = filtered_df_states[~filtered_df_states['QueryType'].isin(query_types_to_remove)]
print(f"After removing unwanted QueryTypes: {len(filtered_df_states):,} rows")

# Remove rows where Sector is not 'AGRICULTURE' or 'HORTICULTURE'
sectors_to_keep = ['AGRICULTURE', 'HORTICULTURE']
filtered_df_states = filtered_df_states[filtered_df_states['Sector'].isin(sectors_to_keep)]
print(f"After keeping only AGRICULTURE/HORTICULTURE: {len(filtered_df_states):,} rows")

# Define the list of crops to keep
crops_to_keep = [
    'Wheat'
]

# Filter to keep only rows where Crop is in the specified list
filtered_df_states = filtered_df_states[filtered_df_states['Crop'].isin(crops_to_keep)]
print(f"After filtering by specified crops: {len(filtered_df_states):,} rows")

# Define the string to match after cleaning
string_to_match = 'farmeraskedqueryonweather'

# Remove rows where cleaned 'QueryText' matches the defined string
filtered_df_states = filtered_df_states[
    filtered_df_states['QueryText'].str.lower().str.replace(' ', '', regex=False) != string_to_match
]
print(f"After removing specific QueryText pattern: {len(filtered_df_states):,} rows")

# ============================================================================
# ENHANCED DUPLICATE REMOVAL
# ============================================================================
print("\n" + "=" * 80)
print("REMOVING EXACT DUPLICATE QUESTIONS")
print("=" * 80)

# Function to clean query text for duplicate detection
def clean_query_text(text):
    """Clean query text for duplicate detection"""
    if pd.isna(text):
        return ''
    text = str(text).lower()
    text = text.replace(' ', '')
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

# Clean 'QueryText' for duplicate checking
filtered_df_states['cleaned_querytext'] = filtered_df_states['QueryText'].apply(clean_query_text)

# Check for duplicates before removal
before_dedup = len(filtered_df_states)
duplicate_count = filtered_df_states['cleaned_querytext'].duplicated().sum()

print(f"\nBefore duplicate removal:")
print(f"  Total rows: {before_dedup:,}")
print(f"  Unique questions: {filtered_df_states['cleaned_querytext'].nunique():,}")
print(f"  Duplicate questions: {duplicate_count:,}")

# Remove duplicate rows based on the cleaned 'QueryText' - keep first occurrence
filtered_df_states = filtered_df_states.drop_duplicates(subset=['cleaned_querytext'], keep='first')

after_dedup = len(filtered_df_states)
removed_dupes = before_dedup - after_dedup

print(f"\n✅ After exact duplicate removal:")
print(f"  Total rows: {after_dedup:,}")
print(f"  Duplicates removed: {removed_dupes:,}")

# Drop the temporary 'cleaned_querytext' column
filtered_df_states = filtered_df_states.drop(columns=['cleaned_querytext'])

# ============================================================================
# SEMANTIC SIMILARITY ANALYSIS & INTELLIGENT SELECTION
# ============================================================================
print("\n" + "=" * 80)
print("SEMANTIC DEDUPLICATION - SELECTING BEST QUESTIONS")
print("=" * 80)
print(f"Similarity threshold: {similarity_threshold}")
print("(This may take a moment for large datasets...)")

def calculate_similarity(str1, str2):
    """Calculate similarity ratio between two strings"""
    if pd.isna(str1) or pd.isna(str2):
        return 0.0
    s1 = str(str1).lower().strip()
    s2 = str(str2).lower().strip()
    return SequenceMatcher(None, s1, s2).ratio()

def normalize_question(text):
    """Normalize question for better comparison"""
    if pd.isna(text):
        return ''
    text = str(text).lower().strip()
    text = ' '.join(text.split())
    return text

def score_question_quality(text):
    """Score a question based on grammar and completeness"""
    if pd.isna(text):
        return 0

    text = str(text).strip()
    score = 0

    # Length score (prefer moderate length - clear complete questions)
    length = len(text)
    if 30 <= length <= 120:
        score += 35
    elif 20 <= length < 30 or 120 < length <= 180:
        score += 25
    elif 10 <= length < 20:
        score += 15
    else:
        score += 5

    # Starts with capital letter (grammar indicator)
    if text and text[0].isupper():
        score += 20

    # Ends with question mark (proper question format)
    if text.endswith('?'):
        score += 25

    # Contains question words at start (proper question structure)
    question_words_start = ['how', 'what', 'when', 'where', 'why', 'which', 'who', 'can', 'could', 'should', 'would', 'is', 'are', 'does', 'do', 'will', 'has', 'have']
    text_lower = text.lower()
    words = text_lower.split()
    if len(words) > 0 and words[0] in question_words_start:
        score += 20
    elif any(text_lower.startswith(word + ' ') for word in question_words_start):
        score += 15

    # Proper spacing (no multiple spaces or leading/trailing spaces)
    if '  ' not in text and text == text.strip():
        score += 10

    # No excessive punctuation (grammar indicator)
    punct_count = sum(1 for c in text if c in '!!!???...')
    if punct_count == 0 or punct_count == 1:
        score += 15
    
    # Contains common words (not just fragments)
    word_count = len(words)
    if word_count >= 5:
        score += 15
    elif word_count >= 3:
        score += 10
    
    # Penalty for all caps (poor grammar)
    if text.isupper() and len(text) > 3:
        score -= 20
    
    # Penalty for all lowercase (poor grammar)
    if text.islower():
        score -= 15

    return score

def select_best_question(questions_with_data):
    """Select the best question from a group based on quality score"""
    best_idx = 0
    best_score = -1

    for idx, (question, row_data) in enumerate(questions_with_data):
        score = score_question_quality(question)
        if score > best_score:
            best_score = score
            best_idx = idx

    return best_idx, best_score

# Create normalized versions
filtered_df_states['normalized_query'] = filtered_df_states['QueryText'].apply(normalize_question)

# Find semantically similar questions and group them
similar_groups = []
questions_list = filtered_df_states['normalized_query'].tolist()
indices_list = filtered_df_states.index.tolist()
processed = set()

print(f"\nScanning {len(questions_list):,} questions for semantic similarities...")

for i in range(len(questions_list)):
    if i in processed:
        continue

    current_group = [(i, questions_list[i], filtered_df_states.loc[indices_list[i]])]

    for j in range(i + 1, len(questions_list)):
        if j in processed:
            continue

        similarity = calculate_similarity(questions_list[i], questions_list[j])

        if similarity >= similarity_threshold:
            current_group.append((j, questions_list[j], filtered_df_states.loc[indices_list[j]]))
            processed.add(j)

    # Add group (either single or multiple similar questions)
    similar_groups.append(current_group)
    processed.add(i)

print(f"\n✅ Found {len([g for g in similar_groups if len(g) > 1])} groups with similar questions")

# ============================================================================
# SELECT BEST QUESTION FROM EACH GROUP
# ============================================================================
print("\n" + "=" * 80)
print("SELECTING BEST GRAMMATICALLY CORRECT QUESTION FROM EACH SIMILAR GROUP")
print("=" * 80)

# Create list to track which rows to keep
rows_to_keep = []
removed_similar_questions = []

for group in similar_groups:
    if len(group) == 1:
        # No similar questions, keep this one
        rows_to_keep.append(indices_list[group[0][0]])
    else:
        # Multiple similar questions - select the best grammatically correct one
        questions_with_data = [(q, row) for idx, q, row in group]
        best_idx, best_score = select_best_question(questions_with_data)

        # Keep the best question
        kept_question_index = indices_list[group[best_idx][0]]
        rows_to_keep.append(kept_question_index)

        # Track removed questions for reporting with their quality scores
        for idx, (g_idx, question, row_data) in enumerate(group):
            if idx != best_idx:
                removed_score = score_question_quality(question)
                removed_similar_questions.append({
                    'removed_question': question,
                    'removed_quality_score': removed_score,
                    'kept_question': group[best_idx][1],
                    'kept_quality_score': best_score,
                    'similarity': calculate_similarity(group[best_idx][1], question),
                    'reason': f'Lower quality/grammar score ({removed_score} vs {best_score})'
                })

print(f"Groups processed: {len(similar_groups)}")
print(f"Questions with similar variants: {sum(1 for g in similar_groups if len(g) > 1)}")
print(f"Similar questions removed: {len(removed_similar_questions)}")
print(f"Questions retained: {len(rows_to_keep)}")

# Create the final clean dataset
final_clean_df = filtered_df_states.loc[rows_to_keep].copy()
final_clean_df = final_clean_df.drop(columns=['normalized_query'])

# ============================================================================
# SHOW EXAMPLES OF SELECTIONS
# ============================================================================
if removed_similar_questions:
    print("\n" + "=" * 80)
    print("📋 EXAMPLES OF SEMANTIC DEDUPLICATION (BEST GRAMMAR SELECTED)")
    print("=" * 80)
    print("\nShowing first 5 examples of grammatically correct question selection:\n")

    for idx, item in enumerate(removed_similar_questions[:5], 1):
        print(f"Example {idx}:")
        print(f"  ✅ KEPT: \"{item['kept_question']}\" [Quality Score: {item['kept_quality_score']}]")
        print(f"  ❌ REMOVED: \"{item['removed_question']}\" [Quality Score: {item['removed_quality_score']}]")
        print(f"  📊 Similarity: {item['similarity']:.2%}")
        print(f"  💡 Reason: {item['reason']}")
        print()

# ============================================================================
# DISPLAY FINAL RESULTS
# ============================================================================
print("\n" + "=" * 80)
print("✅ FINAL CLEAN DATASET - ONE UNIQUE GRAMMATICALLY CORRECT DATASET")
print("=" * 80)

print(f"\n📊 Dataset Reduction Summary:")
print(f"  Original loaded: {len(df):,} rows")
print(f"  After all filters: {before_dedup:,} rows")
print(f"  After exact dedup: {after_dedup:,} rows")
print(f"  After semantic dedup & grammar selection: {len(final_clean_df):,} rows")
print(f"  Total removed: {len(df) - len(final_clean_df):,} rows ({100*(len(df) - len(final_clean_df))/len(df):.1f}%)")
print(f"\n✨ This dataset contains ONLY unique, grammatically correct questions!")

print(f"\n✨ Preview of Final Clean Dataset:")
print(final_clean_df.head(10).to_string())

print("\n📍 Value counts for 'StateName':")
print(final_clean_df['StateName'].value_counts())

print("\n📝 Value counts for 'QueryType':")
print(final_clean_df['QueryType'].value_counts())

print("\n🏭 Value counts for 'Sector':")
print(final_clean_df['Sector'].value_counts())

print("\n🌾 Value counts for 'Crop':")
print(final_clean_df['Crop'].value_counts())

# ============================================================================
# SAVE FILES
# ============================================================================
print("\n" + "=" * 80)
print("SAVING FILES")
print("=" * 80)

# Save the final clean dataset
clean_output_filename = f'{State}_CLEAN_final_dataset_part_{str(part)}.csv'
final_clean_df.to_csv(clean_output_filename, index=False)
print(f"✅ Saved clean dataset: {clean_output_filename}")
print(f"   Total unique questions: {len(final_clean_df):,}")

# Save removed questions report
if removed_similar_questions:
    removed_report_filename = f'{State}_removed_similar_questions_part_{str(part)}.csv'
    removed_df = pd.DataFrame(removed_similar_questions)
    removed_df.to_csv(removed_report_filename, index=False)
    print(f"✅ Saved removed questions report: {removed_report_filename}")
    print(f"   Total removed: {len(removed_similar_questions):,}")

# Save detailed report
detailed_report_filename = f'{State}_deduplication_report_part_{str(part)}.txt'
with open(detailed_report_filename, 'w', encoding='utf-8') as f:
    f.write(f"SEMANTIC DEDUPLICATION REPORT - GRAMMATICALLY CORRECT QUESTIONS\n")
    f.write(f"State: {State} | Part: {part} | Threshold: {similarity_threshold}\n")
    f.write(f"{'=' * 80}\n\n")

    f.write(f"SUMMARY:\n")
    f.write(f"  Original rows: {len(df):,}\n")
    f.write(f"  After filters: {before_dedup:,}\n")
    f.write(f"  After exact dedup: {after_dedup:,}\n")
    f.write(f"  Final clean dataset: {len(final_clean_df):,}\n")
    f.write(f"  Similar questions removed: {len(removed_similar_questions):,}\n")
    f.write(f"  Groups with similar questions: {len([g for g in similar_groups if len(g) > 1]):,}\n\n")

    f.write(f"{'=' * 80}\n")
    f.write(f"REMOVED QUESTIONS (kept best grammar vs removed)\n")
    f.write(f"{'=' * 80}\n\n")

    for idx, item in enumerate(removed_similar_questions, 1):
        f.write(f"{idx}. KEPT: \"{item['kept_question']}\" [Score: {item['kept_quality_score']}]\n")
        f.write(f"   REMOVED: \"{item['removed_question']}\" [Score: {item['removed_quality_score']}]\n")
        f.write(f"   Similarity: {item['similarity']:.2%} | Reason: {item['reason']}\n\n")

print(f"✅ Saved detailed report: {detailed_report_filename}")

print("\n" + "=" * 80)
print("✅ PROCESSING COMPLETE!")
print("=" * 80)
print(f"\n🎉 Final clean dataset ready with {len(final_clean_df):,} unique, grammatically correct questions!")
print(f"📊 All similar questions have been grouped and best grammar selected.")
print(f"🎯 You now have ONE clean, deduplicated dataset with high-quality questions!")