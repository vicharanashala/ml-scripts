import pandas as pd
import re
import os
from difflib import SequenceMatcher

# File path
input_file = r"d:\Kanak Data\OS Folders\Documents\KCC Dataset filtering\kcc dataset part- 1\PUNJAB_wheat_questions_CLEANED.csv"
output_dir = os.path.dirname(input_file)

print("="*80)
print("SEMANTIC SIMILARITY ANALYSIS OF WHEAT QUESTIONS")
print("="*80)

# Read file
df = pd.read_csv(input_file)
print(f"\nLoaded {len(df)} questions\n")

# Extract key topics/entities from questions
def extract_keywords(text):
    """Extract main keywords from question"""
    if not isinstance(text, str):
        return set()
    
    text = text.lower()
    
    # Extract key terms (remove stop words)
    stop_words = {'how', 'can', 'i', 'what', 'is', 'are', 'the', 'to', 'of', 'in', 'wheat', 'a', 'an', 'and', 'or', 'be', 'do', 'does', 'should', 'will', 'would', 'for'}
    
    # Get words
    words = re.findall(r'\b\w+\b', text)
    keywords = set(w for w in words if w not in stop_words and len(w) > 2)
    
    return keywords

# Calculate semantic similarity based on shared keywords
def semantic_similarity(text1, text2):
    """Calculate semantic similarity based on common keywords"""
    keywords1 = extract_keywords(text1)
    keywords2 = extract_keywords(text2)
    
    if not keywords1 or not keywords2:
        return 0
    
    intersection = len(keywords1 & keywords2)
    union = len(keywords1 | keywords2)
    
    return intersection / union if union > 0 else 0

# Find semantic clusters (threshold = 0.4 for semantic similarity)
semantic_threshold = 0.4
print(f"Analyzing semantic similarity (threshold: {semantic_threshold:.0%})\n")

processed = set()
clusters = []
questions_list = df['QueryText'].tolist()

# Store similarity scores
similarity_scores = {}

for i in range(len(questions_list)):
    if i in processed:
        continue
    
    cluster = [i]
    processed.add(i)
    
    # Find similar questions
    for j in range(i + 1, len(questions_list)):
        if j in processed:
            continue
        
        similarity_score = semantic_similarity(questions_list[i], questions_list[j])
        similarity_scores[(i, j)] = similarity_score
        
        if similarity_score >= semantic_threshold:
            cluster.append(j)
            processed.add(j)
    
    clusters.append(cluster)

print(f"[OK] Found {len(clusters)} semantic groups")
print(f"   Groups with multiple questions: {sum(1 for c in clusters if len(c) > 1)}\n")

# Display clusters with multiple similar questions
print("="*80)
print("SEMANTIC SIMILARITY CLUSTERS (Questions with similar meaning)")
print("="*80)

cluster_num = 0
for cluster in clusters:
    if len(cluster) > 1:
        cluster_num += 1
        print(f"\n[CLUSTER {cluster_num}] - {len(cluster)} semantically similar questions:")
        print(f"   Topic: {df.iloc[cluster[0]]['Topic']}")
        print(f"\n   Main Question: {df.iloc[cluster[0]]['QueryText']}")
        
        for idx, c_idx in enumerate(cluster[1:], 1):
            similarity_score = similarity_scores.get((cluster[0], c_idx), 0)
            similarity_label = "[LOW]" if similarity_score < 0.6 else "[HIGH]"
            print(f"\n   Similar {idx}: {df.iloc[c_idx]['QueryText']}")
            print(f"   Similarity: {similarity_score:.1%} {similarity_label}")
        
        print("\n" + "-"*80)

# Create semantic groups summary
print("\n" + "="*80)
print("SEMANTIC GROUPING SUMMARY")
print("="*80)

summary_data = []
for cluster_idx, cluster in enumerate(clusters, 1):
    main_question = df.iloc[cluster[0]]['QueryText']
    topic = df.iloc[cluster[0]]['Topic']
    similar_count = len(cluster) - 1
    total_count = df.iloc[cluster, :]['duplicate_count'].sum()
    
    summary_data.append({
        'Group': cluster_idx,
        'Topic': topic,
        'Main Question': main_question,
        'Variant Count': similar_count,
        'Total Queries': total_count
    })

summary_df = pd.DataFrame(summary_data)
summary_df = summary_df.sort_values('Total Queries', ascending=False)

print("\nTop 15 Semantic Groups by Query Count:\n")
for idx, row in summary_df.head(15).iterrows():
    print(f"{row['Group']}. [{row['Topic']}] {row['Main Question']}")
    print(f"   Variants: {row['Variant Count']} | Total Queries: {row['Total Queries']:,}\n")

# Save summary
summary_file = os.path.join(output_dir, "PUNJAB_semantic_groups_summary.csv")
summary_df.to_csv(summary_file, index=False)

# Create detailed output with all questions including low similarity ones
print("\n" + "="*80)
print("CREATING DETAILED OUTPUT FILE WITH ALL QUESTIONS")
print("="*80)

detailed_data = []
for cluster_idx, cluster in enumerate(clusters, 1):
    main_question = df.iloc[cluster[0]]['QueryText']
    topic = df.iloc[cluster[0]]['Topic']
    
    # Add main question
    detailed_data.append({
        'Cluster_ID': cluster_idx,
        'Topic': topic,
        'Question': main_question,
        'Similarity': '100.0%',
        'Similarity_Level': 'MAIN',
        'Duplicate_Count': df.iloc[cluster[0]]['duplicate_count']
    })
    
    # Add variant questions
    for idx, c_idx in enumerate(cluster[1:], 1):
        similarity_score = similarity_scores.get((cluster[0], c_idx), 0)
        similarity_level = "LOW (<60%)" if similarity_score < 0.6 else "HIGH (≥60%)"
        
        detailed_data.append({
            'Cluster_ID': cluster_idx,
            'Topic': topic,
            'Question': df.iloc[c_idx]['QueryText'],
            'Similarity': f'{similarity_score:.1%}',
            'Similarity_Level': similarity_level,
            'Duplicate_Count': df.iloc[c_idx]['duplicate_count']
        })

detailed_df = pd.DataFrame(detailed_data)

# Save detailed output
detailed_file = os.path.join(output_dir, "PUNJAB_semantic_groups_detailed.csv")
detailed_df.to_csv(detailed_file, index=False)

print(f"\n[OK] Semantic grouping summary saved to: PUNJAB_semantic_groups_summary.csv")
print(f"[OK] Detailed output saved to: PUNJAB_semantic_groups_detailed.csv")
print(f"\n   Total unique semantic groups: {len(clusters)}")
print(f"   Groups with variants: {sum(1 for c in clusters if len(c) > 1)}")
print(f"   Total questions in detailed output: {len(detailed_df)}")
print(f"   Questions with LOW similarity (<60%): {len(detailed_df[detailed_df['Similarity_Level'] == 'LOW (<60%)'])}")
print(f"   Questions with HIGH similarity (≥60%): {len(detailed_df[detailed_df['Similarity_Level'] == 'HIGH (≥60%)'])}")
print(f"{'='*80}")
