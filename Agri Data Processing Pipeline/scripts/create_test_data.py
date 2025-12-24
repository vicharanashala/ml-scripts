#!/usr/bin/env python3
"""
Quick test script to verify the deduplication pipeline setup.
Tests with a small sample dataset.
"""

import pandas as pd
from pathlib import Path

# Create sample data
sample_questions = [
    "What is the best fertilizer for wheat?",
    "What is the best fertilizer for wheat?",  # Exact duplicate
    "What is best fertilizer for wheat",  # Fuzzy duplicate (missing question mark)
    "Which fertilizer is best for wheat crop?",  # Semantic duplicate
    "How to control pests in rice?",
    "How to control pest in rice",  # Fuzzy duplicate
    "What are pest control methods for rice?",  # Semantic duplicate
    "When to plant tomatoes?",
    "What is the planting time for tomatoes?",  # Semantic duplicate
    "How much water does corn need?",
]

# Create sample DataFrame
df_sample = pd.DataFrame({
    'QueryText': sample_questions,
    'Answer': ['Answer ' + str(i) for i in range(len(sample_questions))],
    'Category': ['Agriculture'] * len(sample_questions)
})

# Save to CSV
output_dir = Path('Data/test')
output_dir.mkdir(parents=True, exist_ok=True)

sample_file = output_dir / 'sample_questions.csv'
df_sample.to_csv(sample_file, index=False)

print(f"✓ Created sample dataset: {sample_file}")
print(f"  Total questions: {len(df_sample)}")
print(f"  Expected unique: ~4-5 (depending on thresholds)")
print()
print("Run the deduplication pipeline:")
print(f"  python deduplicate_questions.py --input {sample_file} --output Data/test/sample_deduplicated.csv")
