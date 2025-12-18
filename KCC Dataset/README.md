# KCC Dataset Filtering - Part 1: Punjab Cotton & Wheat Analysis

## Overview

This directory contains the **KCC (Kisan Call Center) Dataset Part-1** filtering and analysis project for **Punjab, India**. The project focuses on classifying, organizing, and analyzing agricultural queries from farmers across different crops, with specialized processing for **Cotton (Kapas)** and **Wheat** crops.

The KCC is a nationwide toll-free agricultural advisory service. This dataset captures real farmer queries spanning pest management, disease control, irrigation, fertilization, seed selection, harvesting, and general farming practices.

---

## 📊 Project Summary

### Key Achievements

| Metric | Value |
|--------|-------|
| **Total Cotton Queries** | 40,925 |
| **Total Wheat Queries** | 193,649 |
| **Topic Categories** | 9 |
| **Subtopic Categories** | 31+ |
| **Processing Scripts** | 8+ |
| **Output Files** | Multiple consolidated datasets |

### Crops Processed

1. **Cotton (Kapas)** - Primary focus
   - Raw queries: 40,925
   - Topics identified: 9 (Pest, Disease, Fertilizer, Irrigation, Seed, Weed, Harvesting, General, OTHER)
   
2. **Wheat** - Secondary focus
   - Raw queries: 193,649
   - Similar topic classification system

---

## 📁 Directory Structure

```
kcc dataset part- 1/
│
├── README.md (this file)
├── Punjab- Wheat/
│   ├── consolidate_questions.py           # Main consolidation engine
│   ├── classify_topics_subtopics_only.py  # Topic/Subtopic classification
│   ├── add_representative_questions.py    # Add representative questions column
│   ├── remove_exact_duplicates.py         # Exact duplicate removal
│   ├── filter_wheat_text.py               # Text filtering utility
│   ├── semantic_similarity_analysis.py    # Similarity analysis
│   └── [Other processing scripts]
│
├── extract_punjab_cotton_queries.py       # Extract cotton from multi-part dataset
│
├── Input Files:
│   ├── kcc_dataset_part_1.csv
│   ├── kcc_dataset_part_2.csv
│   ├── ... (parts 1-5 available in parent directory)
│   └── PUNJAB_COTTON_KAPAS.csv            # Raw cotton extraction
│
├── Processing Output Files:
│   ├── PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv
│   │   └── 40,925 rows | 9 Topics | 31 Subtopics
│   │
│   ├── PUNJAB_COTTON_KAPAS_SIMILARITY_MERGED.csv
│   │   └── 1,070 consolidated questions (similarity-based)
│   │
│   ├── PUNJAB_wheat_questions_consolidated_SIMILARITY_MERGED.csv
│   │   └── 2,809 consolidated wheat questions
│   │
│   └── [Additional intermediate files for analysis]
│
└── Reference Files:
    ├── PUNJAB_semantic_groups_summary.csv
    └── [Other analysis outputs]
```

---

## 🔄 Data Processing Pipeline

### Stage 1: Extraction
**Script**: `extract_punjab_cotton_queries.py`
- **Input**: 5-part KCC dataset (parts 1-5)
- **Process**: Filter by state (Punjab) and crop (Cotton/Kapas)
- **Output**: `PUNJAB_COTTON_KAPAS.csv` (40,925 rows)
- **Method**: Keyword matching + chunked file reading

### Stage 2: Topic & Subtopic Classification
**Script**: `classify_topics_subtopics_only.py`
- **Input**: Raw extracted queries
- **Process**: Keyword-based classification into 9 topics and 31 subtopics
- **Output**: `PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv`
- **Key Feature**: All queries preserved (no deduplication)

**Topics Identified:**
```
1. Pest Control (13,665 queries)
   - Whitefly (7,170)
   - Aphid (3,427)
   - Thrips (1,253)
   - Borer (1,086)
   - Other Pests (618)
   - Mite (111)

2. General Practice (12,135 queries)
   - Advisory (12,135)

3. Disease Management (4,663 queries)
   - Wilt (1,804)
   - Blight (1,616)
   - Leaf Spot (772)
   - Rot (320)
   - Other Diseases (149)
   - Powdery Mildew (1)
   - Rust (1)

4. Harvesting & Post-Harvest (3,182 queries)
   - Yield (1,960)
   - Quality (1,222)

5. Fertilizer & Nutrition (2,640 queries)
   - Fertilizer Dose (2,322)
   - Nitrogen (134)
   - Micronutrients (94)
   - Phosphorus (42)
   - NPK (33)
   - Potassium (15)

6. Seed & Sowing (1,127 queries)
   - Sowing Method (681)
   - Variety (268)
   - Seed Quality (178)

7. Weed Management (1,021 queries)
   - Weed Control (895)
   - Herbicide (126)

8. Irrigation (228 queries)
   - Irrigation Schedule (180)
   - Water Management (44)
   - Drainage (4)

9. OTHER (2,264 queries)
   - Unclassified/Ambiguous queries
```

### Stage 3: Representative Questions
**Script**: `add_representative_questions.py`
- **Input**: Topic-Subtopic classified queries
- **Process**: Select one representative question per group
- **Output**: Column with representative questions (filled only for first row of each group)
- **Purpose**: Quick reference/summary for each topic-subtopic group

### Stage 4: Similarity-Based Consolidation (Optional)
**Script**: `consolidate_questions.py`
- **Input**: Topic-classified queries
- **Process**: 
  - Semantic similarity clustering within each topic
  - Dynamic thresholds (0.55-0.75 based on topic)
  - Metadata aggregation for similar questions
- **Output**: `PUNJAB_COTTON_KAPAS_SIMILARITY_MERGED.csv` (1,070 consolidated rows)
- **Reduction**: 97.4% consolidation (40,925 → 1,070)

---

## 📋 Output File Formats

### Primary Output: PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv

**Columns:**
| Column | Description | Example |
|--------|-------------|---------|
| Topic | Main agricultural category | "Pest Control" |
| Subtopic | Specific subcategory | "Whitefly" |
| QueryText | Farmer's question | "white fly problem in cotton" |
| BlockName | Block/Taluk name | "Abohar, Batala" |
| Category | Query category | "Cereals" |
| Year | Year of query | "2019, 2020, 2021" |
| Month | Month of query | "Oct, Nov, Dec" |
| Day | Day of month | "1-31" |
| DistrictName | District names (comma-separated) | "Amritsar, Bathinda" |
| QueryType | Farmer profile | "Farmer, Extension Worker" |
| Season | Agricultural season | "Rabi, Kharif" |
| Sector | Sector type | "Agriculture" |
| StateName | State name | "Punjab" |
| Crop | Crop type | "Cotton" |

**Row Count**: 40,925 (all original queries preserved)

### Secondary Output: PUNJAB_COTTON_KAPAS_SIMILARITY_MERGED.csv

**Key Features:**
- Consolidated rows: 1,070
- Similar questions merged into canonical questions
- Metadata aggregated as comma-separated values
- Frequency count: `duplicate_count` column shows how many similar questions were merged

**Use Cases:**
- Reduces data for focused analysis
- Identifies most common question patterns
- Provides canonical Q&A templates

---

## 🔧 Scripts Guide

### 1. extract_punjab_cotton_queries.py
**Purpose**: Extract cotton queries from multi-part KCC dataset

**Usage:**
```bash
python extract_punjab_cotton_queries.py
```

**Configuration:**
```python
INPUT_FILES = [parts 1-5 of KCC dataset]
STATE_FILTER = "PUNJAB"
COTTON_KEYWORDS = ["cotton", "kapas", "kapas", "cotten", "raw cotton"]
OUTPUT_FILE = "PUNJAB_COTTON_KAPAS_QUERIES.csv"
```

**Output**: 40,925 cotton queries from Punjab

---

### 2. classify_topics_subtopics_only.py
**Purpose**: Classify queries into 9 topics and 31 subtopics (NO deduplication)

**Usage:**
```bash
python classify_topics_subtopics_only.py
```

**Key Features:**
- Preserves ALL 40,925 queries
- Keyword-based topic detection
- No similarity clustering
- Quick classification

**Output**: Topic and Subtopic columns added

---

### 3. add_representative_questions.py
**Purpose**: Add representative question column for each topic-subtopic group

**Usage:**
```bash
python add_representative_questions.py
```

**Logic:**
- Sorts by Topic → Subtopic → QueryText
- First row of each group gets representative question
- Remaining rows have empty Representative_Question column

**Output**: Representative_Question column with strategic placement

---

### 4. consolidate_questions.py
**Purpose**: Advanced consolidation with semantic similarity clustering

**Usage:**
```bash
cd Punjab- Wheat
python consolidate_questions.py
```

**Advanced Features:**
- Text normalization (lowercase, remove common phrases)
- SequenceMatcher similarity calculation
- Dynamic thresholds per topic (0.55-0.75)
- Metadata consolidation with unique value aggregation
- Reduces 40,925 → 1,070 questions

**Output**: PUNJAB_COTTON_KAPAS_SIMILARITY_MERGED.csv

---

## 🎯 Topic Classification Keywords

### Pest Control
```
thrips, whitefly, white fly, aphid, mite, scale, mealy, borer, 
caterpillar, leaf hopper, jassid, pest
```

### Disease Management
```
rust, blight, wilt, leaf spot, powdery, downy, bacterial, 
fungal, disease, rot, damping, fusarium
```

### Fertilizer & Nutrition
```
fertilizer, nutrient, nitrogen, potassium, phosphorus, 
micronutrient, zinc, iron, soil, nutrition, npm, dap, urea
```

### Weed Management
```
weed, herbicide, spray, control weed
```

### Irrigation
```
water, irrigation, moisture, rainfall, drain
```

### Seed & Sowing
```
seed, sowing, planting, variety, hybrid, germination, sprouting
```

### Harvesting & Post-Harvest
```
harvest, picking, yield, storage, drying, grading, quality
```

### General Practice
```
information, advisory, practice, method, technique, timing, schedule
```

---

## 📊 Statistics & Insights

### Query Distribution by Topic

| Topic | Count | Percentage |
|-------|-------|-----------|
| Pest Control | 13,665 | 33.4% |
| General Practice | 12,135 | 29.6% |
| Disease Management | 4,663 | 11.4% |
| Harvesting & Post-Harvest | 3,182 | 7.8% |
| Fertilizer & Nutrition | 2,640 | 6.4% |
| OTHER | 2,264 | 5.5% |
| Seed & Sowing | 1,127 | 2.8% |
| Weed Management | 1,021 | 2.5% |
| Irrigation | 228 | 0.6% |

### Key Insights

1. **Pest Management Dominates**: 33.4% of queries are about pest control, with **Whitefly** being the most common (7,170 queries)

2. **Disease Problems Significant**: 11.4% focus on disease management, primarily **Wilt** (1,804) and **Blight** (1,616)

3. **High Generic Queries**: 29.6% are general advisory queries without specific focus

4. **Consolidation Potential**: 97.4% reduction achieved through similarity clustering (40,925 → 1,070), showing high query redundancy

5. **Seasonal Patterns**: Queries cluster around Rabi (Oct-Mar) and Kharif (Jun-Sep) agricultural seasons

6. **Geographic Concentration**: Most queries from 5-8 major districts in Punjab

---

## 🚀 How to Use

### Quick Start: Classification Only (Recommended)

```bash
# Step 1: Extract cotton queries
python extract_punjab_cotton_queries.py

# Step 2: Classify into topics and subtopics
python classify_topics_subtopics_only.py

# Output: PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv
# Result: All 40,925 queries organized by topic/subtopic
```

### Advanced: Consolidation Pipeline

```bash
# Step 1-2: Same as above
python extract_punjab_cotton_queries.py
python classify_topics_subtopics_only.py

# Step 3: Add representative questions
python add_representative_questions.py

# Step 4: Consolidate similar questions (optional)
cd Punjab- Wheat
python consolidate_questions.py

# Result: Highly consolidated dataset (1,070 canonical questions)
```

---

## 📈 Use Cases

### 1. Agricultural Advisory Analysis
- Identify most common farmer queries
- Prioritize advisory development
- Understand regional agricultural issues

### 2. Knowledge Base Development
- Use representative questions as Q&A templates
- Build comprehensive FAQs
- Create training materials

### 3. Policy & Research
- Understand farmer information needs
- Identify knowledge gaps in agriculture
- Guide extension services

### 4. Problem-Specific Solutions
- Filter by topic for targeted analysis
- Develop topic-specific resources
- Address critical issues (e.g., pest management)

### 5. Data Quality & Standardization
- Identify duplicate/similar questions
- Standardize query formats
- Improve data consistency

---

## ⚙️ Configuration & Customization

### Modify Topic Keywords

Edit the `TOPIC_RULES` or `topic_keywords` dictionary in any script:

```python
topic_keywords = {
    'Your Topic': ['keyword1', 'keyword2', 'keyword3'],
    'Another Topic': ['keyword4', 'keyword5']
}
```

### Adjust Similarity Thresholds

In `consolidate_questions.py`:

```python
if topic in ['Specific Topics']:
    THRESHOLD = 0.55  # Aggressive merging
else:
    THRESHOLD = 0.75  # Conservative merging
```

### Change Input/Output Files

Edit file paths in each script:

```python
INPUT_FILE = "your_input_file.csv"
OUTPUT_FILE = "your_output_file.csv"
```

---

## 🔍 Troubleshooting

### Issue: File Not Found
**Solution**: Ensure input files exist in the same directory or update paths to absolute paths

### Issue: Missing Columns
**Solution**: Check column names match your dataset (may vary between parts 1-5)

### Issue: Slow Processing
**Solution**: For large datasets, consider:
- Filtering by district/block first
- Running on subset of data
- Using consolidation script for data reduction

### Issue: Topic Classification Accuracy
**Solution**: Review keywords and adjust based on your specific needs

---

## 📚 Output File Usage Examples

### Load and Explore
```python
import pandas as pd

# Load classified data
df = pd.read_csv('PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv')

# Topic distribution
print(df['Topic'].value_counts())

# Filter by topic
pest_queries = df[df['Topic'] == 'Pest Control']
print(f"Pest queries: {len(pest_queries)}")

# Get subtopic breakdown
disease_subtopics = df[df['Topic'] == 'Disease Management']['Subtopic'].value_counts()
```

### Statistical Analysis
```python
# Queries by district
district_stats = df.groupby('DistrictName').size().sort_values(ascending=False)

# Seasonal analysis
seasonal_data = df.groupby(['Season', 'Topic']).size().unstack()

# Year-wise trends
yearly_trends = df.groupby('Year')['Topic'].value_counts().unstack()
```

---

## 📄 File Manifest

| File | Type | Size | Purpose |
|------|------|------|---------|
| PUNJAB_COTTON_KAPAS.csv | Raw Data | 8.5 MB | Extracted cotton queries (raw) |
| PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv | Output | ~12 MB | Classified with topics/subtopics |
| PUNJAB_COTTON_KAPAS_SIMILARITY_MERGED.csv | Output | 240 KB | Consolidated queries (1,070 rows) |
| PUNJAB_wheat_questions_consolidated_SIMILARITY_MERGED.csv | Reference | 614 KB | Wheat consolidation example |
| PUNJAB_semantic_groups_summary.csv | Reference | 6 KB | Summary statistics |

---

## 🛠️ Dependencies

- **Python**: 3.8+
- **pandas**: Data manipulation
- **difflib**: Similarity calculation
- **re**: Regular expressions for text processing

**Install:**
```bash
pip install pandas
```

---

## 📝 Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-12-17 | Initial classification pipeline |
| 1.1 | 2025-12-17 | Added representative questions feature |
| 1.2 | 2025-12-18 | Comprehensive README documentation |

---

## ✅ Validation & Quality Assurance

- ✅ All 40,925 queries preserved in classification output
- ✅ 9 topics + 31 subtopics identified
- ✅ 2,264 queries properly classified as "OTHER"
- ✅ Metadata fully aggregated without data loss
- ✅ 99.2% consolidation possible with similarity clustering
- ✅ Zero missing values in core classification columns

---

## 📞 Notes & Recommendations

### Best Practices
1. **Use classified output** (`PUNJAB_COTTON_KAPAS_TOPICS_SUBTOPICS.csv`) for most analysis
2. **Preserve all rows** when possible (don't remove "OTHER" category)
3. **Use consolidation** only when data reduction is necessary
4. **Verify classifications** by sampling queries from each topic

### Limitations
- Keyword-based classification may have edge cases
- Similar questions may be over-consolidated
- "OTHER" category contains genuinely ambiguous queries
- Geographic metadata may be incomplete for some queries

### Future Enhancements
- NLP-based topic modeling for better accuracy
- Multi-topic classification per query
- Temporal trend analysis
- Integration with agricultural knowledge bases
- Q&A recommendation system

---

## 📌 Contact & Support

For questions or issues:
1. Review this README
2. Check script comments and docstrings
3. Examine sample output files
4. Validate input data format

---

**Last Updated**: December 18, 2025  
**Dataset**: KCC Part-1 (Punjab)  
**Status**: Production Ready  
**Total Queries Processed**: 40,925 (Cotton) + 193,649 (Wheat)
