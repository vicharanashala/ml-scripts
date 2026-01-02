# Agricultural Question Deduplication & Q&A Generation Pipeline

A comprehensive data processing pipeline for agricultural questions from KCC (Kisan Call Center) datasets. The system performs deduplication across multiple states and generates high-quality Q&A pairs using Claude AI.

## Overview

This project processes agricultural questions from multiple Indian states, removes duplicates using hybrid matching (exact, fuzzy, and semantic), and generates comprehensive Q&A pairs for farmers.

### Key Features

- **Multi-State Processing**: Handles data from 12 states (Tamil Nadu, West Bengal, Odisha, Haryana, Bihar, Madhya Pradesh, Chhattisgarh, Andhra Pradesh, Telangana, Uttarakhand, Karnataka, Maharashtra)
- **Hybrid Deduplication**: Combines exact, fuzzy, and semantic matching for 95-98% duplicate reduction
- **Batch Q&A Generation**: Processes questions through Claude AI batch API for cost-effective generation
- **Cross-Dataset Analysis**: Identifies duplicates across different datasets

## Project Structure

```
unique-qs/
├── scripts/                    # Processing scripts
│   ├── data_processing/       # Deduplication and filtering
│   └── qa_generation/         # Q&A generation
├── outputs/                   # Final outputs
│   ├── final/                # Deduplicated datasets
│   │   ├── State_Paddy/     # 12 state Paddy datasets (47k unique questions)
│   │   ├── UP_Data/         # UP Paddy & Wheat datasets
│   │   └── Cross_Duplicates/
│   └── qa_results/           # Generated Q&A pairs
├── utils/                     # Utility modules
├── config.yaml               # Configuration
└── requirements.txt          # Dependencies
```

## Results Summary

### State-Wise Paddy Processing
- **Total States**: 12
- **Raw Questions**: 1,776,110
- **Unique Questions**: 41,325
- **Overall Reduction**: 97.67%

### Top States by Unique Questions
1. Odisha: 10,097
2. Tamil Nadu: 7,154
3. West Bengal: 5,815
4. Bihar: 4,210
5. Haryana: 3,400

### UP Data Processing
- **UP Paddy**: 834 unique questions → 834 Q&A pairs
- **UP Wheat**: 926 unique questions → 926 Q&A pairs (98.62% reduction from 67k)

## Installation

```bash
# Clone repository
git clone <repository-url>
cd unique-qs

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### 1. Deduplication

```bash
PYTHONPATH=. python scripts/data_processing/deduplicate_questions.py \
  --input <input_file.csv> \
  --output <output_file.csv> \
  --column QueryText
```

### 2. Q&A Generation

```bash
python scripts/qa_generation/generate_punjab_qa.py \
  --input <input_file.csv> \
  --output <output_file.csv> \
  --rows all \
  --api-key <your-api-key>
```

### 3. Cross-Dataset Duplicate Detection

```bash
PYTHONPATH=. python scripts/data_processing/check_cross_duplicates.py \
  --target <target_file.csv> \
  --reference <reference_file.csv> \
  --output <output_file.csv>
```

## Key Technologies

- **Python 3.12**
- **Pandas**: Data manipulation
- **Sentence Transformers**: Semantic similarity (paraphrase-multilingual-mpnet-base-v2)
- **Anthropic Claude**: Q&A generation (Batch API)
- **RapidFuzz**: Fuzzy string matching
- **PyTorch**: GPU acceleration for embeddings

## Configuration

Edit `config.yaml` to customize:
- Similarity thresholds
- Batch processing parameters
- Model selection
- Output formats

## Performance

- **Deduplication Speed**: ~10-15 minutes per state (GPU-accelerated)
- **Q&A Generation**: ~15-20 minutes per 1,000 questions (batch API)
- **Average Reduction**: 97.4% across all states

## Output Files

### Final Datasets
- `outputs/final/State_Paddy/`: State-wise unique Paddy questions
- `outputs/final/State_Paddy/STATE_PADDY_SUMMARY.csv`: Summary statistics

### Q&A Results
- `outputs/qa_results/State_Paddy/`: Generated Q&A pairs
- `outputs/qa_results/UP_Data/`: UP-specific Q&A results

## License

[Add your license here]

## Contributors

[Add contributors]

## Acknowledgments

- KCC (Kisan Call Center) for agricultural data
- Anthropic for Claude AI API
- Sentence Transformers community
