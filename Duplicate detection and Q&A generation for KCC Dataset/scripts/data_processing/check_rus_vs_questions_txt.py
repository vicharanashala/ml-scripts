#!/usr/bin/env python3
"""
Check RUS dataset against questions.txt and create CSV with marker columns.

Usage:
    python scripts/data_processing/check_rus_vs_questions_txt.py \
        --target "Data/RUS - Q and LLM - A.xlsx" \
        --reference Data/questions.txt \
        --output Data/RUS_with_questions_txt_check.csv
"""

import os
import sys
import argparse
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Optional
from tqdm import tqdm
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.text_processing import normalize_text, clean_question
from utils.similarity import (
    fuzzy_similarity, 
    EmbeddingGenerator,
    compute_semantic_similarity
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RUSvsQuestionsTxtChecker:
    """
    Check RUS dataset questions against questions.txt.
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """
        Initialize the checker with configuration.
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.embedding_model = None
        
        # Initialize semantic model if enabled
        if self.config['deduplication']['semantic']['enabled']:
            logger.info("Initializing semantic similarity model...")
            model_name = self.config['deduplication']['semantic']['model']
            # Remove 'sentence-transformers/' prefix if present
            if model_name.startswith('sentence-transformers/'):
                model_name = model_name.replace('sentence-transformers/', '')
            
            self.embedding_model = EmbeddingGenerator(
                model_name=model_name,
                use_gpu=self.config['deduplication']['semantic']['use_gpu'],
                batch_size=self.config['deduplication']['semantic']['batch_size']
            )
    
    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file."""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def load_rus_dataset(self, filepath: str, question_column: str = 'Question') -> pd.DataFrame:
        """
        Load RUS dataset from Excel file.
        
        Args:
            filepath: Path to Excel file
            question_column: Name of column containing questions
        
        Returns:
            DataFrame with loaded data
        """
        logger.info(f"Loading RUS dataset from {filepath}...")
        
        if filepath.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(filepath)
        elif filepath.endswith('.csv'):
            df = pd.read_csv(filepath)
        else:
            raise ValueError(f"Unsupported file format: {filepath}")
        
        # Validate question column exists
        if question_column not in df.columns:
            raise ValueError(f"Column '{question_column}' not found in {filepath}")
        
        # Remove rows with empty questions
        df = df[df[question_column].notna()].copy()
        df = df[df[question_column].astype(str).str.strip() != ''].copy()
        
        logger.info(f"Loaded {len(df)} rows with valid questions")
        return df
    
    def load_questions_txt(self, filepath: str) -> list:
        """
        Load questions from text file (one question per line).
        
        Args:
            filepath: Path to text file
        
        Returns:
            List of questions
        """
        logger.info(f"Loading questions from {filepath}...")
        
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Remove empty lines and strip whitespace
        questions = [line.strip() for line in lines if line.strip()]
        
        # Remove header if it looks like one
        if questions and questions[0].upper().startswith('QUESTION'):
            questions = questions[1:]
        
        logger.info(f"Loaded {len(questions)} questions")
        return questions
    
    def find_similar_question(self, 
                             target_question: str, 
                             reference_questions: list) -> Tuple[bool, Optional[str], float, str]:
        """
        Find if a similar question exists in the reference dataset.
        
        Args:
            target_question: Question to check
            reference_questions: List of reference questions
        
        Returns:
            Tuple of (has_similar, similar_question, similarity_score, match_type)
        """
        # Normalize target question
        target_normalized = clean_question(target_question)
        
        # Stage 1: Exact matching
        if self.config['deduplication']['exact']['enabled']:
            for ref_q in reference_questions:
                ref_normalized = clean_question(ref_q)
                if target_normalized == ref_normalized:
                    return True, ref_q, 1.0, 'exact'
        
        # Stage 2: Fuzzy matching
        if self.config['deduplication']['fuzzy']['enabled']:
            fuzzy_threshold = self.config['deduplication']['fuzzy']['threshold']
            fuzzy_algorithm = self.config['deduplication']['fuzzy']['algorithm']
            
            best_fuzzy_score = 0.0
            best_fuzzy_match = None
            
            for ref_q in reference_questions:
                ref_normalized = clean_question(ref_q)
                score = fuzzy_similarity(target_normalized, ref_normalized, fuzzy_algorithm)
                
                if score >= fuzzy_threshold and score > best_fuzzy_score:
                    best_fuzzy_score = score
                    best_fuzzy_match = ref_q
            
            if best_fuzzy_match is not None:
                return True, best_fuzzy_match, best_fuzzy_score, 'fuzzy'
        
        # Stage 3: Semantic matching
        if self.config['deduplication']['semantic']['enabled'] and self.embedding_model:
            semantic_threshold = self.config['deduplication']['semantic']['similarity_threshold']
            
            best_semantic_score = 0.0
            best_semantic_match = None
            
            for ref_q in reference_questions:
                score = compute_semantic_similarity(
                    target_question, 
                    ref_q, 
                    self.embedding_model
                )
                
                if score >= semantic_threshold and score > best_semantic_score:
                    best_semantic_score = score
                    best_semantic_match = ref_q
            
            if best_semantic_match is not None:
                return True, best_semantic_match, best_semantic_score, 'semantic'
        
        # No match found
        return False, None, 0.0, 'none'
    
    def check_duplicates(self,
                        target_file: str,
                        reference_file: str,
                        output_file: str,
                        question_column: str = 'Question') -> pd.DataFrame:
        """
        Check for duplicates between RUS dataset and questions.txt.
        
        Args:
            target_file: Path to RUS dataset (Excel or CSV)
            reference_file: Path to questions.txt
            output_file: Path to output CSV file
            question_column: Name of column containing questions in RUS
        
        Returns:
            DataFrame with duplicate information
        """
        # Load datasets
        target_df = self.load_rus_dataset(target_file, question_column)
        reference_questions = self.load_questions_txt(reference_file)
        
        logger.info(f"Target dataset (RUS): {len(target_df)} questions")
        logger.info(f"Reference dataset (questions.txt): {len(reference_questions)} questions")
        
        # Initialize new columns
        target_df['has_similar_in_questions_txt'] = False
        target_df['similar_questions_txt_question'] = ''
        target_df['similarity_score'] = 0.0
        target_df['match_type'] = 'none'
        
        # Check each target question
        logger.info("Checking for similar questions...")
        matches_found = 0
        
        for idx, row in tqdm(target_df.iterrows(), total=len(target_df), desc="Processing"):
            target_question = row[question_column]
            
            has_similar, similar_q, score, match_type = self.find_similar_question(
                target_question,
                reference_questions
            )
            
            target_df.at[idx, 'has_similar_in_questions_txt'] = has_similar
            target_df.at[idx, 'similar_questions_txt_question'] = similar_q if similar_q else ''
            target_df.at[idx, 'similarity_score'] = score
            target_df.at[idx, 'match_type'] = match_type
            
            if has_similar:
                matches_found += 1
        
        # Save results
        logger.info(f"Saving results to {output_file}...")
        target_df.to_csv(output_file, index=False)
        
        # Generate summary report
        self._generate_report(target_df, output_file, matches_found)
        
        return target_df
    
    def _generate_report(self, df: pd.DataFrame, output_file: str, matches_found: int):
        """Generate summary report."""
        total = len(df)
        match_percentage = (matches_found / total * 100) if total > 0 else 0
        
        # Count by match type
        match_type_counts = df['match_type'].value_counts().to_dict()
        
        report = f"""
╔══════════════════════════════════════════════════════════════╗
║        RUS Dataset vs questions.txt Report                    ║
╚══════════════════════════════════════════════════════════════╝

📊 Summary Statistics
─────────────────────────────────────────────────────────────
Total questions in RUS dataset:        {total:,}
Questions with similar match in txt:   {matches_found:,}
Match percentage:                       {match_percentage:.2f}%

📈 Matches by Type
─────────────────────────────────────────────────────────────
"""
        for match_type in ['exact', 'fuzzy', 'semantic', 'none']:
            count = match_type_counts.get(match_type, 0)
            percentage = (count / total * 100) if total > 0 else 0
            report += f"{match_type.capitalize():15} {count:6,} ({percentage:5.2f}%)\n"
        
        # Average similarity score for matches
        matches_df = df[df['has_similar_in_questions_txt'] == True]
        if len(matches_df) > 0:
            avg_score = matches_df['similarity_score'].mean()
            min_score = matches_df['similarity_score'].min()
            max_score = matches_df['similarity_score'].max()
            
            report += f"""
📊 Similarity Scores (for matches only)
─────────────────────────────────────────────────────────────
Average:  {avg_score:.4f}
Minimum:  {min_score:.4f}
Maximum:  {max_score:.4f}
"""
        
        report += f"""
💾 Output
─────────────────────────────────────────────────────────────
Results saved to: {output_file}

✅ Processing Complete!
"""
        
        print(report)
        
        # Save report to file
        report_file = output_file.replace('.csv', '_report.txt')
        with open(report_file, 'w') as f:
            f.write(report)
        logger.info(f"Report saved to {report_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check RUS dataset against questions.txt"
    )
    parser.add_argument(
        '--target', '-t',
        required=True,
        help='Path to RUS dataset (Excel or CSV)'
    )
    parser.add_argument(
        '--reference', '-r',
        required=True,
        help='Path to questions.txt'
    )
    parser.add_argument(
        '--output', '-o',
        required=True,
        help='Path to output CSV file'
    )
    parser.add_argument(
        '--question-column', '-q',
        default='Question',
        help='Name of column containing questions in RUS (default: Question)'
    )
    parser.add_argument(
        '--config', '-c',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Validate input files exist
    if not os.path.exists(args.target):
        logger.error(f"Target file not found: {args.target}")
        sys.exit(1)
    
    if not os.path.exists(args.reference):
        logger.error(f"Reference file not found: {args.reference}")
        sys.exit(1)
    
    # Create output directory if needed
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Run duplicate check
    checker = RUSvsQuestionsTxtChecker(args.config)
    checker.check_duplicates(
        target_file=args.target,
        reference_file=args.reference,
        output_file=args.output,
        question_column=args.question_column
    )


if __name__ == "__main__":
    main()
