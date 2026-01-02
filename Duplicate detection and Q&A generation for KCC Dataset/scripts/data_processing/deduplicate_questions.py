#!/usr/bin/env python3
"""
Question Deduplication Pipeline

A sophisticated pipeline for removing duplicate and semantically similar questions
from agricultural query datasets. Uses a hybrid approach combining:
1. Exact duplicate removal
2. Fuzzy matching for near-duplicates
3. Semantic similarity detection using sentence embeddings

Usage:
    python deduplicate_questions.py --input Data/AI_ANS_25K.csv --output Data/filtered/AI_ANS_25K_deduplicated.csv
    python deduplicate_questions.py --config config.yaml
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Set
import yaml
import pandas as pd
import numpy as np
from tqdm import tqdm

from utils import (
    normalize_text, clean_question, is_valid_question,
    EmbeddingGenerator, compute_cosine_similarity_matrix, find_similar_pairs,
    fuzzy_similarity,
    cluster_by_pairs, get_cluster_representatives, get_items_to_remove,
    DeduplicationReport, print_sample_duplicates
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class QuestionDeduplicator:
    """
    Main deduplication pipeline class.
    """
    
    def __init__(self, config: dict):
        """
        Initialize deduplicator with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.report = DeduplicationReport()
        self.embedding_model = None
        
        # Track clusters for group export
        self.last_clusters = None
        self.last_representatives = None
        self.last_df = None
        self.last_question_col = None
    
    def load_data(self, filepath: str) -> pd.DataFrame:
        """
        Load data from CSV or Excel file.
        
        Args:
            filepath: Path to input file
        
        Returns:
            DataFrame with loaded data
        """
        logger.info(f"Loading data from {filepath}")
        
        file_path = Path(filepath)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {filepath}")
        
        # Determine file type and load
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(filepath, encoding='utf-8')
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        logger.info(f"Loaded {len(df)} rows")
        return df
    
    def save_data(self, df: pd.DataFrame, filepath: str):
        """
        Save data to CSV or Excel file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
        """
        logger.info(f"Saving data to {filepath}")
        
        # Create output directory if needed
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        
        # Determine file type and save
        file_path = Path(filepath)
        if file_path.suffix.lower() == '.csv':
            df.to_csv(filepath, index=False, encoding='utf-8')
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            df.to_excel(filepath, index=False)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
        
        logger.info(f"Saved {len(df)} rows")
    
    def remove_exact_duplicates(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Remove exact duplicate questions (Stage 1).
        
        Args:
            df: Input DataFrame
            column: Name of question column
        
        Returns:
            DataFrame with exact duplicates removed
        """
        logger.info("Stage 1: Removing exact duplicates...")
        
        original_count = len(df)
        
        # Normalize text for comparison
        df['_normalized'] = df[column].apply(
            lambda x: normalize_text(str(x)) if pd.notna(x) else ""
        )
        
        # Remove duplicates (keep first occurrence)
        df_dedup = df.drop_duplicates(subset=['_normalized'], keep='first')
        df_dedup = df_dedup.drop(columns=['_normalized'])
        
        removed = original_count - len(df_dedup)
        self.report.set_exact_duplicates(removed)
        
        logger.info(f"Removed {removed} exact duplicates ({removed/original_count*100:.2f}%)")
        
        return df_dedup.reset_index(drop=True)
    
    def remove_fuzzy_duplicates(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Remove fuzzy duplicate questions (Stage 2).
        
        Args:
            df: Input DataFrame
            column: Name of question column
        
        Returns:
            DataFrame with fuzzy duplicates removed
        """
        if not self.config['deduplication']['fuzzy']['enabled']:
            logger.info("Stage 2: Fuzzy matching disabled, skipping...")
            self.report.set_fuzzy_duplicates(0)
            return df
        
        logger.info("Stage 2: Removing fuzzy duplicates...")
        
        original_count = len(df)
        threshold = self.config['deduplication']['fuzzy']['threshold']
        algorithm = self.config['deduplication']['fuzzy']['algorithm']
        max_comparisons = self.config['deduplication']['fuzzy'].get('max_comparisons', 100000)
        use_sampling = self.config['deduplication']['fuzzy'].get('use_sampling', True)
        
        # Normalize questions
        questions = df[column].apply(lambda x: clean_question(str(x)) if pd.notna(x) else "").tolist()
        
        # For large datasets, use optimized approach
        n = len(questions)
        total_comparisons = n * (n - 1) // 2
        
        if use_sampling and total_comparisons > max_comparisons:
            logger.info(f"Large dataset detected ({n} questions, {total_comparisons:,} comparisons)")
            logger.info(f"Using optimized sampling approach (max {max_comparisons:,} comparisons)")
            similar_pairs = self._fuzzy_match_with_sampling(questions, threshold, algorithm, max_comparisons)
        else:
            logger.info(f"Computing fuzzy similarities (threshold={threshold})...")
            similar_pairs = []
            for i in tqdm(range(len(questions)), desc="Fuzzy matching"):
                for j in range(i + 1, len(questions)):
                    sim = fuzzy_similarity(questions[i], questions[j], algorithm)
                    if sim >= threshold:
                        similar_pairs.append((i, j, sim))
        
        logger.info(f"Found {len(similar_pairs)} fuzzy duplicate pairs")
        
        # Cluster similar questions
        if similar_pairs:
            clusters = cluster_by_pairs(len(questions), similar_pairs)
            
            # Select representatives (keep first occurrence)
            representatives = get_cluster_representatives(clusters, strategy="first")
            indices_to_remove = get_items_to_remove(len(questions), clusters, representatives)
            
            # Remove duplicates
            df_dedup = df.drop(index=list(indices_to_remove)).reset_index(drop=True)
            
            removed = original_count - len(df_dedup)
            self.report.set_fuzzy_duplicates(removed)
            
            logger.info(f"Removed {removed} fuzzy duplicates ({removed/original_count*100:.2f}%)")
            
            return df_dedup
        else:
            self.report.set_fuzzy_duplicates(0)
            return df
    
    def _fuzzy_match_with_sampling(self, questions: list, threshold: float, 
                                   algorithm: str, max_comparisons: int) -> list:
        """
        Optimized fuzzy matching using smart sampling for large datasets.
        
        Strategy: For each question, only compare with a sample of other questions
        that are likely to be similar (based on length and first few characters).
        """
        import random
        from collections import defaultdict
        
        similar_pairs = []
        n = len(questions)
        
        # Group questions by length buckets (±20% length tolerance)
        length_buckets = defaultdict(list)
        for i, q in enumerate(questions):
            bucket = len(q) // 10  # Bucket by length (groups of 10 chars)
            length_buckets[bucket].append(i)
        
        # For each question, compare with questions in nearby length buckets
        comparisons_made = 0
        max_per_question = min(100, max_comparisons // n)  # Limit comparisons per question
        
        for i in tqdm(range(n), desc="Fuzzy matching (optimized)"):
            q_len = len(questions[i])
            bucket = q_len // 10
            
            # Get candidate indices from nearby buckets
            candidates = []
            for b in range(max(0, bucket - 2), bucket + 3):  # ±2 buckets
                candidates.extend(length_buckets[b])
            
            # Remove self and already processed
            candidates = [j for j in candidates if j > i]
            
            # Sample if too many candidates
            if len(candidates) > max_per_question:
                candidates = random.sample(candidates, max_per_question)
            
            # Compare with candidates
            for j in candidates:
                if comparisons_made >= max_comparisons:
                    break
                    
                sim = fuzzy_similarity(questions[i], questions[j], algorithm)
                if sim >= threshold:
                    similar_pairs.append((i, j, sim))
                
                comparisons_made += 1
            
            if comparisons_made >= max_comparisons:
                logger.info(f"Reached max comparisons limit ({max_comparisons:,})")
                break
        
        logger.info(f"Completed {comparisons_made:,} comparisons (vs {n*(n-1)//2:,} full)")
        return similar_pairs
    
    def remove_semantic_duplicates(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Remove semantically similar questions (Stage 3).
        
        Args:
            df: Input DataFrame
            column: Name of question column
        
        Returns:
            DataFrame with semantic duplicates removed
        """
        if not self.config['deduplication']['semantic']['enabled']:
            logger.info("Stage 3: Semantic similarity disabled, skipping...")
            self.report.set_semantic_duplicates(0)
            return df
        
        logger.info("Stage 3: Removing semantic duplicates...")
        
        original_count = len(df)
        threshold = self.config['deduplication']['semantic']['similarity_threshold']
        
        # Initialize embedding model if not already done
        if self.embedding_model is None:
            model_name = self.config['deduplication']['semantic']['model']
            use_gpu = self.config['deduplication']['semantic']['use_gpu']
            batch_size = self.config['deduplication']['semantic']['batch_size']
            
            self.embedding_model = EmbeddingGenerator(
                model_name=model_name,
                use_gpu=use_gpu,
                batch_size=batch_size
            )
        
        # Prepare questions
        questions = df[column].apply(lambda x: clean_question(str(x)) if pd.notna(x) else "").tolist()
        
        # Generate embeddings
        logger.info("Generating embeddings...")
        embeddings = self.embedding_model.encode(questions, show_progress=True)
        
        # Compute similarity matrix
        logger.info("Computing similarity matrix...")
        similarity_matrix = compute_cosine_similarity_matrix(embeddings)
        
        # Find similar pairs
        logger.info(f"Finding similar pairs (threshold={threshold})...")
        similar_pairs = find_similar_pairs(similarity_matrix, threshold)
        
        logger.info(f"Found {len(similar_pairs)} semantic duplicate pairs")
        
        # Cluster similar questions
        if similar_pairs:
            clusters = cluster_by_pairs(len(questions), similar_pairs)
            
            # Select representatives
            # Prefer longer questions (more complete)
            question_lengths = np.array([len(q) for q in questions])
            representatives = get_cluster_representatives(
                clusters, 
                scores=question_lengths,
                strategy="best"
            )
            
            # Store clusters and representatives for later export
            self.last_clusters = clusters
            self.last_representatives = representatives
            self.last_df = df.copy()
            self.last_question_col = column
            
            # Store some duplicate groups for reporting
            for cluster_id, items in list(clusters.items())[:20]:
                if len(items) > 1:
                    rep_idx = representatives[cluster_id]
                    group_questions = [questions[i] for i in items]
                    self.report.add_duplicate_group(
                        group=group_questions,
                        representative=questions[rep_idx],
                        similarity=threshold
                    )
            
            indices_to_remove = get_items_to_remove(len(questions), clusters, representatives)
            
            # Remove duplicates
            df_dedup = df.drop(index=list(indices_to_remove)).reset_index(drop=True)
            
            removed = original_count - len(df_dedup)
            self.report.set_semantic_duplicates(removed)
            
            logger.info(f"Removed {removed} semantic duplicates ({removed/original_count*100:.2f}%)")
            
            return df_dedup
        else:
            self.report.set_semantic_duplicates(0)
            return df
    
    def deduplicate(self, input_file: str, output_file: str, question_column: str) -> pd.DataFrame:
        """
        Run full deduplication pipeline.
        
        Args:
            input_file: Path to input file
            output_file: Path to output file
            question_column: Name of column containing questions
        
        Returns:
            Deduplicated DataFrame
        """
        self.report.start_timer()
        
        # Load data
        df = self.load_data(input_file)
        self.report.set_original_count(len(df))
        
        # Validate question column
        if question_column not in df.columns:
            raise ValueError(f"Column '{question_column}' not found in data. Available columns: {df.columns.tolist()}")
        
        # Filter invalid questions
        logger.info("Filtering invalid questions...")
        valid_mask = df[question_column].apply(lambda x: is_valid_question(str(x)) if pd.notna(x) else False)
        df = df[valid_mask].reset_index(drop=True)
        logger.info(f"Kept {len(df)} valid questions")
        
        # Stage 1: Exact duplicates
        if self.config['deduplication']['exact']['enabled']:
            df = self.remove_exact_duplicates(df, question_column)
        
        # Stage 2: Fuzzy duplicates
        df = self.remove_fuzzy_duplicates(df, question_column)
        
        # Stage 3: Semantic duplicates
        df = self.remove_semantic_duplicates(df, question_column)
        
        # Save results
        self.save_data(df, output_file)
        
        # Export groups if enabled
        if self.config.get('output', {}).get('export_groups', False):
            if self.last_clusters is not None and self.last_representatives is not None:
                groups_dir = self.config.get('output', {}).get('groups_directory', 'Data/groups')
                
                # Create subdirectory for this file
                from pathlib import Path
                input_basename = Path(input_file).stem
                file_groups_dir = f"{groups_dir}/{input_basename}"
                
                logger.info(f"Exporting duplicate groups to {file_groups_dir}...")
                groups_saved = self.report.save_groups_to_csv(
                    output_dir=file_groups_dir,
                    df_original=self.last_df,
                    clusters=self.last_clusters,
                    representatives=self.last_representatives,
                    question_col=self.last_question_col,
                    file_prefix=input_basename
                )
                logger.info(f"Exported {groups_saved} groups")
        
        # Finalize report
        self.report.set_final_count(len(df))
        self.report.stop_timer()
        
        return df


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config file
    
    Returns:
        Configuration dictionary
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Deduplicate questions from CSV/Excel files"
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help='Path to configuration file'
    )
    parser.add_argument(
        '--input',
        type=str,
        help='Input file path (overrides config)'
    )
    parser.add_argument(
        '--output',
        type=str,
        help='Output file path (overrides config)'
    )
    parser.add_argument(
        '--column',
        type=str,
        help='Question column name (overrides config)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {args.config}")
        sys.exit(1)
    
    # Override config with command-line arguments
    if args.input:
        input_file = args.input
    else:
        # Try to get from config
        input_file = config.get('input', {}).get('csv_file') or config.get('input', {}).get('excel_file')
        if not input_file:
            logger.error("No input file specified")
            sys.exit(1)
    
    if args.output:
        output_file = args.output
    else:
        # Generate output filename
        input_path = Path(input_file)
        output_dir = config.get('output', {}).get('directory', 'Data/filtered')
        suffix = config.get('output', {}).get('suffix', '_deduplicated')
        output_file = f"{output_dir}/{input_path.stem}{suffix}{input_path.suffix}"
    
    question_column = args.column or config.get('input', {}).get('question_column', 'QueryText')
    
    logger.info("="*70)
    logger.info("QUESTION DEDUPLICATION PIPELINE")
    logger.info("="*70)
    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"Question column: {question_column}")
    logger.info(f"Strategy: {config['deduplication']['strategy']}")
    logger.info("="*70)
    
    # Run deduplication
    deduplicator = QuestionDeduplicator(config)
    
    try:
        df_result = deduplicator.deduplicate(input_file, output_file, question_column)
        
        # Print report
        deduplicator.report.print_summary()
        
        # Save report if configured
        if config.get('output', {}).get('save_report', True):
            report_file = f"{output_file}.report.txt"
            deduplicator.report.save_to_file(report_file)
        
        logger.info("Deduplication completed successfully!")
        
    except Exception as e:
        logger.error(f"Deduplication failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
