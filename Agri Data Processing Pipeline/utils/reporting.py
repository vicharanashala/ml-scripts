"""
Reporting utilities for deduplication statistics and visualization.
"""

import pandas as pd
from typing import Dict, List, Set
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DeduplicationReport:
    """
    Generate and manage deduplication reports.
    """
    
    def __init__(self):
        self.stats = {
            'original_count': 0,
            'exact_duplicates_removed': 0,
            'fuzzy_duplicates_removed': 0,
            'semantic_duplicates_removed': 0,
            'final_count': 0,
            'total_removed': 0,
            'reduction_percentage': 0.0,
            'processing_time': 0.0
        }
        self.duplicate_groups = []
        self.start_time = None
        self.end_time = None
    
    def set_original_count(self, count: int):
        """Set original item count."""
        self.stats['original_count'] = count
    
    def set_exact_duplicates(self, count: int):
        """Set number of exact duplicates removed."""
        self.stats['exact_duplicates_removed'] = count
    
    def set_fuzzy_duplicates(self, count: int):
        """Set number of fuzzy duplicates removed."""
        self.stats['fuzzy_duplicates_removed'] = count
    
    def set_semantic_duplicates(self, count: int):
        """Set number of semantic duplicates removed."""
        self.stats['semantic_duplicates_removed'] = count
    
    def set_final_count(self, count: int):
        """Set final item count."""
        self.stats['final_count'] = count
        self.stats['total_removed'] = self.stats['original_count'] - count
        if self.stats['original_count'] > 0:
            self.stats['reduction_percentage'] = (
                self.stats['total_removed'] / self.stats['original_count'] * 100
            )
    
    def start_timer(self):
        """Start processing timer."""
        self.start_time = datetime.now()
    
    def stop_timer(self):
        """Stop processing timer."""
        self.end_time = datetime.now()
        if self.start_time:
            self.stats['processing_time'] = (self.end_time - self.start_time).total_seconds()
    
    def add_duplicate_group(self, group: List[str], representative: str, similarity: float = 1.0):
        """
        Add a duplicate group to the report.
        
        Args:
            group: List of duplicate questions
            representative: The question that was kept
            similarity: Similarity score
        """
        self.duplicate_groups.append({
            'representative': representative,
            'duplicates': group,
            'count': len(group),
            'similarity': similarity
        })
    
    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "="*70)
        print("DEDUPLICATION REPORT")
        print("="*70)
        print(f"Original count:              {self.stats['original_count']:,}")
        print(f"Exact duplicates removed:    {self.stats['exact_duplicates_removed']:,}")
        print(f"Fuzzy duplicates removed:    {self.stats['fuzzy_duplicates_removed']:,}")
        print(f"Semantic duplicates removed: {self.stats['semantic_duplicates_removed']:,}")
        print("-"*70)
        print(f"Total removed:               {self.stats['total_removed']:,}")
        print(f"Final count:                 {self.stats['final_count']:,}")
        print(f"Reduction:                   {self.stats['reduction_percentage']:.2f}%")
        print(f"Processing time:             {self.stats['processing_time']:.2f}s")
        print("="*70)
    
    def save_to_file(self, filepath: str):
        """
        Save report to text file.
        
        Args:
            filepath: Path to save report
        """
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("DEDUPLICATION REPORT\n")
            f.write("="*70 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("SUMMARY STATISTICS\n")
            f.write("-"*70 + "\n")
            f.write(f"Original count:              {self.stats['original_count']:,}\n")
            f.write(f"Exact duplicates removed:    {self.stats['exact_duplicates_removed']:,}\n")
            f.write(f"Fuzzy duplicates removed:    {self.stats['fuzzy_duplicates_removed']:,}\n")
            f.write(f"Semantic duplicates removed: {self.stats['semantic_duplicates_removed']:,}\n")
            f.write(f"Total removed:               {self.stats['total_removed']:,}\n")
            f.write(f"Final count:                 {self.stats['final_count']:,}\n")
            f.write(f"Reduction:                   {self.stats['reduction_percentage']:.2f}%\n")
            f.write(f"Processing time:             {self.stats['processing_time']:.2f}s\n")
            f.write("="*70 + "\n\n")
            
            if self.duplicate_groups:
                f.write("SAMPLE DUPLICATE GROUPS (first 20)\n")
                f.write("-"*70 + "\n")
                for i, group in enumerate(self.duplicate_groups[:20], 1):
                    f.write(f"\nGroup {i} (similarity: {group['similarity']:.3f}):\n")
                    f.write(f"  KEPT: {group['representative']}\n")
                    for dup in group['duplicates'][:5]:  # Show max 5 duplicates per group
                        if dup != group['representative']:
                            f.write(f"  DUP:  {dup}\n")
                    if len(group['duplicates']) > 5:
                        f.write(f"  ... and {len(group['duplicates']) - 5} more\n")
        
        logger.info(f"Report saved to {filepath}")
    
    def save_duplicates_log(self, filepath: str, df_original: pd.DataFrame, 
                           indices_to_remove: Set[int], question_col: str):
        """
        Save detailed log of removed duplicates.
        
        Args:
            filepath: Path to save log
            df_original: Original dataframe
            indices_to_remove: Set of indices that were removed
            question_col: Name of question column
        """
        removed_df = df_original.iloc[list(indices_to_remove)].copy()
        removed_df.to_csv(filepath, index=False, encoding='utf-8')
        logger.info(f"Duplicates log saved to {filepath}")
    
    def save_groups_to_csv(self, output_dir: str, df_original: pd.DataFrame,
                          clusters: Dict[int, List[int]], 
                          representatives: Dict[int, int],
                          question_col: str,
                          file_prefix: str = "group"):
        """
        Save each duplicate group as a separate CSV file.
        
        Args:
            output_dir: Directory to save group CSV files
            df_original: Original dataframe
            clusters: Dictionary mapping cluster_id -> list of item indices
            representatives: Dictionary mapping cluster_id -> representative item index
            question_col: Name of question column
            file_prefix: Prefix for group filenames
        """
        import os
        from pathlib import Path
        
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Save each group with more than 1 item
        groups_saved = 0
        for cluster_id, items in clusters.items():
            if len(items) > 1:  # Only save groups with duplicates
                # Get representative index
                rep_idx = representatives[cluster_id]
                
                # Create dataframe for this group
                group_df = df_original.iloc[items].copy()
                
                # Add metadata columns
                group_df['is_representative'] = False
                group_df.loc[group_df.index[items.index(rep_idx)], 'is_representative'] = True
                group_df['cluster_id'] = cluster_id
                group_df['cluster_size'] = len(items)
                
                # Sort by representative first
                group_df = group_df.sort_values('is_representative', ascending=False)
                
                # Generate filename
                rep_question = str(df_original.iloc[rep_idx][question_col])[:50]
                safe_name = "".join(c if c.isalnum() or c in (' ', '_') else '_' for c in rep_question)
                safe_name = safe_name.replace(' ', '_').strip('_')
                filename = f"{file_prefix}_{cluster_id:05d}_{safe_name}.csv"
                filepath = os.path.join(output_dir, filename)
                
                # Save to CSV
                group_df.to_csv(filepath, index=False, encoding='utf-8')
                groups_saved += 1
        
        logger.info(f"Saved {groups_saved} duplicate groups to {output_dir}")
        return groups_saved


def print_sample_duplicates(groups: List[Dict], n: int = 5):
    """
    Print sample duplicate groups.
    
    Args:
        groups: List of duplicate group dictionaries
        n: Number of groups to print
    """
    print(f"\nSample duplicate groups (showing {min(n, len(groups))} of {len(groups)}):")
    print("-"*70)
    
    for i, group in enumerate(groups[:n], 1):
        print(f"\nGroup {i}:")
        print(f"  Representative: {group['representative'][:100]}...")
        print(f"  Duplicates ({len(group['duplicates'])}):")
        for dup in group['duplicates'][:3]:
            if dup != group['representative']:
                print(f"    - {dup[:100]}...")
