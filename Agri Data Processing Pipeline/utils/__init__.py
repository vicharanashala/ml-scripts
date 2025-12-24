"""
Utility modules for question deduplication pipeline.
"""

from .text_processing import (
    normalize_text,
    normalize_batch,
    clean_question,
    extract_keywords,
    is_valid_question
)

from .similarity import (
    fuzzy_similarity,
    compute_fuzzy_similarity_matrix,
    EmbeddingGenerator,
    compute_cosine_similarity_matrix,
    find_similar_pairs,
    compute_semantic_similarity
)

from .clustering import (
    cluster_by_similarity,
    cluster_by_pairs,
    get_cluster_representatives,
    get_items_to_keep,
    get_items_to_remove
)

from .reporting import (
    DeduplicationReport,
    print_sample_duplicates
)

__all__ = [
    # Text processing
    'normalize_text',
    'normalize_batch',
    'clean_question',
    'extract_keywords',
    'is_valid_question',
    
    # Similarity
    'fuzzy_similarity',
    'compute_fuzzy_similarity_matrix',
    'EmbeddingGenerator',
    'compute_cosine_similarity_matrix',
    'find_similar_pairs',
    'compute_semantic_similarity',
    
    # Clustering
    'cluster_by_similarity',
    'cluster_by_pairs',
    'get_cluster_representatives',
    'get_items_to_keep',
    'get_items_to_remove',
    
    # Reporting
    'DeduplicationReport',
    'print_sample_duplicates',
]
