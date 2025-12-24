"""
Similarity computation utilities.
Handles fuzzy matching, embedding generation, and similarity calculations.
"""

import numpy as np
from typing import List, Tuple, Optional
from rapidfuzz import fuzz
from sklearn.metrics.pairwise import cosine_similarity
import logging

logger = logging.getLogger(__name__)


def fuzzy_similarity(text1: str, text2: str, algorithm: str = "token_sort_ratio") -> float:
    """
    Compute fuzzy similarity between two texts.
    
    Args:
        text1: First text
        text2: Second text
        algorithm: Fuzzy matching algorithm
            - "ratio": Simple Levenshtein distance
            - "token_sort_ratio": Sort tokens before comparison (good for word reordering)
            - "token_set_ratio": Compare unique tokens (good for duplicates)
    
    Returns:
        Similarity score between 0 and 1
    """
    if algorithm == "ratio":
        score = fuzz.ratio(text1, text2)
    elif algorithm == "token_sort_ratio":
        score = fuzz.token_sort_ratio(text1, text2)
    elif algorithm == "token_set_ratio":
        score = fuzz.token_set_ratio(text1, text2)
    else:
        raise ValueError(f"Unknown algorithm: {algorithm}")
    
    # Convert from 0-100 to 0-1
    return score / 100.0


def compute_fuzzy_similarity_matrix(texts: List[str], 
                                   algorithm: str = "token_sort_ratio",
                                   threshold: float = 0.0) -> np.ndarray:
    """
    Compute pairwise fuzzy similarity matrix.
    
    Args:
        texts: List of text strings
        algorithm: Fuzzy matching algorithm
        threshold: Only compute similarities above this threshold (optimization)
    
    Returns:
        NxN similarity matrix
    """
    n = len(texts)
    similarity_matrix = np.zeros((n, n))
    
    for i in range(n):
        similarity_matrix[i, i] = 1.0
        for j in range(i + 1, n):
            sim = fuzzy_similarity(texts[i], texts[j], algorithm)
            if sim >= threshold:
                similarity_matrix[i, j] = sim
                similarity_matrix[j, i] = sim
    
    return similarity_matrix


class EmbeddingGenerator:
    """
    Generate semantic embeddings using sentence transformers.
    """
    
    def __init__(self, model_name: str = "paraphrase-multilingual-mpnet-base-v2", 
                 use_gpu: bool = False,
                 batch_size: int = 32):
        """
        Initialize embedding generator.
        
        Args:
            model_name: Name of the sentence transformer model
            use_gpu: Whether to use GPU
            batch_size: Batch size for encoding
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ImportError("sentence-transformers not installed. Run: pip install sentence-transformers")
        
        self.model_name = model_name
        self.batch_size = batch_size
        
        device = 'cuda' if use_gpu else 'cpu'
        logger.info(f"Loading model {model_name} on {device}...")
        self.model = SentenceTransformer(model_name, device=device)
        logger.info("Model loaded successfully")
    
    def encode(self, texts: List[str], show_progress: bool = True) -> np.ndarray:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings
            show_progress: Show progress bar
        
        Returns:
            NxD embedding matrix (N texts, D dimensions)
        """
        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            show_progress_bar=show_progress,
            convert_to_numpy=True
        )
        return embeddings


def compute_cosine_similarity_matrix(embeddings: np.ndarray) -> np.ndarray:
    """
    Compute pairwise cosine similarity matrix from embeddings.
    
    Args:
        embeddings: NxD embedding matrix
    
    Returns:
        NxN similarity matrix
    """
    return cosine_similarity(embeddings)


def find_similar_pairs(similarity_matrix: np.ndarray, 
                      threshold: float = 0.85) -> List[Tuple[int, int, float]]:
    """
    Find pairs of similar items from similarity matrix.
    
    Args:
        similarity_matrix: NxN similarity matrix
        threshold: Minimum similarity threshold
    
    Returns:
        List of (index1, index2, similarity) tuples
    """
    n = similarity_matrix.shape[0]
    pairs = []
    
    for i in range(n):
        for j in range(i + 1, n):
            sim = similarity_matrix[i, j]
            if sim >= threshold:
                pairs.append((i, j, sim))
    
    # Sort by similarity (descending)
    pairs.sort(key=lambda x: x[2], reverse=True)
    
    return pairs


def compute_semantic_similarity(text1: str, text2: str, model) -> float:
    """
    Compute semantic similarity between two texts using embeddings.
    
    Args:
        text1: First text
        text2: Second text
        model: EmbeddingGenerator instance
    
    Returns:
        Cosine similarity score between 0 and 1
    """
    embeddings = model.encode([text1, text2], show_progress=False)
    similarity = cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]
    return float(similarity)
