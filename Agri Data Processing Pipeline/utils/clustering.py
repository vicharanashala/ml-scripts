"""
Clustering utilities for grouping similar questions.
"""

import numpy as np
from typing import List, Dict, Set
import logging

logger = logging.getLogger(__name__)


class UnionFind:
    """
    Union-Find (Disjoint Set Union) data structure for clustering.
    """
    
    def __init__(self, n: int):
        """
        Initialize Union-Find structure.
        
        Args:
            n: Number of elements
        """
        self.parent = list(range(n))
        self.rank = [0] * n
    
    def find(self, x: int) -> int:
        """Find root of element x with path compression."""
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]
    
    def union(self, x: int, y: int):
        """Union two sets containing x and y."""
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return
        
        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1
    
    def get_clusters(self) -> Dict[int, List[int]]:
        """
        Get all clusters.
        
        Returns:
            Dictionary mapping cluster_id -> list of element indices
        """
        clusters = {}
        for i in range(len(self.parent)):
            root = self.find(i)
            if root not in clusters:
                clusters[root] = []
            clusters[root].append(i)
        return clusters


def cluster_by_similarity(similarity_matrix: np.ndarray, 
                         threshold: float = 0.85) -> Dict[int, List[int]]:
    """
    Cluster items based on similarity matrix using Union-Find.
    
    Args:
        similarity_matrix: NxN similarity matrix
        threshold: Minimum similarity to consider items in same cluster
    
    Returns:
        Dictionary mapping cluster_id -> list of item indices
    """
    n = similarity_matrix.shape[0]
    uf = UnionFind(n)
    
    # Union items that are similar
    for i in range(n):
        for j in range(i + 1, n):
            if similarity_matrix[i, j] >= threshold:
                uf.union(i, j)
    
    clusters = uf.get_clusters()
    logger.info(f"Created {len(clusters)} clusters from {n} items")
    
    return clusters


def cluster_by_pairs(n_items: int, 
                    similar_pairs: List[tuple]) -> Dict[int, List[int]]:
    """
    Cluster items based on list of similar pairs.
    
    Args:
        n_items: Total number of items
        similar_pairs: List of (index1, index2, similarity) tuples
    
    Returns:
        Dictionary mapping cluster_id -> list of item indices
    """
    uf = UnionFind(n_items)
    
    for i, j, _ in similar_pairs:
        uf.union(i, j)
    
    clusters = uf.get_clusters()
    logger.info(f"Created {len(clusters)} clusters from {n_items} items")
    
    return clusters


def get_cluster_representatives(clusters: Dict[int, List[int]], 
                               scores: np.ndarray = None,
                               strategy: str = "first") -> Dict[int, int]:
    """
    Select representative item from each cluster.
    
    Args:
        clusters: Dictionary mapping cluster_id -> list of item indices
        scores: Optional scores for each item (higher is better)
        strategy: Selection strategy
            - "first": Select first item in cluster
            - "random": Select random item
            - "best": Select item with highest score (requires scores)
    
    Returns:
        Dictionary mapping cluster_id -> representative item index
    """
    representatives = {}
    
    for cluster_id, items in clusters.items():
        if strategy == "first":
            representatives[cluster_id] = items[0]
        elif strategy == "random":
            import random
            representatives[cluster_id] = random.choice(items)
        elif strategy == "best":
            if scores is None:
                raise ValueError("scores required for 'best' strategy")
            best_idx = max(items, key=lambda i: scores[i])
            representatives[cluster_id] = best_idx
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
    
    return representatives


def get_items_to_keep(clusters: Dict[int, List[int]], 
                     representatives: Dict[int, int]) -> Set[int]:
    """
    Get set of item indices to keep (one per cluster).
    
    Args:
        clusters: Dictionary mapping cluster_id -> list of item indices
        representatives: Dictionary mapping cluster_id -> representative item index
    
    Returns:
        Set of item indices to keep
    """
    return set(representatives.values())


def get_items_to_remove(n_items: int,
                       clusters: Dict[int, List[int]], 
                       representatives: Dict[int, int]) -> Set[int]:
    """
    Get set of item indices to remove (duplicates).
    
    Args:
        n_items: Total number of items
        clusters: Dictionary mapping cluster_id -> list of item indices
        representatives: Dictionary mapping cluster_id -> representative item index
    
    Returns:
        Set of item indices to remove
    """
    keep = get_items_to_keep(clusters, representatives)
    all_items = set(range(n_items))
    return all_items - keep
