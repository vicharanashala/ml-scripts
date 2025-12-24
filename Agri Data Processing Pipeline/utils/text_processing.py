"""
Text processing utilities for question deduplication.
Handles normalization, cleaning, and preprocessing of text data.
"""

import re
import unicodedata
from typing import List, Optional


def normalize_text(text: str, 
                   lowercase: bool = True,
                   remove_extra_whitespace: bool = True,
                   remove_punctuation: bool = False,
                   normalize_unicode: bool = True) -> str:
    """
    Normalize text for comparison.
    
    Args:
        text: Input text to normalize
        lowercase: Convert to lowercase
        remove_extra_whitespace: Remove extra spaces, tabs, newlines
        remove_punctuation: Remove punctuation marks
        normalize_unicode: Normalize unicode characters (important for Hindi/regional text)
    
    Returns:
        Normalized text string
    """
    if not isinstance(text, str):
        return ""
    
    # Unicode normalization (NFC form - canonical composition)
    if normalize_unicode:
        text = unicodedata.normalize('NFC', text)
    
    # Convert to lowercase
    if lowercase:
        text = text.lower()
    
    # Remove extra whitespace
    if remove_extra_whitespace:
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
    
    # Remove punctuation (optional)
    if remove_punctuation:
        text = re.sub(r'[^\w\s]', '', text)
    
    return text


def normalize_batch(texts: List[str], **kwargs) -> List[str]:
    """
    Normalize a batch of texts.
    
    Args:
        texts: List of text strings
        **kwargs: Arguments to pass to normalize_text
    
    Returns:
        List of normalized text strings
    """
    return [normalize_text(text, **kwargs) for text in texts]


def clean_question(text: str) -> str:
    """
    Clean a question text specifically for agricultural queries.
    
    Args:
        text: Question text
    
    Returns:
        Cleaned question text
    """
    # Normalize
    text = normalize_text(text)
    
    # Remove common prefixes/suffixes
    prefixes = ['question:', 'query:', 'q:', 'प्रश्न:']
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
    
    # Remove question marks at the end (for comparison purposes)
    text = text.rstrip('?')
    
    return text


def extract_keywords(text: str, min_length: int = 3) -> List[str]:
    """
    Extract keywords from text.
    
    Args:
        text: Input text
        min_length: Minimum keyword length
    
    Returns:
        List of keywords
    """
    # Normalize
    text = normalize_text(text, remove_punctuation=True)
    
    # Split into words
    words = text.split()
    
    # Filter by length
    keywords = [w for w in words if len(w) >= min_length]
    
    return keywords


def is_valid_question(text: str, min_length: int = 10, max_length: int = 500) -> bool:
    """
    Check if text is a valid question.
    
    Args:
        text: Question text
        min_length: Minimum length
        max_length: Maximum length
    
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(text, str):
        return False
    
    text = text.strip()
    
    if len(text) < min_length or len(text) > max_length:
        return False
    
    # Check if it's not just whitespace or special characters
    if not re.search(r'\w', text):
        return False
    
    return True
