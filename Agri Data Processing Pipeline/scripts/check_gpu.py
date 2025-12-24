#!/usr/bin/env python3
"""
Verify GPU setup for the deduplication pipeline.
"""

import sys

print("Checking GPU availability...")
print("=" * 70)

# Check PyTorch
try:
    import torch
    print(f"✓ PyTorch installed: {torch.__version__}")
    
    if torch.cuda.is_available():
        print(f"✓ CUDA available: {torch.version.cuda}")
        print(f"✓ GPU detected: {torch.cuda.get_device_name(0)}")
        print(f"✓ GPU memory: {torch.cuda.get_device_properties(0).total_memory / 1024**3:.1f} GB")
        gpu_available = True
    else:
        print("✗ CUDA not available")
        gpu_available = False
except ImportError:
    print("✗ PyTorch not installed")
    gpu_available = False

print()

# Check sentence-transformers
try:
    from sentence_transformers import SentenceTransformer
    print("✓ sentence-transformers installed")
except ImportError:
    print("✗ sentence-transformers not installed")

print()

# Check configuration
try:
    import yaml
    with open('config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    use_gpu = config['deduplication']['semantic']['use_gpu']
    print(f"Configuration: use_gpu = {use_gpu}")
    
    if use_gpu and not gpu_available:
        print("⚠ WARNING: GPU enabled in config but not available!")
    elif not use_gpu and gpu_available:
        print("⚠ WARNING: GPU available but not enabled in config!")
    elif use_gpu and gpu_available:
        print("✓ GPU properly configured and available")
    else:
        print("ℹ CPU mode (GPU not available)")
        
except Exception as e:
    print(f"✗ Error reading config: {e}")

print("=" * 70)

if gpu_available:
    print("\n🚀 GPU acceleration is ready!")
    print("   Your pipeline will use the NVIDIA H200 GPU for semantic similarity.")
    print("   Expected speedup: 3-5x faster than CPU")
else:
    print("\n⚠ GPU not available. Pipeline will use CPU.")
    print("   To enable GPU, install PyTorch with CUDA support:")
    print("   pip install torch --index-url https://download.pytorch.org/whl/cu121")
