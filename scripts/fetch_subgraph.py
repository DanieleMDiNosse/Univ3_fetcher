#!/usr/bin/env python3
"""
Entry point script for subgraph-based data fetching.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from univ3_fetcher.core.harvesters.subgraph_harvester import main

if __name__ == "__main__":
    main()

