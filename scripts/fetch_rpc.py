#!/usr/bin/env python3
"""
Entry point script for RPC-based data fetching.
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import and run the RPC harvester
# Note: This will need to be updated when rpc_harvester is refactored
import univ3_fetcher.core.harvesters.rpc_harvester as rpc_module

if __name__ == "__main__":
    # RPC harvester runs at module level, so just import it
    pass

