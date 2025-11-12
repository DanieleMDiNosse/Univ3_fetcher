# Project Structure

This document describes the professional organization of the `univ3_fetcher` codebase.

## Directory Structure

```
Univ3_fetcher/
├── univ3_fetcher/          # Main package
│   ├── __init__.py         # Package initialization and exports
│   ├── core/               # Core functionality
│   │   ├── __init__.py
│   │   ├── utils.py        # Utility functions and pool metadata
│   │   ├── harvesters/     # Data harvesting modules
│   │   │   ├── __init__.py
│   │   │   ├── subgraph_harvester.py  # Subgraph-first harvester
│   │   │   └── rpc_harvester.py       # RPC-only harvester (legacy)
│   │   └── liquidity/      # Liquidity analysis
│   │       ├── __init__.py
│   │       └── animator.py  # Liquidity profile GIF generator
│   ├── ui/                 # User interface components
│   │   ├── __init__.py
│   │   └── streamlit_app.py  # Streamlit web interface
│   └── config/             # Configuration management
│       ├── __init__.py
│       └── default_configs/  # Default configuration templates
│           ├── data_fetch_config.yml
│           └── liquidity_config.yml
├── scripts/                # CLI entry point scripts
│   ├── fetch_subgraph.py
│   ├── fetch_rpc.py
│   └── liquidity_animator.py
├── data/                   # User data (gitignored)
├── downloads/              # Download outputs (gitignored)
├── runs/                   # Liquidity animation outputs (gitignored)
├── setup.py                # Package installation configuration
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
└── .gitignore              # Git ignore rules
```

## Package Organization

### Core Package (`univ3_fetcher.core`)

- **`utils.py`**: Shared utility functions, pool metadata, and helper functions
- **`harvesters/`**: Data fetching implementations
  - `subgraph_harvester.py`: Primary harvester using The Graph subgraph
  - `rpc_harvester.py`: Legacy RPC-only harvester
- **`liquidity/`**: Liquidity analysis and visualization
  - `animator.py`: Generates animated GIFs of liquidity profiles

### UI Package (`univ3_fetcher.ui`)

- **`streamlit_app.py`**: Complete Streamlit web interface with tabs for:
  - Data fetching configuration and execution
  - Liquidity visualization setup

### Configuration (`univ3_fetcher.config`)

- Default configuration templates for:
  - Data fetching (`data_fetch_config.yml`)
  - Liquidity visualization (`liquidity_config.yml`)

## Usage

### As a Package

```python
from univ3_fetcher.core.utils import POOL_METADATA, sqrtPriceX96_to_price
from univ3_fetcher.core.harvesters.subgraph_harvester import run_harvester
```

### CLI Entry Points

1. **Subgraph Harvester**:
   ```bash
   python scripts/fetch_subgraph.py
   # or
   python -m univ3_fetcher.core.harvesters.subgraph_harvester
   ```

2. **RPC Harvester** (legacy):
   ```bash
   python scripts/fetch_rpc.py
   ```

3. **Liquidity Animator**:
   ```bash
   python scripts/liquidity_animator.py
   ```

4. **Streamlit UI**:
   ```bash
   streamlit run univ3_fetcher/ui/streamlit_app.py
   ```

### Installation

```bash
pip install -e .
```

This installs the package in editable mode and makes CLI entry points available.

## Import Paths

All imports use absolute paths from the `univ3_fetcher` package:

- `from univ3_fetcher.core.utils import ...`
- `from univ3_fetcher.core.harvesters.subgraph_harvester import ...`
- `from univ3_fetcher.core.liquidity.animator import ...`
- `from univ3_fetcher.ui.streamlit_app import ...`

## Configuration Files

Configuration files can be placed at the repository root or specified via environment variables:

- `DATA_FETCH_CONFIG_PATH`: Path to data fetch configuration
- `LIQUIDITY_CONFIG_PATH`: Path to liquidity visualization configuration

Default templates are available in `univ3_fetcher/config/default_configs/`.

