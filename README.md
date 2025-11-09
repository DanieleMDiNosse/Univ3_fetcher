# Uniswap v3 Data Downloader

This repository hosts a Streamlit app plus the underlying harvesting script used to collect Swap/Mint/Burn events' details for any Uniswap v3 pool on Ethereum mainnet. The UI lets you configure RPC endpoints, select pools and time windows, and then orchestrates the `data_fetch.py` pipeline to download, enrich, and package the resulting dataset.

## Features
- Friendly Streamlit interface for configuring RPC URLs, pool selection (presets + custom), time ranges, output destinations, and advanced tuning knobs.
- Server-side worker that resumes safely via checkpoints, splits large block ranges automatically, and enriches rows with pool state before/after each event.
- Optional gas-metadata fetching when gas columns are requested to limit unnecessary RPC calls.
- Built-in artifact organization so each run bundles logs, configuration, chunked raw data, merged pickles, and metadata into a unique timestamped folder.

## Requirements
- Python 3.10 or later.
- Recommended: virtual environment (venv, Conda, etc.).
- Install dependencies from `requirements.txt`.

### Installation

You can set up the environment using either `venv` or Conda:

<details>
<summary><strong>Using venv</strong></summary>

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```
</details>

<details>
<summary><strong>Using Conda</strong></summary>

```bash
conda create -n univ3_fetcher python=3.10
conda activate univ3_fetcher
pip install --upgrade pip
pip install -r requirements.txt
```
</details>

## Running the Streamlit App
1. Activate your environment and start Streamlit:
   ```bash
   streamlit run streamlit_app.py
   ```
2. Provide one or more HTTPS JSON-RPC URLs. Multiple endpoints improve reliability because the fetcher uses `eth-defi`'s multi-provider client.
3. Pick a pool preset or input a custom pool address. Presets automatically surface known Uniswap v3 pools and token metadata.
4. Choose the UTC start/end date and time. The app validates that the end time is after the start time.
5. Select the columns to keep in the merged dataset. Selecting any gas column enables transaction receipt lookups (slower but richer data).
6. Set a destination folder; each run creates a unique slugged subdirectory with config, logs, outputs, and metadata.
7. Adjust advanced parameters if needed:
   - **Chunk size (blocks)**: max block span per log query before auto-splitting.
   - **Parallel workers**: threads used for parsing chunks.
   - **Batch size for metadata RPC**: how many receipts/origins are fetched per batch.
8. Click **Start download**. The UI streams the harvesting logs, shows progress, and surfaces any errors.

After a successful run, the final directory (inside `downloads/` by default) contains:
- `config/data_fetch_config.yml`: exact parameters used.
- `logs/data_fetch.log`: full stdout from `data_fetch.py`.
- `outputs/chunks/<pool>/`: raw chunk pickles straight from the harvester.
- `outputs/merged/<pool>.pkl`: filtered dataset matching your column choices.
- `metadata.json`: summary metadata (pool label, time range, row counts, artifact paths).

## Using `data_fetch.py` Directly
The Streamlit app ultimately invokes `data_fetch.py` with a generated YAML config. You can run it standalone if you prefer scripting:

```bash
python data_fetch.py
```

By default it reads `data_fetch_config.yml`. Override the path via the `DATA_FETCH_CONFIG_PATH` environment variable. Key config fields include `json_rpc_urls`, `pool_addr`, `start_ts`, `end_ts`, `chunk_size_blocks`, `parallel_workers`, `batch_receipt_size`, `checkpoint_path`, `out_dir`, `skip_gas_data`, and `compute_price_column`.

## Tips
- RPC nodes may rate limit large requests. Supplying multiple URLs and modest chunk sizes improves reliability.
- Gas columns (`gasUsed`, `gasPrice`, `effectiveGasPrice`) require transaction receipts, which significantly increase runtime—only enable them when needed.
- The combined pickle saved under `data/<pool>.pkl` (if present) is reused to speed up repeated runs; clear it if you want a fresh aggregation.

Happy data fetching!
