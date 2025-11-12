"""
data_fetch.py — Uniswap v3 pool event harvester (origin separated from gas fields)

Overview
--------
This script extracts Swap, Mint, and Burn events for a single Uniswap v3 pool
over a given time window, enriches each event with the pool's *running state*
before/after the event, and (optionally) augments rows with on-chain gas data:
`gasUsed`, `gasPrice`, and `effectiveGasPrice`. Results are persisted as pickled DataFrames.
All runtime parameters are loaded from `data_fetch_config.yml` (see sample file).

Key features
------------
- Robust, adaptive `get_logs` over large block ranges (auto-splitting on RPC limits).
- Parallel parsing of logs.
- **Separated metadata fetch**:
  - Always fetch `origin` (tx.from) from `eth_getTransaction`.
  - Optionally fetch gas fields from `eth_getTransactionReceipt` (plus legacy `gasPrice`).
- Resume-safe checkpoints (block cursor + last known pool state).
- Running state columns for liquidity and sqrt price before/after each event.

Workflow (high level)
---------------------
1) Convert the requested time window to block numbers via binary search.
2) Fetch Swap/Mint/Burn logs in adaptively sized block chunks.
3) Decode logs into event rows (base columns).
4) **Fetch origins only** for unique `transactionHash` values.
5) If `SKIP_GAS_DATA` is False, batch-fetch gas fields for those hashes.
6) Walk the event sequence in block/log order to compute pool *running state*.
7) Append rows to the pickled output and persist a checkpoint.

Configuration knobs
-------------------
- `POOL_ADDR`: Pool address (checksum).
- `START_TS` / `END_TS`: Time window (UNIX seconds or ISO8601 string).
- `CHUNK_SIZE_BLOCKS`: Max block span per log request (auto-splits further on RPC limits).
- `PARALLEL_WORKERS`: Thread fan-out for log parsing.
- `BATCH_RECEIPT_SIZE`: Batch size for metadata lookups.
- `SKIP_GAS_DATA`: If True, skip `gasUsed` / `effectiveGasPrice` / `gasPrice` (faster).

Output schema
-------------
All rows share a common schema; some fields are `None` when not applicable to the
event type. Types are Python primitives as written by pandas.

Core identifiers and timing:
- `eventType` (str): `"Mint"`, `"Burn"`, or direction-aware swap labels `"swap_x2y"` / `"swap_y2x"`.
- `blockNumber` (int): Block height containing the event.
- `logIndex` (int): Index of the log within the block (for strict ordering).
- `timestamp` (int): Block timestamp in UNIX seconds (UTC).
- `transactionHash` (str): Hex string of the tx hash (0x-prefixed).

Gas / tx metadata:
- `origin` (str|None): Transaction sender (`from` address). **Always fetched.**
- `gasUsed` (int|None): Gas units actually consumed by the tx (from receipt).
- `gasPrice` (int|None): Legacy tx gas price in wei (from tx). May be `None` for
  EIP-1559 transactions which instead have `maxFeePerGas`/`maxPriorityFeePerGas`.
- `effectiveGasPrice` (int|None): Actual wei paid per gas (from receipt), i.e.
  `min(maxFeePerGas, baseFee + maxPriorityFeePerGas)` for type-2, or `gasPrice`
  for legacy. Use `gasUsed * effectiveGasPrice / 1e18` to get ETH spent.

Event-specific payload:
- `sender` (str|None): For `Swap`: event `sender`. For `Mint`/`Burn`: `None`.
- `owner` (str|None): For `Mint`/`Burn`: position owner. Else `None`.
- `recipient` (str|None): For `Swap`: event `recipient`. Else `None`.
- `amount0` (int):
  * Swap: signed pool delta of token0. **Sign convention (Uniswap v3):**
    Positive => pool received token0; Negative => pool sent token0.
  * Mint/Burn: unsigned amount of token0 moved due to liquidity change (event field).
- `amount1` (int):
  * Swap: signed pool delta of token1 (same sign convention as above).
  * Mint/Burn: unsigned amount of token1 moved due to liquidity change.
- `sqrtPriceX96_event` (int|None): For `Swap`: the new sqrt price Q64.96 after the swap.
  For `Mint`/`Burn`: `None`.
- `tick_event` (int|None): For `Swap`: the new tick after the swap. Else `None`.
- `liquidityAfter_event` (int|None): For `Swap`: pool active liquidity AFTER the swap event.
  For `Mint`/`Burn`: `None`.
- `tickLower` (int|None): For `Mint`/`Burn`: lower tick of the position. Else `None`.
- `tickUpper` (int|None): For `Mint`/`Burn`: upper tick of the position. Else `None`.

Running-state (derived):
- `L_before`, `sqrt_before`, `tick_before`, `x_before`, `y_before`
- `L_after`,  `sqrt_after`,  `tick_after`,  `x_after`,  `y_after`
- `affectsActive` (bool|None): For Mint/Burn: whether the position spans active tick.
- `deltaL_applied` (int|None): Liquidity change applied to active L (Mint positive, Burn negative when spanning).
"""

import datetime
import json
import os
import pickle
import shutil
import tempfile
import time
import numbers
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from pathlib import Path

import pandas as pd
from eth_defi.provider.multi_provider import create_multi_provider_web3
from web3 import Web3
from web3._utils.events import get_event_data
import yaml
import numpy as np

from univ3_fetcher.core.utils import sqrtPriceX96_to_price

# ---------------- Configuration ----------------
CONFIG_PATH = os.environ.get("DATA_FETCH_CONFIG_PATH", "parameters_yml/data_fetch_config.yml")

DEFAULT_CONFIG: Dict[str, Any] = {
    "json_rpc_urls": [
        "https://eth.llamarpc.com/sk_llama_252714c1e64c9873e3b21ff94d7f1a3f",
        "https://mainnet.infura.io/v3/5f38fb376e0548c8a828112252a6a588",
    ],
    "pool_addr": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "start_ts": "2023-01-01T00:00:00Z",
    "end_ts": "2023-01-01T2:59:59Z",
    "chunk_size_blocks": 200,
    "parallel_workers": 8,
    "batch_receipt_size": 100,
    "checkpoint_path": "univ3_checkpoint.json",
    "out_dir": "data/univ3_{pool}_pkl",
    "skip_gas_data": False,
    "compute_price_column": True,
}


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        print(f"Configuration file {path!r} not found. Using defaults.")
        return DEFAULT_CONFIG.copy()
    with open(path, "r") as fh:
        loaded = yaml.safe_load(fh) or {}
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(loaded)
    if not isinstance(cfg.get("json_rpc_urls"), list) or not cfg["json_rpc_urls"]:
        raise ValueError("Config field 'json_rpc_urls' must be a non-empty list.")
    return cfg


CONFIG = load_config(CONFIG_PATH)

JSON_RPC_URLS: List[str] = [str(url).strip() for url in CONFIG["json_rpc_urls"] if str(url).strip()]
if not JSON_RPC_URLS:
    raise ValueError("No valid RPC URLs provided in configuration.")
JSON_RPC_LINE = " ".join(JSON_RPC_URLS)

POOL_ADDR = Web3.to_checksum_address(CONFIG["pool_addr"]).lower()
START_TS: Union[int, str] = CONFIG["start_ts"]
END_TS: Union[int, str] = CONFIG["end_ts"]
CHUNK_SIZE_BLOCKS = int(CONFIG["chunk_size_blocks"])
PARALLEL_WORKERS = int(CONFIG["parallel_workers"])
BATCH_RECEIPT_SIZE = int(CONFIG["batch_receipt_size"])
CHECKPOINT_PATH = CONFIG["checkpoint_path"]

raw_out_dir = str(CONFIG["out_dir"])
OUT_DIR = raw_out_dir.format(pool=POOL_ADDR)
SKIP_GAS_DATA = bool(CONFIG["skip_gas_data"])
COMPUTE_PRICE = bool(CONFIG.get("compute_price_column", True))

# Ensure base output directory ancestor exists upfront.
out_parent = os.path.abspath(os.path.join(OUT_DIR, os.pardir))
os.makedirs(out_parent, exist_ok=True)

# ---------------- Setup ----------------
w3 = create_multi_provider_web3(JSON_RPC_LINE, request_kwargs={"timeout": 60.0})
POOL = Web3.to_checksum_address(POOL_ADDR)

Q96 = 1 << 96

# ABIs required for early metadata fetches
POOL_INFO_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_DECIMALS_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    }
]


def fetch_token_decimals(pool_address: str) -> Tuple[Optional[int], Optional[int]]:
    """Fetch token0/token1 decimals for the pool; return (dec0, dec1)."""
    try:
        pool_contract = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=POOL_INFO_ABI)
        token0_addr = pool_contract.functions.token0().call()
        token1_addr = pool_contract.functions.token1().call()
        token0_contract = w3.eth.contract(address=token0_addr, abi=ERC20_DECIMALS_ABI)
        token1_contract = w3.eth.contract(address=token1_addr, abi=ERC20_DECIMALS_ABI)
        dec0 = int(token0_contract.functions.decimals().call())
        dec1 = int(token1_contract.functions.decimals().call())
        return dec0, dec1
    except Exception as exc:
        print(f"⚠️  Unable to fetch token decimals for pool {pool_address}: {exc}")
        return (None, None)


if COMPUTE_PRICE:
    TOKEN0_DECIMALS, TOKEN1_DECIMALS = fetch_token_decimals(POOL)
else:
    TOKEN0_DECIMALS, TOKEN1_DECIMALS = (None, None)


def derive_price_series(df: pd.DataFrame) -> pd.Series:
    """Compute price column from sqrtPriceX96 values using known token decimals."""
    if df.empty or TOKEN0_DECIMALS is None or TOKEN1_DECIMALS is None:
        return pd.Series(np.nan, index=df.index)
    sqrt_series = df.get("sqrtPriceX96_event")
    if sqrt_series is None:
        sqrt_series = pd.Series(np.nan, index=df.index)
    else:
        sqrt_series = pd.to_numeric(sqrt_series, errors="coerce")
    for fallback_col in ["sqrt_after", "sqrt_before"]:
        if fallback_col in df.columns:
            sqrt_series = sqrt_series.fillna(pd.to_numeric(df[fallback_col], errors="coerce"))
    prices = sqrt_series.apply(
        lambda val: sqrtPriceX96_to_price(float(val), TOKEN0_DECIMALS, TOKEN1_DECIMALS)
        if pd.notna(val) and val != 0
        else np.nan
    )
    return prices


def compute_price_value(row: Dict[str, Any]) -> Optional[float]:
    if not COMPUTE_PRICE or TOKEN0_DECIMALS is None or TOKEN1_DECIMALS is None:
        return None
    sqrt_val = row.get("sqrtPriceX96_event") or row.get("sqrt_after") or row.get("sqrt_before")
    if sqrt_val in (None, 0):
        return None
    try:
        return sqrtPriceX96_to_price(float(sqrt_val), TOKEN0_DECIMALS, TOKEN1_DECIMALS)
    except Exception:
        return None

def to_unix(ts: Union[int, str, datetime.datetime, pd.Timestamp]) -> int:
    if isinstance(ts, numbers.Integral):
        return int(ts)
    if isinstance(ts, str):
        stripped = ts.strip()
        if stripped.lstrip("-").isdigit():
            return int(stripped)
        return int(pd.to_datetime(stripped, utc=True).value // 10**9)
    return int(pd.to_datetime(ts, utc=True).value // 10**9)

START_TS = to_unix(START_TS)
END_TS   = to_unix(END_TS)
if END_TS < START_TS:
    raise ValueError("END_TS must be >= START_TS")

# ---------------- ABIs ----------------
SWAP_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True,  "internalType":"address","name":"sender","type":"address"},
        {"indexed": True,  "internalType":"address","name":"recipient","type":"address"},
        {"indexed": False, "internalType":"int256", "name":"amount0","type":"int256"},
        {"indexed": False, "internalType":"int256", "name":"amount1","type":"int256"},
        {"indexed": False, "internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},
        {"indexed": False, "internalType":"uint128","name":"liquidity","type":"uint128"},
        {"indexed": False, "internalType":"int24",  "name":"tick","type":"int24"}
    ],
    "name": "Swap", "type": "event"
}
MINT_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": False, "internalType":"address","name":"sender","type":"address"},
        {"indexed": True,  "internalType":"address","name":"owner","type":"address"},
        {"indexed": True,  "internalType":"int24",  "name":"tickLower","type":"int24"},
        {"indexed": True,  "internalType":"int24",  "name":"tickUpper","type":"int24"},
        {"indexed": False, "internalType":"uint128","name":"amount","type":"uint128"},
        {"indexed": False, "internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed": False, "internalType":"uint256","name":"amount1","type":"uint256"}
    ],
    "name": "Mint", "type": "event"
}
BURN_EVENT_ABI = {
    "anonymous": False,
    "inputs": [
        {"indexed": True,  "internalType":"address","name":"owner","type":"address"},
        {"indexed": True,  "internalType":"int24",  "name":"tickLower","type":"int24"},
        {"indexed": True,  "internalType":"int24",  "name":"tickUpper","type":"int24"},
        {"indexed": False, "internalType":"uint128","name":"amount","type":"uint128"},
        {"indexed": False, "internalType":"uint256","name":"amount0","type":"uint256"},
        {"indexed": False, "internalType":"uint256","name":"amount1","type":"uint256"}
    ],
    "name": "Burn", "type": "event"
}

POOL_INFO_ABI = [
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_DECIMALS_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    }
]
POOL_STATE_ABI = [
    {"name":"slot0","inputs":[],"outputs":[
        {"type":"uint160","name":"sqrtPriceX96"},
        {"type":"int24","name":"tick"},
        {"type":"uint16","name":"observationIndex"},
        {"type":"uint16","name":"observationCardinality"},
        {"type":"uint16","name":"observationCardinalityNext"},
        {"type":"uint8","name":"feeProtocol"},
        {"type":"bool","name":"unlocked"}],
     "stateMutability":"view","type":"function"},
    {"name":"liquidity","inputs":[],"outputs":[{"type":"uint128"}],
     "stateMutability":"view","type":"function"},
]

contract = w3.eth.contract(address=POOL, abi=[SWAP_EVENT_ABI, MINT_EVENT_ABI, BURN_EVENT_ABI])
state_c  = w3.eth.contract(address=POOL, abi=POOL_STATE_ABI)

SwapEvent = contract.events.Swap
MintEvent = contract.events.Mint
BurnEvent = contract.events.Burn

SWAP_TOPIC0 = w3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
MINT_TOPIC0 = w3.keccak(text="Mint(address,address,int24,int24,uint128,uint256,uint256)").hex()
BURN_TOPIC0 = w3.keccak(text="Burn(address,int24,int24,uint128,uint256,uint256)").hex()

# ---------------- Block helpers with caching ----------------
_block_cache: Dict[int, Any] = {}
_ts_cache: Dict[int, int] = {}

def get_block_cached(block_num: int):
    if block_num not in _block_cache:
        _block_cache[block_num] = w3.eth.get_block(block_num)
    return _block_cache[block_num]

def block_ts(bn: int) -> int:
    if bn not in _ts_cache:
        _ts_cache[bn] = get_block_cached(bn)["timestamp"]
    return _ts_cache[bn]

def block_for_timestamp(target_ts: int, mode: str = "start") -> int:
    latest = w3.eth.block_number
    lo, hi = 0, latest
    ans = None
    while lo <= hi:
        mid = (lo + hi) // 2
        ts = get_block_cached(mid)["timestamp"]
        if mode == "start":
            if ts >= target_ts:
                ans, hi = mid, mid - 1
            else:
                lo = mid + 1
        else:
            if ts <= target_ts:
                ans, lo = mid, mid + 1
            else:
                hi = mid - 1
    return ans if ans is not None else (0 if mode == "start" else 0)

START_BLOCK = block_for_timestamp(START_TS, "start")
END_BLOCK   = block_for_timestamp(END_TS,   "end")

# ---------------- Metadata caches ----------------
_rcpt_cache: Dict[str, Any] = {}
# tx hash -> {"from": str|None, "gasPrice": int|None}
_tx_cache: Dict[str, Dict[str, Optional[Union[str, int]]]] = {}


def unique_ordered_hashes(values: Sequence[str]) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for val in values:
        if val not in seen:
            seen.add(val)
            ordered.append(val)
    return ordered


def iter_chunks(values: Sequence[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = len(values) or 1
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]

# ---------------- NEW: focused metadata helpers ----------------
def batch_fetch_origins(tx_hashes: List[str]) -> Dict[str, Optional[str]]:
    """
    Fetch only the transaction sender (`from`) for a set of tx hashes.
    Uses _tx_cache to avoid duplicate RPC calls.
    """
    origin_result: Dict[str, Optional[str]] = {}

    to_fetch = [h for h in tx_hashes if h not in _tx_cache or _tx_cache[h].get("from") is None]

    def fetch_tx(txh: str):
        try:
            tx = w3.eth.get_transaction(txh)
            _tx_cache[txh] = {"from": tx["from"], "gasPrice": tx.get("gasPrice")}
            return txh, _tx_cache[txh]["from"]
        except Exception:
            # cache a miss to avoid refetch storms
            _tx_cache.setdefault(txh, {"from": None, "gasPrice": None})
            return txh, None

    if to_fetch:
        with ThreadPoolExecutor(max_workers=min(10, len(to_fetch))) as ex:
            for fut in as_completed([ex.submit(fetch_tx, h) for h in to_fetch]):
                txh, origin = fut.result()
                origin_result[txh] = origin

    # ensure every requested hash has an entry (from cache or fetch)
    for h in tx_hashes:
        origin_result.setdefault(h, _tx_cache.get(h, {}).get("from"))
    return origin_result


def batch_fetch_gas(tx_hashes: List[str]) -> Tuple[
    Dict[str, Optional[int]],  # gasUsed
    Dict[str, Optional[int]],  # effectiveGasPrice
    Dict[str, Optional[int]],  # gasPrice (legacy; None on EIP-1559)
]:
    """
    Fetch gas metadata via receipts (gasUsed, effectiveGasPrice),
    plus legacy gasPrice via tx (using/warming _tx_cache).
    """
    gas_used: Dict[str, Optional[int]] = {}
    eff_price: Dict[str, Optional[int]] = {}
    gas_price: Dict[str, Optional[int]] = {}

    to_fetch_receipts = [h for h in tx_hashes if h not in _rcpt_cache]

    def fetch_receipt(txh: str):
        try:
            rc = w3.eth.get_transaction_receipt(txh)
            _rcpt_cache[txh] = rc
            return txh, rc
        except Exception:
            return txh, None

    if to_fetch_receipts:
        with ThreadPoolExecutor(max_workers=min(10, len(to_fetch_receipts))) as ex:
            for fut in as_completed([ex.submit(fetch_receipt, h) for h in to_fetch_receipts]):
                txh, rc = fut.result()
                if rc is not None:
                    _rcpt_cache[txh] = rc

    # Fill from caches, warming tx cache for gasPrice if needed
    for h in tx_hashes:
        rc = _rcpt_cache.get(h)
        gas_used[h] = int(rc["gasUsed"]) if rc and rc.get("gasUsed") is not None else None
        eff_price[h] = int(rc["effectiveGasPrice"]) if rc and rc.get("effectiveGasPrice") is not None else None

        if h not in _tx_cache:
            try:
                tx = w3.eth.get_transaction(h)
                _tx_cache[h] = {"from": tx["from"], "gasPrice": tx.get("gasPrice")}
            except Exception:
                _tx_cache[h] = {"from": None, "gasPrice": None}

        gprice = _tx_cache[h].get("gasPrice")
        gas_price[h] = int(gprice) if gprice is not None else None

    return gas_used, eff_price, gas_price

# ---------------- OPTIMIZED: Faster getLogs with better retry ----------------
RETRYABLE = (
    "timeout", "503", "502", "500", "429", "rate limit", "too many",
    "limit exceeded", "gateway", "413", "entity too large", 
    "payload too large", "request entity too large", "content too big",
    "range is too large", "max is 1k blocks",
    "query returned more than 10000 results", "exceeds max results",
    "-32005", "-32603", "-32602",
)

def get_logs_chunked_any(topics_any: List[str], start_block: int, end_block: int):
    """Optimized log fetching with aggressive adaptive chunking"""
    SOFT_LOG_LIMIT = 5000

    def extract_suggested_range(error_msg: str):
        import re
        m = re.search(r'range (\d+)-(\d+)', error_msg)
        if m:
            return int(m.group(1)), int(m.group(2))
        return None
    
    def yield_range(a: int, b: int, retry_count: int = 0):
        if retry_count > 2:
            chunk_size = max((b - a) // 10, 50)
            for chunk_start in range(a, b + 1, chunk_size):
                chunk_end = min(chunk_start + chunk_size - 1, b)
                yield from yield_range(chunk_start, chunk_end, 0)
            return
        
        filt = {
            "fromBlock": a,
            "toBlock": b,
            "address": POOL,
            "topics": [topics_any],
        }
        try:
            logs = w3.eth.get_logs(filt)
        except Exception as e:
            msg = str(e).lower()
            if b > a:
                if "exceeds max results" in msg:
                    suggested = extract_suggested_range(str(e))
                    if suggested:
                        s, e2 = suggested
                        if s == a:
                            yield from yield_range(a, e2, 0)
                            if e2 < b:
                                yield from yield_range(e2 + 1, b, 0)
                            return
                    parts = 8
                    part_size = (b - a) // parts
                    for i in range(parts):
                        s = a + i * part_size
                        e2 = a + (i + 1) * part_size - 1 if i < parts - 1 else b
                        if s <= e2:
                            yield from yield_range(s, e2, retry_count + 1)
                    return
                elif any(s in msg for s in RETRYABLE):
                    parts = 8 if ("10000" in msg or "20000" in msg) else (4 if ("entity too large" in msg or "payload too large" in msg) else 2)
                    part_size = (b - a + 1) // parts
                    for i in range(parts):
                        s = a + i * part_size
                        e2 = a + (i + 1) * part_size - 1 if i < parts - 1 else b
                        if s <= e2:
                            yield from yield_range(s, e2, retry_count + 1)
                    return
            raise
        
        if len(logs) > SOFT_LOG_LIMIT and b > a:
            mid = (a + b) // 2
            yield from yield_range(a, mid, retry_count)
            yield from yield_range(mid + 1, b, retry_count)
            return
        
        for lg in logs:
            yield lg
    
    cur = start_block
    while cur <= end_block:
        to_blk = min(cur + CHUNK_SIZE_BLOCKS - 1, end_block)
        try:
            yield from yield_range(cur, to_blk)
        except Exception as e:
            print(f"  ⚠️  Error in range {cur}-{to_blk}: {str(e)[:100]}")
            smaller_chunk = max(CHUNK_SIZE_BLOCKS // 4, 100)
            inner_cur = cur
            while inner_cur <= to_blk:
                inner_end = min(inner_cur + smaller_chunk - 1, to_blk)
                yield from yield_range(inner_cur, inner_end)
                inner_cur = inner_end + 1
        cur = to_blk + 1

# ---------------- Checkpoint helpers ----------------
def load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        return json.load(f)

def atomic_write_json(path: str, payload: Dict[str, Any]):
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".ckpt_", dir=d, text=True)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, separators=(",", ":"), sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try: os.remove(tmp)
        except Exception: pass
        raise

def save_checkpoint(path: str, data: Dict[str, Any]): 
    atomic_write_json(path, data)

def pickle_write_chunk(
    df: pd.DataFrame,
    root_dir: str,
    first_block: int,
    last_block: int,
) -> str:
    """
    Persist a dataframe chunk as a pickled file.

    Chunk files are named after the block range they cover, making the
    output deterministic and resume-friendly.
    """
    os.makedirs(root_dir, exist_ok=True)
    chunk_name = f"blocks_{first_block}_{last_block}.pkl"
    out_path = os.path.join(root_dir, chunk_name)
    if os.path.exists(out_path):
        os.remove(out_path)
    df.to_pickle(out_path, compression=None, protocol=pickle.HIGHEST_PROTOCOL)
    return out_path

# ---------------- Virtual balances ----------------
def virt_x(L, sP): return (int(L) * Q96) // int(sP) if sP else None
def virt_y(L, sP): return (int(L) * int(sP)) // Q96 if sP else None

# ---------------- OPTIMIZED: Parallel log processing ----------------
def process_logs_parallel(logs_chunks: List[List], start_block: int, end_block: int):
    """Process multiple log chunks in parallel"""
    all_rows = []
    def process_chunk(logs):
        rows = []
        for log in logs:
            topic0 = log["topics"][0].hex()
            bn = log["blockNumber"]
            ts = block_ts(bn)
            if ts < START_TS or ts > END_TS:
                continue
            txh = log["transactionHash"].hex()
            if topic0 == SWAP_TOPIC0:
                evt = get_event_data(w3.codec, SwapEvent._get_event_abi(), log)
                a = evt["args"]
                amount0 = int(a["amount0"])
                amount1 = int(a["amount1"])
                direction = "swap_y2x" if amount0 < 0 else "swap_x2y"
                rows.append({
                    "eventType": direction,
                    "blockNumber": bn, "logIndex": log["logIndex"], "timestamp": ts,
                    "transactionHash": txh,
                    "gasUsed": None, "gasPrice": None, "effectiveGasPrice": None, "origin": None,
                    "sender": a["sender"], "owner": None, "recipient": a["recipient"],
                    "amount0": amount0, "amount1": amount1,
                    "sqrtPriceX96_event": int(a["sqrtPriceX96"]),
                    "tick_event": int(a["tick"]),
                    "liquidityAfter_event": int(a["liquidity"]),
                    "tickLower": None, "tickUpper": None,
                    "liquidityDelta": None
                })
            elif topic0 == MINT_TOPIC0:
                evt = get_event_data(w3.codec, MintEvent._get_event_abi(), log)
                a = evt["args"]
                rows.append({
                    "eventType": "Mint",
                    "blockNumber": bn, "logIndex": log["logIndex"], "timestamp": ts,
                    "transactionHash": txh,
                    "gasUsed": None, "gasPrice": None, "effectiveGasPrice": None, "origin": None,
                    "sender": a["sender"], "owner": a["owner"], "recipient": None,
                    "amount0": int(a["amount0"]), "amount1": int(a["amount1"]),
                    "sqrtPriceX96_event": None, "tick_event": None, "liquidityAfter_event": None,
                    "tickLower": int(a["tickLower"]), "tickUpper": int(a["tickUpper"]),
                    "liquidityDelta": int(a["amount"])
                })
            elif topic0 == BURN_TOPIC0:
                evt = get_event_data(w3.codec, BurnEvent._get_event_abi(), log)
                a = evt["args"]
                rows.append({
                    "eventType": "Burn",
                    "blockNumber": bn, "logIndex": log["logIndex"], "timestamp": ts,
                    "transactionHash": txh,
                    "gasUsed": None, "gasPrice": None, "effectiveGasPrice": None, "origin": None,
                    "sender": None, "owner": a["owner"], "recipient": None,
                    "amount0": int(a["amount0"]), "amount1": int(a["amount1"]),
                    "sqrtPriceX96_event": None, "tick_event": None, "liquidityAfter_event": None,
                    "tickLower": int(a["tickLower"]), "tickUpper": int(a["tickUpper"]),
                    "liquidityDelta": -int(a["amount"])
                })
        return rows
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in logs_chunks]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    return all_rows

# ---------------- OPTIMIZED: Main processing function ----------------
def process_window(from_block: int, to_block: int, 
                   cur_L: int, cur_sqrt: int, cur_tick: int) -> Dict[str, Any]:
    """Optimized window processing with minimal redundant passes."""
    print(f"  Fetching logs for blocks {from_block}-{to_block}.")
    all_logs = list(get_logs_chunked_any([SWAP_TOPIC0, MINT_TOPIC0, BURN_TOPIC0], 
                                         from_block, to_block))
    if not all_logs:
        return {
            "df_chunk": pd.DataFrame(),
            "new_cur_L": cur_L, "new_cur_sqrt": cur_sqrt, "new_cur_tick": cur_tick,
            "first_event_block": None, "last_event_block": None, "n_rows": 0,
        }

    print(f"  Processing {len(all_logs)} logs.")
    chunk_size = max(len(all_logs) // PARALLEL_WORKERS, 100)
    log_chunks = [all_logs[i:i + chunk_size] for i in range(0, len(all_logs), chunk_size)]
    rows = process_logs_parallel(log_chunks, from_block, to_block)
    if not rows:
        return {
            "df_chunk": pd.DataFrame(),
            "new_cur_L": cur_L, "new_cur_sqrt": cur_sqrt, "new_cur_tick": cur_tick,
            "first_event_block": None, "last_event_block": None, "n_rows": 0,
        }

    rows.sort(key=lambda r: (r["blockNumber"], r["logIndex"]))
    tx_list = unique_ordered_hashes([row["transactionHash"] for row in rows])

    # Origins
    print(f"  Fetching origins for {len(tx_list)} transactions...")
    origin_map: Dict[str, Optional[str]] = {}
    if tx_list:
        for batch in iter_chunks(tx_list, BATCH_RECEIPT_SIZE):
            origin_map.update(batch_fetch_origins(batch))

    # Gas fields
    if not SKIP_GAS_DATA and tx_list:
        print(f"  Fetching gas fields for {len(tx_list)} transactions...")
        gas_used_map: Dict[str, Optional[int]] = {}
        eff_price_map: Dict[str, Optional[int]] = {}
        gas_price_map: Dict[str, Optional[int]] = {}
        for batch in iter_chunks(tx_list, BATCH_RECEIPT_SIZE):
            gused, geff, gprice = batch_fetch_gas(batch)
            gas_used_map.update(gused)
            eff_price_map.update(geff)
            gas_price_map.update(gprice)
    else:
        gas_used_map = {}
        eff_price_map = {}
        gas_price_map = {}

    print("  Computing running state.")
    curL, curSP, curTk = int(cur_L), int(cur_sqrt), int(cur_tick)
    first_block = rows[0]["blockNumber"]
    last_block = rows[-1]["blockNumber"]

    for row in rows:
        txh = row["transactionHash"]
        row["origin"] = origin_map.get(txh)
        if not SKIP_GAS_DATA:
            row["gasUsed"] = gas_used_map.get(txh)
            row["effectiveGasPrice"] = eff_price_map.get(txh)
            row["gasPrice"] = gas_price_map.get(txh)
        else:
            row.setdefault("gasUsed", None)
            row.setdefault("effectiveGasPrice", None)
            row.setdefault("gasPrice", None)

        row["L_before"] = curL
        row["sqrt_before"] = curSP
        row["tick_before"] = curTk
        row["x_before"] = virt_x(curL, curSP)
        row["y_before"] = virt_y(curL, curSP)

        etype = row["eventType"]
        if etype in ("Mint", "Burn"):
            hit = (row["tickLower"] <= curTk) and (curTk < row["tickUpper"])
            dL = int(row["liquidityDelta"]) if hit else 0
            row["affectsActive"] = bool(hit)
            row["deltaL_applied"] = dL
            curL = curL + dL
        elif etype in ("swap_x2y", "swap_y2x"):
            row["affectsActive"] = None
            row["deltaL_applied"] = None
            curL = int(row["liquidityAfter_event"])
            curSP = int(row["sqrtPriceX96_event"])
            curTk = int(row["tick_event"])
        else:
            row["affectsActive"] = None
            row["deltaL_applied"] = None

        row["L_after"] = curL
        row["sqrt_after"] = curSP
        row["tick_after"] = curTk
        row["x_after"] = virt_x(curL, curSP)
        row["y_after"] = virt_y(curL, curSP)
        row["price"] = compute_price_value(row)

    df = pd.DataFrame(rows)
    return {
        "df_chunk": df,
        "new_cur_L": curL,
        "new_cur_sqrt": curSP,
        "new_cur_tick": curTk,
        "first_event_block": int(first_block),
        "last_event_block": int(last_block),
        "n_rows": len(df),
    }

# ---------------- Main execution ----------------
print(f"Starting data fetch for pool {POOL_ADDR}")
print(f"Date range: {pd.to_datetime(START_TS, unit='s')} to {pd.to_datetime(END_TS, unit='s')}")
print(f"Block range: {START_BLOCK} to {END_BLOCK} (~{END_BLOCK - START_BLOCK:,} blocks)")
print(f"Settings: CHUNK_SIZE={CHUNK_SIZE_BLOCKS}, WORKERS={PARALLEL_WORKERS}, SKIP_GAS={SKIP_GAS_DATA}")

ckpt = load_checkpoint(CHECKPOINT_PATH)
fresh = True

if ckpt:
    ok = (
        ckpt.get("pool") == POOL_ADDR
        and ckpt.get("start_ts") == START_TS
        and ckpt.get("end_ts") == END_TS
        and ckpt.get("start_block") == START_BLOCK
        and ckpt.get("end_block") == END_BLOCK
    )
    if ok:
        from_block = int(ckpt["next_from_block"])
        cur_L = int(ckpt["cur_L"])
        cur_sqrt = int(ckpt["cur_sqrt"])
        cur_tick = int(ckpt["cur_tick"])
        fresh = False
        print(f"Resuming from block {from_block} (state: L={cur_L}, sP={cur_sqrt}, tick={cur_tick})")
    else:
        print("Checkpoint params differ. Starting fresh.")
        ckpt = None

if fresh:
    from_block = START_BLOCK
    prev_block = max(from_block - 1, 0)
    s0 = state_c.functions.slot0().call(block_identifier=prev_block)
    L0 = int(state_c.functions.liquidity().call(block_identifier=prev_block))
    cur_sqrt = int(s0[0]); cur_tick = int(s0[1]); cur_L = int(L0)
    
    if os.path.isdir(OUT_DIR):
        print(f"Removing previous output directory {OUT_DIR}")
        shutil.rmtree(OUT_DIR)
    elif os.path.exists(OUT_DIR):
        print(f"Removing previous output file {OUT_DIR}")
        os.remove(OUT_DIR)
    
    ckpt = {
        "pool": POOL_ADDR,
        "start_ts": START_TS,
        "end_ts": END_TS,
        "start_block": START_BLOCK,
        "end_block": END_BLOCK,
        "next_from_block": from_block,
        "cur_L": cur_L,
        "cur_sqrt": cur_sqrt,
        "cur_tick": cur_tick,
        "events_written": 0,
        "last_event_block": None,
        "out_pickle_dir": OUT_DIR,
    }
    save_checkpoint(CHECKPOINT_PATH, ckpt)
    print(f"Initial state at block {prev_block}: L={cur_L}, sP={cur_sqrt}, tick={cur_tick}")

# Progress tracking
import datetime
start_time = time.time()
initial_block = from_block
total_blocks = END_BLOCK - START_BLOCK + 1

while from_block <= END_BLOCK:
    to_block = min(from_block + CHUNK_SIZE_BLOCKS - 1, END_BLOCK)
    
    blocks_done = from_block - initial_block
    pct_done = (blocks_done / total_blocks) * 100 if total_blocks > 0 else 0
    elapsed = time.time() - start_time
    if blocks_done > 0:
        rate = blocks_done / elapsed
        eta = (total_blocks - blocks_done) / rate if rate > 0 else 0
        eta_str = str(datetime.timedelta(seconds=int(eta)))
        print(f"\n[{pct_done:.1f}%] Processing blocks [{from_block:,}, {to_block:,}] "
              f"(Rate: {rate:.0f} blocks/s, ETA: {eta_str})")
    else:
        print(f"\nProcessing blocks [{from_block:,}, {to_block:,}].")
    
    result = process_window(from_block, to_block, cur_L, cur_sqrt, cur_tick)
    df_chunk = result["df_chunk"]
    n_rows = result["n_rows"]
    
    cur_L = result["new_cur_L"]
    cur_sqrt = result["new_cur_sqrt"]
    cur_tick = result["new_cur_tick"]
    
    if n_rows > 0:
        chunk_path = pickle_write_chunk(
            df_chunk,
            OUT_DIR,
            result["first_event_block"],
            result["last_event_block"],
        )
        ckpt.update({
            "cur_L": cur_L,
            "cur_sqrt": cur_sqrt,
            "cur_tick": cur_tick,
            "events_written": int(ckpt.get("events_written", 0)) + n_rows,
            "last_event_block": result["last_event_block"],
            "next_from_block": to_block + 1,
        })
        save_checkpoint(CHECKPOINT_PATH, ckpt)
        print(
            f"  ✓ Wrote {n_rows} rows | Last event block: {result['last_event_block']} "
            f"| Chunk: {chunk_path}"
        )
    else:
        ckpt.update({
            "cur_L": cur_L,
            "cur_sqrt": cur_sqrt,
            "cur_tick": cur_tick,
            "next_from_block": to_block + 1,
        })
        save_checkpoint(CHECKPOINT_PATH, ckpt)
        print("  ✓ No events in window")
    
    from_block = to_block + 1

# Merge chunk files into a single dataset named after the pool address
chunk_paths = sorted(
    Path(OUT_DIR).glob("blocks_*_*.pkl"),
    key=lambda p: tuple(int(part) if part.lstrip("-").isdigit() else 0 for part in p.stem.split("_")[1:3]),
)

if chunk_paths:
    print("\n🔗 Merging chunk files into consolidated pickle…")
    total_rows = 0
    combined_df: Optional[pd.DataFrame] = None
    for chunk_path in chunk_paths:
        df_chunk = pd.read_pickle(chunk_path)
        total_rows += len(df_chunk)
        if combined_df is None:
            combined_df = df_chunk
        else:
            combined_df = pd.concat([combined_df, df_chunk], ignore_index=True)
    if combined_df is None:
        combined_df = pd.DataFrame()
    if COMPUTE_PRICE and ("price" not in combined_df.columns or combined_df["price"].isna().all()):
        combined_df["price"] = derive_price_series(combined_df)
    merged_dir = Path("data")
    merged_dir.mkdir(parents=True, exist_ok=True)
    merged_path = merged_dir / f"{POOL_ADDR}.pkl"
    combined_df.to_pickle(
        merged_path,
        compression=None,
        protocol=pickle.HIGHEST_PROTOCOL,
    )
    print(f"  ✓ Combined {total_rows:,} rows → {merged_path}")
else:
    print("\n⚠️  No chunk files found to merge into a consolidated pickle.")

# Final summary
elapsed_total = time.time() - start_time
print(f"\n{'='*60}")
print(f"✅ COMPLETED!")
print(f"Time range: {pd.to_datetime(START_TS, unit='s')} to {pd.to_datetime(END_TS, unit='s')}")
print(f"Total events written: {ckpt.get('events_written', 0):,}")
print(f"Output directory: {OUT_DIR}")
print(f"Total time: {str(datetime.timedelta(seconds=int(elapsed_total)))}")
print(f"Average rate: {total_blocks/elapsed_total:.0f} blocks/second")
print(f"{'='*60}")
