#!/usr/bin/env python3
"""
Generate an animated GIF of Uniswap v3 liquidity profile (price-space on X) with
progress bars and resumable checkpoints.

Fixes in this patch
-------------------
- Robust parsing of liquidityNet as Python big-int (no string concatenation in cumsum).
- Safe big-int cumsum; float conversion is clipped for plotting.
- Keeps previous: progress bars, checkpoints/resume, RPC-based block resolution, GraphQL retries.

Requirements
------------
pip install requests pandas numpy matplotlib pillow tqdm

Env vars (recommended)
----------------------
export SUBGRAPH="https://gateway.thegraph.com/api/<KEY>/subgraphs/id/<UNISWAP_V3_SUBGRAPH_ID>"
export ETH_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/<YOUR_KEY>"  # or Infura/your node
export POOL_ID="0x..."  # optional, overrides default pool below
export OUT_DIR="runs"   # optional base dir for checkpoints/runs
"""

from __future__ import annotations

import os
import re
import json
import time
import pathlib
from typing import List, Dict, Optional, Tuple, Any

import requests
import threading
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from matplotlib.animation import FuncAnimation, PillowWriter
import yaml
from requests.adapters import HTTPAdapter

from utils import POOL_METADATA, find_pair_name, token_decimals_for_pair


# ============== CONFIG ==============

LIQUIDITY_CONFIG_PATH = os.environ.get("LIQUIDITY_CONFIG_PATH", "liquidity_config.yml")

def _coerce_timestamp(value: Any) -> int:
    """Convert ISO string / int-like inputs into unix seconds."""
    if value is None:
        raise ValueError("Timestamp value cannot be None.")
    if isinstance(value, (int, float, np.integer, np.floating)):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.lstrip("-").isdigit():
            return int(s)
        return int(pd.to_datetime(s, utc=True).timestamp())
    return int(pd.to_datetime(value, utc=True).timestamp())

def load_run_config() -> Dict[str, Any]:
    """
    Load configuration from YAML. No defaults are applied — the file must exist
    and contain all required keys.
    """
    cfg_path = pathlib.Path(LIQUIDITY_CONFIG_PATH)
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {cfg_path}. Set LIQUIDITY_CONFIG_PATH or create the file."
        )

    with open(cfg_path, "r") as fh:
        loaded = yaml.safe_load(fh)
    if not isinstance(loaded, dict) or not loaded:
        raise ValueError("Configuration file is empty or invalid YAML mapping.")

    # Determine which keys are required conditionally
    base_required = {
        "pool_id", "use_subgraph",
        "start_ts", "end_ts", "step_seconds",
        "xlog", "price_min", "price_max",
        "max_ticks_per_snapshot", "anim_fps", "out_gif", "title_prefix",
        "graphql_delay_sec", "out_dir",
        "avg_block_time_sec", "rpc_retries", "rpc_backoff_base",
        # Speed knobs
        "concurrent_blocks", "tick_pagination_page", "use_cursor_pagination",
        "tick_window", "save_liquidity_gross",
    }
    if "use_subgraph" not in loaded:
        raise KeyError("Missing required config key: use_subgraph")
    use_subgraph_flag = bool(loaded["use_subgraph"])
    mode_required = {"subgraph", "eth_rpc_url"} if use_subgraph_flag else {"csv_ticks", "csv_pools"}
    required_keys = base_required | mode_required

    missing = sorted(k for k in required_keys if k not in loaded)
    if missing:
        raise KeyError(f"Missing required config keys: {', '.join(missing)}")

    cfg = dict(loaded)

    # Normalize/validate values
    # pool_id can be parsed unexpectedly; normalize to lowercase hex string
    pool_id = cfg["pool_id"]
    if isinstance(pool_id, int):
        cfg["pool_id"] = "0x" + format(pool_id, "040x")
    elif isinstance(pool_id, str):
        s = pool_id.strip().lower()
        if s.startswith("0x"):
            s = s[2:]
        if len(s) != 40 or any(c not in "0123456789abcdef" for c in s):
            raise ValueError("Invalid pool_id format; expected 0x-prefixed 20-byte hex string.")
        cfg["pool_id"] = "0x" + s

    cfg["start_ts"] = _coerce_timestamp(cfg["start_ts"])
    cfg["end_ts"] = _coerce_timestamp(cfg["end_ts"])
    if cfg["end_ts"] <= cfg["start_ts"]:
        raise ValueError("end_ts must be greater than start_ts")

    cfg["step_seconds"] = int(cfg["step_seconds"])
    cfg["use_subgraph"] = bool(cfg["use_subgraph"])
    cfg["max_ticks_per_snapshot"] = int(cfg["max_ticks_per_snapshot"])
    cfg["anim_fps"] = int(cfg["anim_fps"])
    cfg["graphql_delay_sec"] = float(cfg["graphql_delay_sec"])
    cfg["avg_block_time_sec"] = float(cfg["avg_block_time_sec"])
    cfg["rpc_retries"] = int(cfg["rpc_retries"])
    cfg["rpc_backoff_base"] = float(cfg["rpc_backoff_base"])
    for price_key in ("price_min", "price_max"):
        if cfg[price_key] is not None:
            cfg[price_key] = float(cfg[price_key])
    cfg["concurrent_blocks"] = int(cfg["concurrent_blocks"])
    cfg["tick_pagination_page"] = int(cfg["tick_pagination_page"])
    cfg["use_cursor_pagination"] = bool(cfg["use_cursor_pagination"])
    if cfg["tick_window"] is not None:
        cfg["tick_window"] = int(cfg["tick_window"])
    cfg["save_liquidity_gross"] = bool(cfg["save_liquidity_gross"])

    # Normalize CSV paths if in CSV mode: resolve relative to config file directory
    if not cfg["use_subgraph"]:
        cfg_dir = cfg_path.parent.resolve()
        for key in ("csv_ticks", "csv_pools"):
            p = pathlib.Path(str(cfg[key]))
            if not p.is_absolute():
                p = (cfg_dir / p).resolve()
            cfg[key] = str(p)
            if not p.exists():
                raise FileNotFoundError(f"CSV path not found for {key}: {p}")
    return cfg

RUN_CONFIG = load_run_config()

# Uniswap v3 pool address (lowercase, no checksum). Default: USDC/WETH 5 bps (0x88e6…)
POOL_ID = str(RUN_CONFIG["pool_id"]).strip().lower()

# Choose data source:
USE_SUBGRAPH = bool(RUN_CONFIG["use_subgraph"])

# GraphQL endpoint for Uniswap v3 subgraph
SUBGRAPH = RUN_CONFIG["subgraph"]

# Ethereum mainnet RPC for block-by-timestamp lookups (REQUIRED for USE_SUBGRAPH=True)
ETH_RPC_URL = RUN_CONFIG["eth_rpc_url"]

# Sampling strategy (UTC)
START_TIMESTAMP = RUN_CONFIG["start_ts"]   # inclusive
END_TIMESTAMP   = RUN_CONFIG["end_ts"]     # exclusive
STEP_SECONDS    = RUN_CONFIG["step_seconds"]  # sample cadence, seconds

# If USE_SUBGRAPH = False, point to CSVs you prepared:
# - ticks file (long form): columns ['t','block','tickIdx','liquidityNet']
# - pool state file: columns ['t','block','tick','liquidity']
CSV_TICKS = RUN_CONFIG["csv_ticks"]
CSV_POOLS = RUN_CONFIG["csv_pools"]

# Plot controls
XLOG = bool(RUN_CONFIG["xlog"])
PRICE_MIN = RUN_CONFIG["price_min"]
PRICE_MAX = RUN_CONFIG["price_max"]
MAX_TICKS_PER_SNAPSHOT = RUN_CONFIG["max_ticks_per_snapshot"]
ANIM_FPS = RUN_CONFIG["anim_fps"]
OUT_GIF = RUN_CONFIG["out_gif"]
TITLE_PREFIX = RUN_CONFIG["title_prefix"]

# polite delay between GraphQL calls (helps avoid rate limits)
GRAPHQL_DELAY_SEC = RUN_CONFIG["graphql_delay_sec"]

# checkpointing
OUT_DIR = pathlib.Path(str(RUN_CONFIG["out_dir"])).expanduser()
# Stable run directory per pool to allow reusing previously fetched snapshots
# even when the time window changes in the config.
RUN_NAME = f"pool_{POOL_ID[:6]}"
RUN_DIR = OUT_DIR / RUN_NAME
TICKS_DIR = RUN_DIR / "ticks"
MANIFEST_PATH = RUN_DIR / "manifest.csv"
META_PATH = RUN_DIR / "meta.json"

# Block resolution tuning
AVG_BLOCK_TIME_SEC = RUN_CONFIG["avg_block_time_sec"]  # heuristic
RPC_RETRIES = RUN_CONFIG["rpc_retries"]
RPC_BACKOFF_BASE = RUN_CONFIG["rpc_backoff_base"]  # seconds

# Speed knobs (derived)
CONCURRENT_BLOCKS = int(RUN_CONFIG.get("concurrent_blocks", 6))
TICK_PAGE_SIZE = int(RUN_CONFIG.get("tick_pagination_page", 1000))
USE_CURSOR_PAGINATION = bool(RUN_CONFIG.get("use_cursor_pagination", True))
TICK_WINDOW = RUN_CONFIG.get("tick_window")
SAVE_LIQUIDITY_GROSS = bool(RUN_CONFIG.get("save_liquidity_gross", False))

# ============== HELPERS ==============

def _ensure_dirs() -> None:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    TICKS_DIR.mkdir(parents=True, exist_ok=True)

_SESSION_LOCAL = threading.local()

def _get_session() -> requests.Session:
    s = getattr(_SESSION_LOCAL, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _SESSION_LOCAL.session = s
    return s

def _graphql(endpoint: str, query: str, variables: Optional[dict] = None,
             retries: int = 4, backoff: float = 0.75, timeout: float = 45.0) -> dict:
    """
    Minimal GraphQL client with retries/backoff on HTTP errors and Graph errors.
    """
    payload = {"query": query, "variables": variables or {}}
    for attempt in range(retries):
        try:
            r = _get_session().post(endpoint, json=payload, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                if attempt < retries - 1:
                    time.sleep(backoff * (2 ** attempt))
                    continue
                raise RuntimeError(f"GraphQL error: {data['errors']}")
            return data["data"]
        except requests.RequestException as e:
            if attempt < retries - 1:
                time.sleep(backoff * (2 ** attempt))
                continue
            raise e
        finally:
            if GRAPHQL_DELAY_SEC:
                time.sleep(GRAPHQL_DELAY_SEC)

# -------- JSON-RPC with retries/backoff --------

def _rpc(method: str, params: list, timeout: float = 45.0) -> Any:
    if not ETH_RPC_URL:
        raise RuntimeError("ETH_RPC_URL is not set. Export an Ethereum mainnet RPC endpoint.")
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    last_exc = None
    for attempt in range(RPC_RETRIES):
        try:
            r = requests.post(ETH_RPC_URL, json=body, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            if "error" in j:
                raise RuntimeError(f"RPC error: {j['error']}")
            return j["result"]
        except Exception as e:
            last_exc = e
            time.sleep(RPC_BACKOFF_BASE * (2 ** attempt))
    raise RuntimeError(f"RPC failed after {RPC_RETRIES} attempts: {last_exc}")

def _get_block_by_number(n: int) -> Dict[str, Any]:
    res = _rpc("eth_getBlockByNumber", [hex(n), False])
    if not res:
        raise RuntimeError(f"Block {n} not found")
    return res

def _get_latest_block_number_and_ts() -> Tuple[int, int]:
    n_hex = _rpc("eth_blockNumber", [])
    n = int(n_hex, 16)
    blk = _get_block_by_number(n)
    return n, int(blk["timestamp"], 16)

def _get_genesis_timestamp() -> int:
    b0 = _rpc("eth_getBlockByNumber", ["0x0", False])
    return int(b0["timestamp"], 16)

def _first_block_binary(ts: int, lo: int, hi: int) -> int:
    """First block with timestamp >= ts within [lo, hi] inclusive/exclusive."""
    while lo < hi:
        mid = (lo + hi) // 2
        t_mid = int(_get_block_by_number(mid)["timestamp"], 16)
        if t_mid >= ts:
            hi = mid
        else:
            lo = mid + 1
    return lo

def fetch_block_by_timestamp(ts: int, hint_block: Optional[int] = None,
                             latest_info: Optional[Tuple[int,int]] = None) -> int:
    """
    Return the first block number with block.timestamp >= ts.
    """
    latest_num, latest_ts = latest_info if latest_info else _get_latest_block_number_and_ts()

    if ts <= _get_genesis_timestamp():
        return 0
    if ts > latest_ts:
        return latest_num

    # Initial guess
    if hint_block is None:
        delta_s = max(0, latest_ts - ts)
        guess = int(max(0, latest_num - round(delta_s / max(1.0, AVG_BLOCK_TIME_SEC))))
    else:
        guess = hint_block

    guess = max(0, min(guess, latest_num))
    t_guess = int(_get_block_by_number(guess)["timestamp"], 16)

    if t_guess >= ts:
        return _first_block_binary(ts, 0, guess)
    else:
        # Jump forward with doubling until we bracket
        step = max(1, int((ts - t_guess) / max(1.0, AVG_BLOCK_TIME_SEC)))
        lo = guess
        hi = min(latest_num, guess + step)
        t_hi = int(_get_block_by_number(hi)["timestamp"], 16)
        while t_hi < ts and hi < latest_num:
            lo = hi
            step = min(latest_num - hi, max(1, step * 2))
            hi = hi + step
            t_hi = int(_get_block_by_number(hi)["timestamp"], 16)
        return _first_block_binary(ts, lo, hi)

# -------- Uniswap v3 subgraph queries --------

def fetch_pool_state_at_block(pool_id: str, block_number: Optional[int] = None) -> Optional[dict]:
    """
    Fetch pool state at a specific block: tick, liquidity, sqrtPrice, token meta.
    If block_number is None, fetches from the latest block.
    Returns None if the pool doesn't exist at that block.
    """
    if block_number is not None:
        q = """
        query($id: ID!, $block: Block_height) {
          pool(id: $id, block: $block) {
            id
            tick
            liquidity
            sqrtPrice
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        vars_ = {"id": pool_id, "block": {"number": block_number}}
    else:
        q = """
        query($id: ID!) {
          pool(id: $id) {
            id
            tick
            liquidity
            sqrtPrice
            token0 { id symbol decimals }
            token1 { id symbol decimals }
          }
        }
        """
        vars_ = {"id": pool_id}
    data = _graphql(SUBGRAPH, q, vars_)
    return data.get("pool")

def fetch_ticks_at_block(pool_id: str, block_number: int,
                         center_tick: Optional[int] = None,
                         window: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch initialized ticks at a specific block using cursor pagination on tickIdx.
    Optionally restrict to a window [center_tick - window, center_tick + window].
    Returns a sorted DataFrame with big-int friendly dtypes.
    """
    rows: list[dict[str, int]] = []

    after: Optional[int] = None
    lower: Optional[int] = None
    upper: Optional[int] = None
    if center_tick is not None and window is not None:
        lower = int(center_tick) - int(window)
        upper = int(center_tick) + int(window)

    while True:
        # Build the where clause dynamically
        where_parts = ["pool: $pool", "liquidityGross_gt: \"0\""]
        if after is not None:
            where_parts.append("tickIdx_gt: $after")
        if lower is not None:
            where_parts.append("tickIdx_gte: $lower")
        if upper is not None:
            where_parts.append("tickIdx_lte: $upper")
        where_expr = ", ".join(where_parts)

        fields = "tickIdx liquidityNet" + (" liquidityGross" if SAVE_LIQUIDITY_GROSS else "")

        q = f"""
        query($pool: String!, $block: Block_height, $n: Int!, $after: BigInt, $lower: BigInt, $upper: BigInt) {{
          ticks(first: $n, orderBy: tickIdx, orderDirection: asc,
                where: {{ {where_expr} }}, block: $block) {{
            {fields}
          }}
        }}
        """
        vars_ = {
            "pool": pool_id,
            "block": {"number": block_number},
            "n": TICK_PAGE_SIZE,
            "after": after,
            "lower": lower,
            "upper": upper,
        }
        data = _graphql(SUBGRAPH, q, vars_)
        chunk = data.get("ticks", [])
        if not chunk:
            break
        for t in chunk:
            item = {
                "tickIdx": int(t["tickIdx"]),
                "liquidityNet": int(t["liquidityNet"]),
            }
            if SAVE_LIQUIDITY_GROSS and "liquidityGross" in t:
                item["liquidityGross"] = int(t["liquidityGross"])
            rows.append(item)

        after = int(chunk[-1]["tickIdx"])
        if len(chunk) < TICK_PAGE_SIZE or (MAX_TICKS_PER_SNAPSHOT and len(rows) > MAX_TICKS_PER_SNAPSHOT):
            break

    if not rows:
        cols = ["tickIdx", "liquidityNet"]
        if SAVE_LIQUIDITY_GROSS:
            cols.append("liquidityGross")
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(rows).sort_values("tickIdx").reset_index(drop=True)
    df["liquidityNet"] = df["liquidityNet"].astype(object)
    if SAVE_LIQUIDITY_GROSS and "liquidityGross" in df.columns:
        df["liquidityGross"] = df["liquidityGross"].astype(object)
    return df

# -------- Parsing / big-int utilities --------

_INT_RE = re.compile(r"^[\+\-]?\d+$")

def to_bigint(val: Any) -> int:
    """
    Convert various representations to Python int (big-int).
    Accepts int/np.int, strings with optional sign and commas/spaces.
    """
    if isinstance(val, (int, np.integer)):
        return int(val)
    if pd.isna(val):
        return 0
    s = str(val).strip()
    if s == "":
        return 0
    s = s.replace(",", "")  # remove thousand separators if any
    # sometimes CSV/object columns carry stray plus signs or whitespace
    if not _INT_RE.match(s):
        # last resort: strip all spaces and re-check
        s2 = s.replace(" ", "")
        if not _INT_RE.match(s2):
            raise ValueError(f"Non-integer liquidity value: {val!r}")
        s = s2
    return int(s)

def bigint_cumsum(arr_obj: np.ndarray) -> np.ndarray:
    """Cumulative sum over Python big-ints (dtype=object)."""
    out = np.empty(len(arr_obj), dtype=object)
    total = 0
    for i, v in enumerate(arr_obj):
        total += to_bigint(v)
        out[i] = total
    return out

def bigint_to_float_clipped(arr_obj: np.ndarray, clip: float = 1e308) -> np.ndarray:
    """Convert big-int array to float64 with clipping to avoid OverflowError."""
    out = np.empty(len(arr_obj), dtype=np.float64)
    for i, v in enumerate(arr_obj):
        x = to_bigint(v)
        if x > clip:
            out[i] = clip
        elif x < -clip:
            out[i] = -clip
        else:
            out[i] = float(x)
    return out

# -------- Liquidity profile construction (big-int safe) --------

def tick_to_price(tick: int, decimals0: int, decimals1: int, invert: bool = False) -> float:
    """Convert a Uniswap v3 tick to token1/token0 price using pool decimals."""
    dec_scale = 10 ** (decimals0 - decimals1)
    price = (1.0001 ** float(tick)) * dec_scale
    if invert:
        if price == 0:
            return float("inf")
        return 1.0 / price
    return price

def ticks_to_profile(df_ticks: pd.DataFrame,
                     pool_tick: int,
                     pool_liquidity: int,
                     decimals0: int,
                     decimals1: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Build price edges and active liquidity (L) per interval.
    Returns (price_edges[], L_values[]).
    Uses Python big-ints for accumulation; converts to float (clipped) for plotting.
    """
    if df_ticks.empty:
        return np.array([1.0, 1.0]), np.array([0.0])  # trivial

    # ticks fit int64
    ticks = df_ticks["tickIdx"].to_numpy(dtype=np.int64)

    # Parse liquidityNet -> big-int object array (ensures no strings)
    nets_series = df_ticks["liquidityNet"].apply(to_bigint)
    nets = nets_series.to_numpy(dtype=object)

    # Running liquidity as Python big-ints
    running = bigint_cumsum(nets)

    # INTERVALS: between tick i and i+1, active liquidity equals running[i]
    tick_edges = np.concatenate([ticks, [ticks[-1] + 1]])  # right-open each interval

    # Sanity-check vs pool_liquidity at current tick using big-ints
    idx = np.searchsorted(ticks, pool_tick, side="right") - 1
    est_liq = running[idx] if idx >= 0 else 0
    try:
        pool_liq_int = to_bigint(pool_liquidity)
    except Exception:
        pool_liq_int = int(pool_liquidity)
    if abs(int(est_liq)) - abs(int(pool_liq_int)) > max(1, int(abs(int(pool_liq_int)) * 0.001)):
        print(f"[warn] Liquidity mismatch at current tick. estimated={est_liq} pool={pool_liquidity}")

    # Map tick edges to price edges: price(token1 per token0) = 1.0001^tick * 10^(dec0-dec1)
    dec_scale = 10 ** (decimals0 - decimals1)
    price_edges = (1.0001 ** tick_edges.astype(np.float64)) * dec_scale

    # Convert big-int running liquidity to float for plotting (clipped)
    L_values = bigint_to_float_clipped(running)

    # Keep only positive prices
    mask = np.isfinite(price_edges) & (price_edges > 0)
    price_edges = price_edges[mask]
    L_values = L_values[:len(price_edges)-1]  # one less than edges
    return price_edges, L_values

def frame_plot(ax, price_edges, L_values, title, xlog=True, pmin=None, pmax=None):
    ax.clear()
    ax.stairs(L_values, price_edges)  # len(values) = len(edges) - 1
    ax.set_title(title)
    ax.set_xlabel("Price (token1 per token0)")
    ax.set_ylabel("Active Liquidity (L)")
    if xlog:
        ax.set_xscale("log")
    if pmin is not None and pmax is not None and pmax > pmin:
        ax.set_xlim(pmin, pmax)
    ax.grid(True, which="both", alpha=0.3)

# -------- Checkpointing helpers --------

def ticks_path(block: int) -> pathlib.Path:
    return TICKS_DIR / f"ticks_{block}.csv"

def save_manifest(df: pd.DataFrame) -> None:
    df.to_csv(MANIFEST_PATH, index=False)

def save_meta_once(meta: Dict[str, Any]) -> None:
    if not META_PATH.exists():
        with open(META_PATH, "w") as f:
            json.dump(meta, f, indent=2)

def load_meta() -> Optional[Dict[str, Any]]:
    if META_PATH.exists():
        with open(META_PATH, "r") as f:
            return json.load(f)
    return None

# -------- Block resolution with progress + checkpoints --------

def _initial_manifest(ts_list: List[int]) -> pd.DataFrame:
    df = pd.DataFrame({"t": ts_list})
    df.insert(0, "idx", np.arange(len(df), dtype=int))
    df["block"] = np.nan
    df["status"] = "pending"
    df["pool_tick"] = np.nan
    df["pool_liquidity"] = np.nan
    df["error"] = ""
    return df

def resolve_blocks_incremental(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill 'block' for rows where it's NaN, with a progress bar and on-disk checkpoints.
    Uses previous resolved block as a hint to accelerate subsequent lookups.
    """
    latest_num, latest_ts = _get_latest_block_number_and_ts()

    df.sort_values("t", inplace=True)
    hint_block: Optional[int] = None

    mask_missing = df["block"].isna()
    to_resolve_idx = df.index[mask_missing].tolist()

    pbar = tqdm(total=len(to_resolve_idx), desc="Resolving target blocks")
    for ridx in to_resolve_idx:
        ts = int(df.at[ridx, "t"])
        try:
            b = fetch_block_by_timestamp(ts, hint_block=hint_block, latest_info=(latest_num, latest_ts))
            df.at[ridx, "block"] = int(b)
            hint_block = int(b)
            df.at[ridx, "status"] = "pending"
            df.at[ridx, "error"] = ""
        except Exception as e:
            df.at[ridx, "error"] = f"block_resolve: {str(e)[:400]}"
        finally:
            save_manifest(df)
            pbar.update(1)
    pbar.close()
    return df

# ============== MAIN FLOW (fetching & GIF) ==============

def build_or_load_manifest() -> pd.DataFrame:
    """
    Create or load the manifest (targets + status). On first run, resolves target
    blocks with a progress bar and writes manifest incrementally.
    """
    _ensure_dirs()
    # Desired timestamps for this run
    if END_TIMESTAMP <= START_TIMESTAMP:
        raise ValueError("END_TIMESTAMP must be greater than START_TIMESTAMP")
    ts_list = list(range(int(START_TIMESTAMP), int(END_TIMESTAMP), int(STEP_SECONDS)))
    if not ts_list:
        raise ValueError("No timestamps generated. Check START/END/STEP.")

    if MANIFEST_PATH.exists():
        # Load previous work and merge with current target window; reuse completed rows
        df_prev = pd.read_csv(MANIFEST_PATH)
        for col, default in [
            ("block", np.nan),
            ("status", "pending"),
            ("pool_tick", np.nan),
            ("pool_liquidity", np.nan),
            ("error", ""),
        ]:
            if col not in df_prev.columns:
                df_prev[col] = default

        wanted = set(ts_list)
        have = set(int(x) for x in df_prev["t"].astype(int).tolist())
        keep_mask = df_prev["t"].astype(int).isin(ts_list)
        df_keep = df_prev.loc[keep_mask].copy()

        missing_ts = sorted(wanted - have)
        if missing_ts:
            df_new = _initial_manifest(missing_ts)
            df = pd.concat([df_keep, df_new], ignore_index=True)
        else:
            df = df_keep

        # Sort and reset idx monotonically
        df = df.sort_values("t").reset_index(drop=True)
        df.insert(0, "idx", np.arange(len(df), dtype=int))
    else:
        print(f"Sampling blocks every {STEP_SECONDS}s from {START_TIMESTAMP} to {END_TIMESTAMP} …")
        df = _initial_manifest(ts_list)

    save_manifest(df)
    # Resolve missing blocks only (idempotent)
    df = resolve_blocks_incremental(df)
    return df

def fetch_and_checkpoint(pool_id: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Fetch missing frames as per manifest, writing per-block tick CSVs and pool info.
    Respects prior progress and resumes. Returns the updated manifest and meta dict.
    """
    if not ETH_RPC_URL:
        raise RuntimeError("ETH_RPC_URL is required when USE_SUBGRAPH=True. Please export it.")

    df = build_or_load_manifest()
    meta: Optional[Dict[str, Any]] = load_meta()

    df_ok = df[~df["block"].isna()].copy()
    if df_ok.empty:
        raise RuntimeError("No resolvable blocks found in manifest (block column is NaN).")

    todo_mask = (df_ok["status"] != "done")
    todo_idx = df_ok.index[todo_mask].tolist()
    pbar = tqdm(total=len(todo_idx), desc="Fetching snapshots")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_snapshot(ridx: int):
        row = df_ok.loc[ridx]
        blk = int(row["block"])
        tpath = ticks_path(blk)
        if row["status"] == "done" and tpath.exists():
            return {"ridx": ridx, "skipped": True}

        pool = fetch_pool_state_at_block(pool_id, blk)
        if pool is None:
            raise RuntimeError(f"Pool not found at block {blk} (pool may not exist at this block yet)")

        center = int(pool["tick"]) if pool.get("tick") is not None else None
        ticks_df = fetch_ticks_at_block(pool_id, blk, center_tick=center, window=TICK_WINDOW)
        ticks_df.to_csv(tpath, index=False)
        return {
            "ridx": ridx,
            "pool_tick": int(pool["tick"]),
            "pool_liquidity": int(pool["liquidity"]),
            "meta": {
                "decimals0": int(pool["token0"]["decimals"]),
                "decimals1": int(pool["token1"]["decimals"]),
                "symbol0": pool["token0"]["symbol"],
                "symbol1": pool["token1"]["symbol"],
                "pool_id": pool_id,
                "subgraph": SUBGRAPH,
                "start_ts": START_TIMESTAMP,
                "end_ts": END_TIMESTAMP,
                "step_s": STEP_SECONDS,
            },
        }

    with ThreadPoolExecutor(max_workers=CONCURRENT_BLOCKS) as ex:
        futs = [ex.submit(_fetch_snapshot, ridx) for ridx in todo_idx]
        for fut in as_completed(futs):
            try:
                res = fut.result()
                ridx = res.get("ridx")
                if res.get("skipped"):
                    pbar.update(1)
                    continue
                if meta is None and res.get("meta"):
                    meta = res["meta"]
                    save_meta_once(meta)
                df.loc[ridx, "pool_tick"] = int(res["pool_tick"]) if res.get("pool_tick") is not None else df.loc[ridx, "pool_tick"]
                df.loc[ridx, "pool_liquidity"] = int(res["pool_liquidity"]) if res.get("pool_liquidity") is not None else df.loc[ridx, "pool_liquidity"]
                df.loc[ridx, "status"] = "done"
                df.loc[ridx, "error"] = ""
            except Exception as e:
                # If we cannot identify ridx, skip marking; minimal logging
                pass
            finally:
                save_manifest(df)
                pbar.update(1)

    pbar.close()

    if meta is None:
        loaded = load_meta()
        if loaded:
            meta = loaded
        else:
            # Fallback: try to fetch pool metadata from the latest block
            # This can help if the pool didn't exist at the queried blocks
            print("Warning: No successful pool fetch at queried blocks. Attempting to fetch metadata from latest block...")
            try:
                latest_pool = fetch_pool_state_at_block(pool_id, block_number=None)
                if latest_pool is None:
                    raise RuntimeError(
                        f"Pool {pool_id} not found in subgraph. "
                        f"This could mean:\n"
                        f"  1. The pool ID is incorrect\n"
                        f"  2. The subgraph endpoint is wrong\n"
                        f"  3. The pool doesn't exist\n"
                        f"Please check your configuration (pool_id, subgraph URL) and try again."
                    )
                meta = {
                    "decimals0": int(latest_pool["token0"]["decimals"]),
                    "decimals1": int(latest_pool["token1"]["decimals"]),
                    "symbol0": latest_pool["token0"]["symbol"],
                    "symbol1": latest_pool["token1"]["symbol"],
                    "pool_id": pool_id,
                    "subgraph": SUBGRAPH,
                    "start_ts": START_TIMESTAMP,
                    "end_ts": END_TIMESTAMP,
                    "step_s": STEP_SECONDS,
                    "note": "Metadata fetched from latest block (pool may not exist at queried blocks)"
                }
                save_meta_once(meta)
                print(f"Successfully fetched pool metadata from latest block: {latest_pool['token0']['symbol']}/{latest_pool['token1']['symbol']}")
            except Exception as e:
                error_details = str(e)
                # Check if there are any error messages in the manifest
                error_rows = df[df["status"] == "error"]
                if not error_rows.empty:
                    sample_errors = error_rows["error"].head(3).tolist()
                    error_details += f"\n\nSample errors from manifest:\n" + "\n".join(f"  - {err[:200]}" for err in sample_errors if err)
                raise RuntimeError(
                    f"Could not determine pool metadata (no successful pool fetch).\n\n"
                    f"Error: {error_details}\n\n"
                    f"Possible causes:\n"
                    f"  1. Pool {pool_id} doesn't exist at the queried blocks (blocks may be before pool creation)\n"
                    f"  2. Pool ID is incorrect\n"
                    f"  3. Subgraph endpoint is wrong or unavailable\n"
                    f"  4. Network/RPC issues\n\n"
                    f"Check the manifest file at {MANIFEST_PATH} for detailed error messages."
                )

    return df, meta

def load_snapshots_from_subgraph(pool_id: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Build an in-memory frames dataframe using checkpointed files.
    Will attempt to fetch/checkpoint missing items first.
    """
    df_manifest, meta = fetch_and_checkpoint(pool_id)

    ok = df_manifest[df_manifest["status"] == "done"].copy()
    if ok.empty:
        raise RuntimeError("No successful snapshots found. Check manifest errors in run directory.")

    frames: list[dict[str, Any]] = []
    ok.sort_values("t", inplace=True)
    for _, row in ok.iterrows():
        blk = int(row["block"])
        t = int(row["t"])
        pool_tick = int(row["pool_tick"])
        pool_liq = int(row["pool_liquidity"])
        tpath = ticks_path(blk)
        if not tpath.exists():
            continue
        # Read ticks; don't force int64 for liquidityNet, parse later
        ticks_df = pd.read_csv(tpath, dtype={"tickIdx": np.int64})
        frames.append({
            "t": t,
            "block": blk,
            "pool_tick": pool_tick,
            "pool_liquidity": pool_liq,
            "ticks_df": ticks_df[["tickIdx","liquidityNet"]].sort_values("tickIdx").reset_index(drop=True)
        })

    return pd.DataFrame(frames), meta

def _csv_meta_from_utils(pool_id: str) -> Dict[str, Any]:
    """
    Derive token metadata for CSV-driven runs using utilities shared across the project.
    Falls back to the historical defaults if the pool address is unknown.
    """
    pair_name = find_pair_name(pool_id)
    decimals0, decimals1 = token_decimals_for_pair(pair_name)
    token_symbols: Tuple[str, str] = ("TOKEN0", "TOKEN1")

    if pair_name:
        pair_meta = POOL_METADATA.get(pair_name, {})
        tokens = pair_meta.get("tokens", ())
        if len(tokens) == 2:
            token_symbols = (tokens[0].upper(), tokens[1].upper())

    # Default to previously hard-coded values when metadata is missing
    decimals0 = decimals0 if decimals0 is not None else 18
    decimals1 = decimals1 if decimals1 is not None else 6

    return {
        "decimals0": decimals0,
        "decimals1": decimals1,
        "symbol0": token_symbols[0],
        "symbol1": token_symbols[1],
        "pool_id": pool_id,
        "pair_name": pair_name,
    }

def load_snapshots_from_csv(pool_id: str = POOL_ID) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    ticks = pd.read_csv(CSV_TICKS, dtype={"tickIdx": np.int64})
    pools = pd.read_csv(CSV_POOLS)
    frames = []
    for (t, blk), df_grp in ticks.groupby(["t","block"]):
        state = pools[(pools["t"]==t) & (pools["block"]==blk)].iloc[0]
        frames.append({
            "t": int(t),
            "block": int(blk),
            "pool_tick": int(state["tick"]),
            "pool_liquidity": int(state["liquidity"]),
            "ticks_df": df_grp[["tickIdx","liquidityNet"]].sort_values("tickIdx").reset_index(drop=True)
        })
    meta = _csv_meta_from_utils(pool_id)
    return pd.DataFrame(frames), meta

def main() -> None:
    _ensure_dirs()

    if USE_SUBGRAPH:
        frames_df, meta = load_snapshots_from_subgraph(POOL_ID)
    else:
        frames_df, meta = load_snapshots_from_csv(POOL_ID)

    if frames_df.empty:
        print("No frames found.")
        return

    # Precompute profiles per frame (with a progress bar)
    profiles = []
    pbar = tqdm(total=len(frames_df), desc="Building profiles")
    pool_prices: list[float] = []
    for _, row in frames_df.iterrows():
        pool_price = tick_to_price(
            int(row["pool_tick"]),
            int(meta["decimals0"]),
            int(meta["decimals1"]),
        )
        spot_price = float(pool_price) if np.isfinite(pool_price) and pool_price > 0 else None
        if spot_price is not None:
            pool_prices.append(spot_price)
        pe, Lv = ticks_to_profile(
            row["ticks_df"],
            pool_tick=row["pool_tick"],
            pool_liquidity=row["pool_liquidity"],
            decimals0=meta["decimals0"],
            decimals1=meta["decimals1"],
        )
        profiles.append((int(row["t"]), int(row["block"]), pe, Lv, spot_price))
        pbar.update(1)
    pbar.close()

    # Determine sensible x-limits
    xmin = PRICE_MIN
    xmax = PRICE_MAX

    if (PRICE_MIN is None or PRICE_MAX is None) and pool_prices:
        derived_min = float(np.nanmin(pool_prices))
        derived_max = float(np.nanmax(pool_prices))
        if derived_min > 0 and derived_max > 0:
            if derived_min == derived_max:
                derived_min *= 0.95
                derived_max *= 1.05
            xmin = PRICE_MIN if PRICE_MIN is not None else derived_min
            xmax = PRICE_MAX if PRICE_MAX is not None else derived_max

    if xmin is None or xmax is None:
        all_prices = np.concatenate([pe for (_,_,pe,_,_) in profiles if len(pe)>1])
        xmin = np.nanmin(all_prices) if xmin is None else xmin
        xmax = np.nanmax(all_prices) if xmax is None else xmax

    # Animation
    fig, ax = plt.subplots(figsize=(9, 5))
    sym0, sym1 = meta["symbol0"], meta["symbol1"]

    def update(i):
        ts, blk, pe, Lv, spot_price = profiles[i]
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts))
        title = f"{TITLE_PREFIX} — {sym1}/{sym0} — {ts_str} (block {blk})"
        frame_plot(ax, pe, Lv, title, xlog=XLOG, pmin=xmin, pmax=xmax)
        if spot_price is not None and spot_price > 0:
            ax.axvline(
                spot_price,
                color="crimson",
                linestyle="--",
                linewidth=1.3,
                alpha=0.85,
            )

    out_path = RUN_DIR / OUT_GIF
    print(f"Rendering GIF to: {out_path}")
    anim = FuncAnimation(fig, update, frames=len(profiles), interval=1000/ANIM_FPS)
    writer = PillowWriter(fps=ANIM_FPS)
    anim.save(str(out_path), writer=writer)
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    main()
