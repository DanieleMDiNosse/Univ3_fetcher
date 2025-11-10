
"""
data_fetch_subgraph.py — Subgraph-first Uniswap v3 pool harvester (robust)
--------------------------------------------------------------------------
Improvements in this patch:
  • Receipt fetching is resilient to TransactionNotFound (retry + graceful skip).
  • If a receipt is missing, we FALL BACK to eth_getLogs at the specific block to decode Swap.liquidity.
  • Optional progress bar via tqdm; if tqdm is unavailable, falls back to simple prints.

Behavior remains the same:
  - Subgraph for events & state, RPC only for Swap.liquidity and (if skip_gas_data=False) gas fields.
  - Checkpoints/resume, chunking, running-state fields preserved.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import pickle
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
import requests
import yaml
from eth_defi.provider.multi_provider import create_multi_provider_web3
from web3 import Web3
from web3._utils.events import get_event_data
from web3.exceptions import TransactionNotFound

from decimal import Decimal

from utils import sqrtPriceX96_to_price

# ---------- Optional progress bar ----------
try:
    from tqdm import tqdm as _tqdm
    def TQDM(total=None, desc=""):
        return _tqdm(total=total, desc=desc, dynamic_ncols=True, leave=False)
    TQDM_AVAILABLE = True
except Exception:
    TQDM_AVAILABLE = False
    class DummyPB:
        def __init__(self, total=None, desc=""): self.n=0
        def update(self, k=1): self.n+=k
        def set_postfix(self, **k): pass
        def close(self): pass
    def TQDM(total=None, desc=""): return DummyPB()

# ---------------- Configuration ----------------

CONFIG_PATH = os.environ.get("DATA_FETCH_CONFIG_PATH", "data_fetch_config.yml")

DEFAULT_CONFIG: Dict[str, Any] = {
    "graph_url": "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
    "json_rpc_urls": [
        "https://eth.llamarpc.com",
        "https://mainnet.infura.io/v3/YOUR_KEY",
    ],
    "pool_addr": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "start_ts": "2023-01-01T00:00:00Z",
    "end_ts": "2023-01-01T23:59:59Z",
    "subgraph_page_size": 1000,
    "chunk_events": 10000,
    "checkpoint_path": "univ3_checkpoint.json",
    "out_dir": "data/univ3_{pool}_pkl",
    "skip_gas_data": True,
    "batch_receipt_size": 100,
    "compute_price_column": True,
}


def _to_unix(ts: Any) -> int:
    if isinstance(ts, (int, np.integer)):
        return int(ts)
    if isinstance(ts, str):
        s = ts.strip()
        if s.lstrip("-").isdigit():
            return int(s)
        return int(pd.to_datetime(s, utc=True).value // 10**9)
    return int(pd.to_datetime(ts, utc=True).value // 10**9)


def load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        print(f"Configuration file {path!r} not found. Using defaults.")
        cfg = DEFAULT_CONFIG.copy()
    else:
        with open(path, "r") as fh:
            loaded = yaml.safe_load(fh) or {}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update(loaded)

    cfg["json_rpc_urls"] = [str(u).strip() for u in cfg.get("json_rpc_urls", []) if str(u).strip()]
    if not cfg["json_rpc_urls"]:
        raise ValueError("Config field 'json_rpc_urls' must be a non-empty list (needed for receipts).")

    cfg["graph_url"] = str(cfg["graph_url"]).strip()
    if not cfg["graph_url"]:
        raise ValueError("Config field 'graph_url' must be set to a valid Uniswap v3 subgraph endpoint.")

    cfg["pool_addr"] = Web3.to_checksum_address(cfg["pool_addr"]).lower()

    cfg["start_ts"] = _to_unix(cfg["start_ts"])
    cfg["end_ts"]   = _to_unix(cfg["end_ts"])
    if cfg["end_ts"] < cfg["start_ts"]:
        raise ValueError("end_ts must be >= start_ts")

    cfg["subgraph_page_size"] = int(cfg.get("subgraph_page_size", 1000))
    cfg["chunk_events"]       = int(cfg.get("chunk_events", 10000))
    cfg["batch_receipt_size"] = int(cfg.get("batch_receipt_size", 100))
    cfg["skip_gas_data"]      = bool(cfg.get("skip_gas_data", False))
    cfg["compute_price_column"] = bool(cfg.get("compute_price_column", True))

    return cfg


CFG = load_config(CONFIG_PATH)

GRAPH_URL: str = CFG["graph_url"]
JSON_RPC_URLS: List[str] = CFG["json_rpc_urls"]
POOL_ADDR: str = CFG["pool_addr"]
START_TS: int = CFG["start_ts"]
END_TS: int = CFG["end_ts"]
PAGE_SIZE: int = CFG["subgraph_page_size"]
CHUNK_EVENTS: int = CFG["chunk_events"]
BATCH_RECEIPT_SIZE: int = CFG["batch_receipt_size"]
CHECKPOINT_PATH: str = CFG["checkpoint_path"]
OUT_DIR: str = CFG["out_dir"].format(pool=POOL_ADDR)
SKIP_GAS_DATA: bool = CFG["skip_gas_data"]
COMPUTE_PRICE: bool = CFG["compute_price_column"]

os.makedirs(os.path.abspath(os.path.join(OUT_DIR, os.pardir)), exist_ok=True)

w3 = create_multi_provider_web3(" ".join(JSON_RPC_URLS), request_kwargs={"timeout": 60.0})
Q96 = 1 << 96


# ---------------- Graph helpers ----------------

def gql(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(5):
        try:
            r = requests.post(GRAPH_URL, json={"query": query, "variables": variables}, timeout=60)
            r.raise_for_status()
            data = r.json()
            if "errors" in data:
                raise RuntimeError(str(data["errors"]))
            return data["data"]
        except Exception:
            if attempt == 4:
                raise
            time.sleep(1.0 + attempt * 0.5)
    raise RuntimeError("GraphQL request failed after retries")


Q_SWAP_PAGE = """
query($pool: Bytes!, $lo: BigInt!, $hi: BigInt!, $afterId: ID!, $n: Int!) {
  swaps(first: $n, orderBy: timestamp, orderDirection: asc,
        where: { pool: $pool, timestamp_gte: $lo, timestamp_lt: $hi, id_gt: $afterId }) {
    id
    timestamp
    logIndex
    origin
    transaction { id blockNumber }
    sender
    recipient
    amount0
    amount1
    sqrtPriceX96
    tick
  }
}
"""

Q_MINT_PAGE = """
query($pool: Bytes!, $lo: BigInt!, $hi: BigInt!, $afterId: ID!, $n: Int!) {
  mints(first: $n, orderBy: timestamp, orderDirection: asc,
        where: { pool: $pool, timestamp_gte: $lo, timestamp_lt: $hi, id_gt: $afterId }) {
    id
    timestamp
    logIndex
    origin
    transaction { id blockNumber }
    sender
    owner
    amount
    amount0
    amount1
    tickLower
    tickUpper
  }
}
"""

Q_BURN_PAGE = """
query($pool: Bytes!, $lo: BigInt!, $hi: BigInt!, $afterId: ID!, $n: Int!) {
  burns(first: $n, orderBy: timestamp, orderDirection: asc,
        where: { pool: $pool, timestamp_gte: $lo, timestamp_lt: $hi, id_gt: $afterId }) {
    id
    timestamp
    logIndex
    origin
    transaction { id blockNumber }
    owner
    amount
    amount0
    amount1
    tickLower
    tickUpper
  }
}
"""

Q_POOL_AT_BLOCK = """
query($pool: ID!, $b: Int!) {
  pool(id: $pool, block: { number: $b }) {
    liquidity
    sqrtPrice
    tick
    token0 { decimals }
    token1 { decimals }
  }
}
"""

Q_FIRST_EVENT_BLOCKS = """
query($pool: Bytes!, $ts: BigInt!) {
  s: swaps(first:1, orderBy: timestamp, orderDirection: asc, where:{pool:$pool, timestamp_gte:$ts}) {
    transaction{ blockNumber }
  }
  m: mints(first:1, orderBy: timestamp, orderDirection: asc, where:{pool:$pool, timestamp_gte:$ts}) {
    transaction{ blockNumber }
  }
  b: burns(first:1, orderBy: timestamp, orderDirection: asc, where:{pool:$pool, timestamp_gte:$ts}) {
    transaction{ blockNumber }
  }
}
"""


def find_first_event_block(pool: str, start_ts: int) -> Optional[int]:
    d = gql(Q_FIRST_EVENT_BLOCKS, {"pool": pool, "ts": start_ts})
    cands = []
    for key in ("s", "m", "b"):
        arr = d.get(key) or []
        if arr:
            try:
                cands.append(int(arr[0]["transaction"]["blockNumber"]))
            except Exception:
                pass
    return min(cands) if cands else None


def fetch_pool_state_and_decimals_at_block(pool: str, block_num: int) -> Tuple[int, int, int, Optional[int], Optional[int]]:
    d = gql(Q_POOL_AT_BLOCK, {"pool": pool, "b": block_num})
    p = d.get("pool")
    if not p:
        raise RuntimeError(f"Subgraph returned no pool state at block {block_num}")
    L = int(p["liquidity"])
    sP = int(p["sqrtPrice"])
    tk = int(p["tick"])
    dec0 = int(p["token0"]["decimals"]) if p.get("token0") and p["token0"].get("decimals") is not None else None
    dec1 = int(p["token1"]["decimals"]) if p.get("token1") and p["token1"].get("decimals") is not None else None
    return L, sP, tk, dec0, dec1


@dataclass
class Cursor:
    last_id: str = ""
    exhausted: bool = False


@dataclass
class EventRow:
    eventType: str
    blockNumber: int
    logIndex: int
    timestamp: int
    transactionHash: str
    origin: Optional[str]
    gasUsed: Optional[int]
    gasPrice: Optional[int]
    effectiveGasPrice: Optional[int]
    sender: Optional[str]
    owner: Optional[str]
    recipient: Optional[str]
    amount0: Optional[str]
    amount1: Optional[str]
    sqrtPriceX96_event: Optional[int]
    tick_event: Optional[int]
    liquidityAfter_event: Optional[int]
    tickLower: Optional[int]
    tickUpper: Optional[int]
    liquidityDelta: Optional[int]
    L_before: Optional[int] = None
    sqrt_before: Optional[int] = None
    tick_before: Optional[int] = None
    x_before: Optional[int] = None
    y_before: Optional[int] = None
    affectsActive: Optional[bool] = None
    deltaL_applied: Optional[int] = None
    L_after: Optional[int] = None
    sqrt_after: Optional[int] = None
    tick_after: Optional[int] = None
    x_after: Optional[int] = None
    y_after: Optional[int] = None
    price: Optional[float] = None


# ---------------- Receipt helpers (RPC) ----------------

SWAP_TOPIC0 = Web3.keccak(text="Swap(address,address,int256,int256,uint160,uint128,int24)").hex()
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

def safe_get_receipt(tx_hash: str, retries: int = 3, delay: float = 0.5):
    for k in range(retries):
        try:
            return w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            time.sleep(delay * (k+1))
        except Exception:
            time.sleep(delay * (k+1))
    return None

def batch_fetch_receipts(tx_hashes: Sequence[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    tx_hashes = list(dict.fromkeys(tx_hashes))
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=min(10, len(tx_hashes) or 1)) as ex:
        futs = [ex.submit(lambda h: (h, safe_get_receipt(h)), h) for h in tx_hashes]
        for fut in as_completed(futs):
            h, rc = fut.result()
            out[h] = rc
    return out

def _decode_liquidity_from_log(log: Dict[str, Any]) -> Optional[int]:
    try:
        decoded = get_event_data(w3.codec, SWAP_EVENT_ABI, log)
        return int(decoded["args"]["liquidity"])
    except Exception:
        return None

def enrich_swaps_with_liquidity(rows: List[EventRow], receipts_cache: Dict[str, Any]) -> None:
    # 1) Primary path: use receipts
    per_tx_liq: Dict[str, Dict[int, int]] = {}
    for txh, rc in receipts_cache.items():
        if not rc:
            continue
        liq_by_log: Dict[int, int] = {}
        for lg in rc.get("logs", []):
            try:
                if (lg.get("address","").lower() != POOL_ADDR):
                    continue
                t0raw = lg.get("topics", [None])[0]
                t0 = t0raw.hex() if hasattr(t0raw, "hex") else t0raw
                if t0 != SWAP_TOPIC0:
                    continue
                liq = _decode_liquidity_from_log(lg)
                if liq is not None:
                    liq_by_log[int(lg["logIndex"])] = liq
            except Exception:
                continue
        if liq_by_log:
            per_tx_liq[txh] = liq_by_log

    # 2) Fill from receipts
    missing: List[EventRow] = []
    for r in rows:
        if r.eventType.startswith("swap"):
            m = per_tx_liq.get(r.transactionHash, {})
            liq = m.get(r.logIndex)
            if liq is not None:
                r.liquidityAfter_event = liq
            else:
                missing.append(r)

    # 3) Fallback: query logs at the *specific block* and decode
    for r in missing:
        try:
            logs = w3.eth.get_logs({
                "fromBlock": r.blockNumber,
                "toBlock": r.blockNumber,
                "address": POOL_ADDR,
                "topics": [SWAP_TOPIC0],
            })
            for lg in logs:
                # Web3 returns attribute access; normalize
                txh = lg.get("transactionHash")
                txh_hex = txh.hex() if hasattr(txh, "hex") else str(txh)
                if txh_hex != r.transactionHash:
                    continue
                if int(lg.get("logIndex")) != r.logIndex:
                    continue
                liq = _decode_liquidity_from_log(lg)
                if liq is not None:
                    r.liquidityAfter_event = liq
                    break
        except Exception:
            # leave as None if not obtainable
            pass

def enrich_with_gas(rows: List[EventRow], receipts_cache: Dict[str, Any]) -> None:
    for r in rows:
        rc = receipts_cache.get(r.transactionHash)
        if not rc:
            continue
        try:
            r.gasUsed = int(rc.get("gasUsed")) if rc.get("gasUsed") is not None else None
        except Exception:
            pass
        try:
            r.effectiveGasPrice = int(rc.get("effectiveGasPrice")) if rc.get("effectiveGasPrice") is not None else None
        except Exception:
            pass
        if r.gasPrice is None:
            try:
                tx = w3.eth.get_transaction(r.transactionHash)
                if tx and tx.get("gasPrice") is not None:
                    r.gasPrice = int(tx["gasPrice"])
            except Exception:
                pass


# ---------------- Utility ----------------

def virt_x(L: int, sP: int) -> Optional[int]:
    try:
        return (int(L) * (1 << 96)) // int(sP) if (L is not None and sP) else None
    except Exception:
        return None

def virt_y(L: int, sP: int) -> Optional[int]:
    try:
        return (int(L) * int(sP)) // (1 << 96) if (L is not None and sP) else None
    except Exception:
        return None


def compute_price_value(sqrt_val: Optional[int], dec0: Optional[int], dec1: Optional[int]) -> Optional[float]:
    if not COMPUTE_PRICE or dec0 is None or dec1 is None:
        return None
    if sqrt_val in (None, 0):
        return None
    try:
        return sqrtPriceX96_to_price(float(sqrt_val), int(dec0), int(dec1))
    except Exception:
        return None


# ---------------- Paging generators ----------------

@dataclass
class Streams:
    swap: Cursor
    mint: Cursor
    burn: Cursor
    buf_swap: List[Dict[str, Any]]
    buf_mint: List[Dict[str, Any]]
    buf_burn: List[Dict[str, Any]]


def page_swaps(pool: str, lo_ts: int, hi_ts: int, cur: Cursor) -> List[Dict[str, Any]]:
    d = gql(Q_SWAP_PAGE, {"pool": pool, "lo": lo_ts, "hi": hi_ts, "afterId": cur.last_id or "", "n": PAGE_SIZE})
    return d.get("swaps") or []

def page_mints(pool: str, lo_ts: int, hi_ts: int, cur: Cursor) -> List[Dict[str, Any]]:
    d = gql(Q_MINT_PAGE, {"pool": pool, "lo": lo_ts, "hi": hi_ts, "afterId": cur.last_id or "", "n": PAGE_SIZE})
    return d.get("mints") or []

def page_burns(pool: str, lo_ts: int, hi_ts: int, cur: Cursor) -> List[Dict[str, Any]]:
    d = gql(Q_BURN_PAGE, {"pool": pool, "lo": lo_ts, "hi": hi_ts, "afterId": cur.last_id or "", "n": PAGE_SIZE})
    return d.get("burns") or []


@dataclass
class Cursor:
    last_id: str = ""
    exhausted: bool = False


@dataclass
class Streams:
    swap: Cursor
    mint: Cursor
    burn: Cursor
    buf_swap: List[Dict[str, Any]]
    buf_mint: List[Dict[str, Any]]
    buf_burn: List[Dict[str, Any]]


def merged_event_stream(pool: str, lo_ts: int, hi_ts: int) -> Iterator[Tuple[str, Dict[str, Any]]]:
    st = Streams(Cursor(), Cursor(), Cursor(), [], [], [])
    def refill(kind: str):
        cur = getattr(st, kind)
        buf = getattr(st, f"buf_{kind}")
        if cur.exhausted or len(buf) >= PAGE_SIZE // 4:
            return
        page_fn = {"swap": page_swaps, "mint": page_mints, "burn": page_burns}[kind]
        while True:
            page = page_fn(pool, lo_ts, hi_ts, cur)
            if not page:
                cur.exhausted = True
                break
            buf.extend(page)
            cur.last_id = page[-1]["id"]
            if len(buf) > 0:
                break

    refill("swap"); refill("mint"); refill("burn")

    while True:
        if all([st.swap.exhausted, st.mint.exhausted, st.burn.exhausted]) and not (st.buf_swap or st.buf_mint or st.buf_burn):
            return

        cands: List[Tuple[str, Dict[str, Any], Tuple[int, int, int]]] = []
        if st.buf_swap:
            s = st.buf_swap[0]
            cands.append(("swap", s, (int(s["timestamp"]), int(s["transaction"]["blockNumber"]), int(s["logIndex"] or 0))))
        if st.buf_mint:
            m = st.buf_mint[0]
            cands.append(("mint", m, (int(m["timestamp"]), int(m["transaction"]["blockNumber"]), int(m["logIndex"] or 0))))
        if st.buf_burn:
            b = st.buf_burn[0]
            cands.append(("burn", b, (int(b["timestamp"]), int(b["transaction"]["blockNumber"]), int(b["logIndex"] or 0))))

        if not cands:
            refill("swap"); refill("mint"); refill("burn")
            if not (st.buf_swap or st.buf_mint or st.buf_burn):
                return
            continue

        cands.sort(key=lambda x: x[2])
        etype, item, _ = cands[0]
        if etype == "swap":
            st.buf_swap.pop(0); refill("swap")
        elif etype == "mint":
            st.buf_mint.pop(0); refill("mint")
        else:
            st.buf_burn.pop(0); refill("burn")

        yield etype, item


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


def pickle_write_chunk(df: pd.DataFrame, root_dir: str, first_block: int, last_block: int) -> str:
    os.makedirs(root_dir, exist_ok=True)
    chunk_name = f"blocks_{first_block}_{last_block}.pkl"
    out_path = os.path.join(root_dir, chunk_name)
    if os.path.exists(out_path):
        os.remove(out_path)
    df.to_pickle(out_path, compression=None, protocol=pickle.HIGHEST_PROTOCOL)
    return out_path


# ---------------- Driver ----------------

print(f"Starting (subgraph-first) data fetch for pool {POOL_ADDR}")
print(f"Date range: {pd.to_datetime(START_TS, unit='s')} to {pd.to_datetime(END_TS, unit='s')}")
print(f"Settings: PAGE_SIZE={PAGE_SIZE}, CHUNK_EVENTS={CHUNK_EVENTS}, SKIP_GAS={SKIP_GAS_DATA}")

ckpt = load_checkpoint(CHECKPOINT_PATH)

cur_L = cur_sqrt = cur_tick = None
TOKEN0_DECIMALS = TOKEN1_DECIMALS = None

first_window_block: Optional[int] = None
last_window_block: Optional[int] = None

if ckpt and ckpt.get("pool") == POOL_ADDR and ckpt.get("start_ts") == START_TS and ckpt.get("end_ts") == END_TS:
    print("Resuming from checkpoint…")
    cur_L = int(ckpt["cur_L"])
    cur_sqrt = int(ckpt["cur_sqrt"])
    cur_tick = int(ckpt["cur_tick"])
    TOKEN0_DECIMALS = ckpt.get("token0_decimals")
    TOKEN1_DECIMALS = ckpt.get("token1_decimals")
    first_window_block = ckpt.get("first_window_block")
    last_window_block = ckpt.get("last_window_block")
    out_dir_ckpt = ckpt.get("out_pickle_dir")
    if out_dir_ckpt and out_dir_ckpt != OUT_DIR:
        print(f"  ⚠️  Output directory changed from ckpt: {out_dir_ckpt} -> {OUT_DIR}")
else:
    b0 = find_first_event_block(POOL_ADDR, START_TS)
    if b0 is None:
        print("No events found in the requested window; nothing to do.")
        raise SystemExit(0)
    first_window_block = b0
    prev_block = max(b0 - 1, 0)

    cur_L, cur_sqrt, cur_tick, TOKEN0_DECIMALS, TOKEN1_DECIMALS = fetch_pool_state_and_decimals_at_block(POOL_ADDR, prev_block)
    print(f"Initial state at block {prev_block}: L={cur_L}, sP={cur_sqrt}, tick={cur_tick}")
    if os.path.isdir(OUT_DIR):
        print(f"Removing previous output directory {OUT_DIR}")
        shutil.rmtree(OUT_DIR)
    elif os.path.exists(OUT_DIR):
        print(f"Removing previous output file {OUT_DIR}")
        os.remove(OUT_DIR)

    ckpt = {
        "version": "subgraph_first_v1",
        "pool": POOL_ADDR,
        "graph_url": GRAPH_URL,
        "start_ts": START_TS,
        "end_ts": END_TS,
        "first_window_block": first_window_block,
        "last_window_block": None,
        "cur_L": int(cur_L),
        "cur_sqrt": int(cur_sqrt),
        "cur_tick": int(cur_tick),
        "token0_decimals": TOKEN0_DECIMALS,
        "token1_decimals": TOKEN1_DECIMALS,
        "events_written": 0,
        "out_pickle_dir": OUT_DIR,
        "cursor": {
            "swap_last_id": "",
            "mint_last_id": "",
            "burn_last_id": "",
            "exhausted_swap": False,
            "exhausted_mint": False,
            "exhausted_burn": False,
            "last_timestamp_seen": START_TS,
        },
    }
    save_checkpoint(CHECKPOINT_PATH, ckpt)

cur_swap = Cursor(ckpt["cursor"]["swap_last_id"], ckpt["cursor"]["exhausted_swap"])
cur_mint = Cursor(ckpt["cursor"]["mint_last_id"], ckpt["cursor"]["exhausted_mint"])
cur_burn = Cursor(ckpt["cursor"]["burn_last_id"], ckpt["cursor"]["exhausted_burn"])

def merged_stream_with_cursors(pool: str, lo_ts: int, hi_ts: int, cur_swap: Cursor, cur_mint: Cursor, cur_burn: Cursor):
    st = Streams(cur_swap, cur_mint, cur_burn, [], [], [])

    def refill(kind: str):
        cur = getattr(st, kind)
        buf = getattr(st, f"buf_{kind}")
        if cur.exhausted or len(buf) >= PAGE_SIZE // 4:
            return
        page_fn = {"swap": page_swaps, "mint": page_mints, "burn": page_burns}[kind]
        while True:
            page = page_fn(pool, lo_ts, hi_ts, cur)
            if not page:
                cur.exhausted = True
                break
            buf.extend(page)
            cur.last_id = page[-1]["id"]
            ckpt["cursor"][f"{kind}_last_id"] = cur.last_id
            if len(buf) > 0:
                break

    refill("swap"); refill("mint"); refill("burn")

    while True:
        if all([st.swap.exhausted, st.mint.exhausted, st.burn.exhausted]) and not (st.buf_swap or st.buf_mint or st.buf_burn):
            return
        cands: List[Tuple[str, Dict[str, Any], Tuple[int,int,int]]] = []
        if st.buf_swap:
            s = st.buf_swap[0]; cands.append(("swap", s, (int(s["timestamp"]), int(s["transaction"]["blockNumber"]), int(s["logIndex"] or 0))))
        if st.buf_mint:
            m = st.buf_mint[0]; cands.append(("mint", m, (int(m["timestamp"]), int(m["transaction"]["blockNumber"]), int(m["logIndex"] or 0))))
        if st.buf_burn:
            b = st.buf_burn[0]; cands.append(("burn", b, (int(b["timestamp"]), int(b["transaction"]["blockNumber"]), int(b["logIndex"] or 0))))
        if not cands:
            refill("swap"); refill("mint"); refill("burn")
            if not (st.buf_swap or st.buf_mint or st.buf_burn):
                return
            continue
        cands.sort(key=lambda x: x[2])
        etype, item, _ = cands[0]
        if etype == "swap": st.buf_swap.pop(0); refill("swap")
        elif etype == "mint": st.buf_mint.pop(0); refill("mint")
        else: st.buf_burn.pop(0); refill("burn")
        yield etype, item

start_t = time.time()
buf_rows: List[EventRow] = []
events_total = int(ckpt.get("events_written", 0))
chunk_first_block: Optional[int] = None
chunk_last_block: Optional[int] = None

pb_events = TQDM(total=None, desc="Events")

def flush_chunk():
    global buf_rows, cur_L, cur_sqrt, cur_tick, events_total, chunk_first_block, chunk_last_block, last_window_block

    if not buf_rows:
        return

    # Progress for receipts
    swap_txs = sorted(set([r.transactionHash for r in buf_rows if r.eventType.startswith("swap")]))
    receipts_map: Dict[str, Any] = {}
    if swap_txs:
        pb_rc = TQDM(total=len(swap_txs), desc="Receipts")
        for i in range(0, len(swap_txs), BATCH_RECEIPT_SIZE):
            batch = swap_txs[i:i+BATCH_RECEIPT_SIZE]
            receipts_map.update(batch_fetch_receipts(batch))
            pb_rc.update(len(batch))
        pb_rc.close()

    enrich_swaps_with_liquidity(buf_rows, receipts_map)

    if not SKIP_GAS_DATA:
        txs_all = sorted(set([r.transactionHash for r in buf_rows]))
        if txs_all:
            pb_g = TQDM(total=len(txs_all), desc="Gas meta")
            for i in range(0, len(txs_all), BATCH_RECEIPT_SIZE):
                batch = txs_all[i:i+BATCH_RECEIPT_SIZE]
                missing = [h for h in batch if h not in receipts_map]
                if missing:
                    receipts_map.update(batch_fetch_receipts(missing))
                pb_g.update(len(batch))
            pb_g.close()
        enrich_with_gas(buf_rows, receipts_map)

    buf_rows.sort(key=lambda r: (r.blockNumber, r.logIndex))

    for r in buf_rows:
        r.L_before = int(cur_L)
        r.sqrt_before = int(cur_sqrt)
        r.tick_before = int(cur_tick)
        r.x_before = virt_x(r.L_before, r.sqrt_before)
        r.y_before = virt_y(r.L_before, r.sqrt_before)

        if r.eventType in ("Mint", "Burn"):
            hit = (r.tickLower <= r.tick_before < r.tickUpper)
            r.affectsActive = bool(hit)
            applied = int(r.liquidityDelta) if hit else 0
            r.deltaL_applied = applied
            cur_L = int(cur_L) + applied
        else:
            if r.liquidityAfter_event is not None:
                cur_L = int(r.liquidityAfter_event)
            cur_sqrt = int(r.sqrtPriceX96_event) if r.sqrtPriceX96_event is not None else cur_sqrt
            cur_tick = int(r.tick_event) if r.tick_event is not None else cur_tick

        r.L_after = int(cur_L)
        r.sqrt_after = int(cur_sqrt)
        r.tick_after = int(cur_tick)
        r.x_after = virt_x(r.L_after, r.sqrt_after)
        r.y_after = virt_y(r.L_after, r.sqrt_after)
        sqrt_for_price = r.sqrtPriceX96_event or r.sqrt_after or r.sqrt_before
        r.price = compute_price_value(sqrt_for_price, ckpt.get("token0_decimals"), ckpt.get("token1_decimals"))

    df = pd.DataFrame([r.__dict__ for r in buf_rows])

    fb = int(chunk_first_block) if chunk_first_block is not None else int(df["blockNumber"].min())
    lb = int(chunk_last_block) if chunk_last_block is not None else int(df["blockNumber"].max())
    last_window_block = max(last_window_block or lb, lb)

    path = pickle_write_chunk(df, OUT_DIR, fb, lb)
    events_total += len(df)

    ckpt.update({
        "cur_L": int(cur_L), "cur_sqrt": int(cur_sqrt), "cur_tick": int(cur_tick),
        "events_written": int(events_total),
        "last_window_block": int(last_window_block),
        "out_pickle_dir": OUT_DIR,
    })
    save_checkpoint(CHECKPOINT_PATH, ckpt)

    print(f"  ✓ Wrote {len(df):,} rows | Last event block: {lb} | Chunk: {path}")

    buf_rows = []
    chunk_first_block = None
    chunk_last_block = None


print("Streaming events from subgraph…")
stream_iter = merged_stream_with_cursors(POOL_ADDR, START_TS, END_TS, cur_swap, cur_mint, cur_burn)

for etype, ev in stream_iter:
    bn = int(ev["transaction"]["blockNumber"])
    ts = int(ev["timestamp"])
    txh = ev["transaction"]["id"]

    if first_window_block is None:
        first_window_block = bn
        ckpt["first_window_block"] = first_window_block
    last_window_block = bn

    if chunk_first_block is None:
        chunk_first_block = bn
    chunk_last_block = bn

    if etype == "swap":
        amt0_dec = Decimal(ev["amount0"])
        direction = "swap_y2x" if amt0_dec < 0 else "swap_x2y"
        row = EventRow(
            eventType=direction, blockNumber=bn, logIndex=int(ev["logIndex"] or 0), timestamp=ts, transactionHash=txh,
            origin=ev.get("origin"), gasUsed=None, gasPrice=None, effectiveGasPrice=None,
            sender=ev.get("sender"), owner=None, recipient=ev.get("recipient"),
            amount0=str(ev["amount0"]), amount1=str(ev["amount1"]),
            sqrtPriceX96_event=int(ev["sqrtPriceX96"]), tick_event=int(ev["tick"]), liquidityAfter_event=None,
            tickLower=None, tickUpper=None, liquidityDelta=None
        )
    elif etype == "mint":
        row = EventRow(
            eventType="Mint", blockNumber=bn, logIndex=int(ev["logIndex"] or 0), timestamp=ts, transactionHash=txh,
            origin=ev.get("origin"), gasUsed=None, gasPrice=None, effectiveGasPrice=None,
            sender=ev.get("sender"), owner=ev.get("owner"), recipient=None,
            amount0=str(ev["amount0"]), amount1=str(ev["amount1"]),
            sqrtPriceX96_event=None, tick_event=None, liquidityAfter_event=None,
            tickLower=int(ev["tickLower"]), tickUpper=int(ev["tickUpper"]), liquidityDelta=int(ev["amount"])
        )
    else:  # burn
        row = EventRow(
            eventType="Burn", blockNumber=bn, logIndex=int(ev["logIndex"] or 0), timestamp=ts, transactionHash=txh,
            origin=ev.get("origin"), gasUsed=None, gasPrice=None, effectiveGasPrice=None,
            sender=None, owner=ev.get("owner"), recipient=None,
            amount0=str(ev["amount0"]), amount1=str(ev["amount1"]),
            sqrtPriceX96_event=None, tick_event=None, liquidityAfter_event=None,
            tickLower=int(ev["tickLower"]), tickUpper=int(ev["tickUpper"]), liquidityDelta=-int(ev["amount"])
        )

    buf_rows.append(row)
    pb_events.update(1)
    if len(buf_rows) >= CHUNK_EVENTS:
        pb_events.set_postfix(block=bn, events=events_total + len(buf_rows))
        flush_chunk()

# Final flush
flush_chunk()
pb_events.close()

# Merge chunk files
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

    if COMPUTE_PRICE:
        if "price" not in combined_df.columns or combined_df["price"].isna().all():
            def _price_row(r):
                sv = r.get("sqrtPriceX96_event") or r.get("sqrt_after") or r.get("sqrt_before")
                try:
                    return sqrtPriceX96_to_price(float(sv), int(ckpt.get("token0_decimals")), int(ckpt.get("token1_decimals"))) if sv else np.nan
                except Exception:
                    return np.nan
            combined_df["price"] = combined_df.apply(_price_row, axis=1)

    merged_dir = Path("data")
    merged_dir.mkdir(parents=True, exist_ok=True)
    merged_path = merged_dir / f"{POOL_ADDR}.pkl"
    combined_df.to_pickle(merged_path, compression=None, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"  ✓ Combined {total_rows:,} rows → {merged_path}")
else:
    print("\n⚠️  No chunk files found to merge into a consolidated pickle.")

elapsed_total = time.time() - start_t
print(f"\n{'='*60}")
print("✅ COMPLETED!")
print(f"Time range: {pd.to_datetime(START_TS, unit='s')} to {pd.to_datetime(END_TS, unit='s')}")
print(f"Total events written: {ckpt.get('events_written', 0):,}")
print(f"Output directory: {OUT_DIR}")
if first_window_block is not None and last_window_block is not None:
    print(f"Block range (observed): {first_window_block} to {last_window_block}")
print(f"Total time: {str(_dt.timedelta(seconds=int(elapsed_total)))}")
print(f"{'='*60}")
