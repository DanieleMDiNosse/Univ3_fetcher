import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from datetime import date, datetime, time, timezone
from numbers import Integral
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
import yaml
from web3 import Web3

from utils import POOL_METADATA

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "data_fetch_config.yml"

DEFAULTS: Dict[str, object] = {
    "graph_url": "https://gateway.thegraph.com/api/42e297632dfbe248cf6ac11ded17e89f/subgraphs/id/5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "json_rpc_urls": [
        "https://eth.llamarpc.com/sk_llama_252714c1e64c9873e3b21ff94d7f1a3f",
        "https://mainnet.infura.io/v3/5f38fb376e0548c8a828112252a6a588",
    ],
    "pool_addr": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
    "start_ts": "2023-01-01T00:00:00Z",
    "end_ts": "2023-01-02T00:00:00Z",
    "chunk_size_blocks": 200,
    "parallel_workers": 8,
    "batch_receipt_size": 100,
    "subgraph_page_size": 1000,
    "chunk_events": 10000,
}

if DEFAULT_CONFIG_PATH.exists():
    try:
        with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as fh:
            from_file = yaml.safe_load(fh) or {}
        # Update DEFAULTS with values from config file, preserving defaults for keys not in config
        for k in DEFAULTS:
            if k in from_file:
                DEFAULTS[k] = from_file[k]
        # Also add any additional keys from config file that aren't in DEFAULTS
        for k, v in from_file.items():
            if k not in DEFAULTS:
                DEFAULTS[k] = v
    except Exception:
        pass


def parse_default_timestamp(value: object, fallback: datetime) -> datetime:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value).astimezone(timezone.utc)
        except ValueError:
            pass
    return fallback


def slugify(value: str, max_length: int = 64) -> str:
    slug = re.sub(r"[^0-9A-Za-z_-]+", "-", value.strip().lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:max_length] or "run"


def isoformat_utc(d: date, t: time) -> Tuple[str, datetime]:
    clean_time = t.replace(tzinfo=None)
    dt = datetime.combine(d, clean_time, tzinfo=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z"), dt


def ensure_unique(path: Path) -> Path:
    base = path
    counter = 1
    while path.exists():
        path = base.with_name(f"{base.stem}_{counter}{base.suffix}")
        counter += 1
    return path


def should_suppress_log_line(line: str) -> bool:
    lowered = line.lower()
    return ("site-packages" in lowered and "warning" in lowered) or lowered.startswith("python warning")


PROGRESS_PATTERN = re.compile(r"\[(\d+(?:\.\d+)?)%]")


ALL_COLUMNS: Sequence[str] = [
    "eventType",
    "blockNumber",
    "timestamp",
    "logIndex",
    "transactionHash",
    "origin",
    "sender",
    "recipient",
    "owner",
    "amount0",
    "amount1",
    "sqrtPriceX96_event",
    "tick_event",
    "liquidityAfter_event",
    "tickLower",
    "tickUpper",
    "liquidityDelta",
    "gasUsed",
    "gasPrice",
    "effectiveGasPrice",
    "L_before",
    "sqrt_before",
    "tick_before",
    "x_before",
    "y_before",
    "L_after",
    "sqrt_after",
    "tick_after",
    "x_after",
    "y_after",
    "affectsActive",
    "deltaL_applied",
    "price",
]
DEFAULT_COLUMN_SELECTION: Sequence[str] = [
    "eventType",
    "timestamp",
    "blockNumber",
    "transactionHash",
    "origin",
    "amount0",
    "amount1",
    "price",
]
GAS_COLUMNS = {"gasUsed", "gasPrice", "effectiveGasPrice"}

LOG_STATE_KEY = "last_run_log_text"


def write_config_file(run_dir: Path, config_payload: Dict[str, object]) -> Path:
    config_path = run_dir / "config" / "data_fetch_config.yml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with open(config_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(config_payload, fh, sort_keys=True)
    return config_path


def execute_fetch(
    log_placeholder,
    config_path: Path,
    keep_env: Dict[str, str],
    progress_bar=None,
) -> Tuple[bool, Path]:
    log_dir = config_path.parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "data_fetch.log"
    command = [sys.executable, "data_fetch_subgraph.py"]

    env = os.environ.copy()
    env.update(keep_env)
    env["DATA_FETCH_CONFIG_PATH"] = str(config_path)

    process = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )

    lines: List[str] = []
    suppress_warning_context = False
    last_pct = 0.0
    with open(log_path, "w", encoding="utf-8") as log_file:
        assert process.stdout is not None
        for line in iter(process.stdout.readline, ""):
            if line == "":
                break
            stripped = line.rstrip("\n")
            log_file.write(line)

            if suppress_warning_context:
                if stripped.startswith((" ", "\t", "from ", "  from ")):
                    continue
                suppress_warning_context = False

            if should_suppress_log_line(stripped):
                suppress_warning_context = True
                continue

            lines.append(stripped)
            if len(lines) > 400:
                lines = lines[-400:]
            update_log_output(log_placeholder, lines)
            if progress_bar is not None:
                match = PROGRESS_PATTERN.match(stripped)
                if match:
                    pct = float(match.group(1))
                    last_pct = max(0.0, min(100.0, pct))
                    progress_bar.progress(int(last_pct))
    return_code = process.wait()
    if return_code != 0:
        log_placeholder.error("data_fetch_subgraph.py failed — check log output above.")
        if progress_bar is not None:
            progress_bar.progress(int(last_pct))
    else:
        log_placeholder.success("data_fetch_subgraph.py finished successfully.")
        if progress_bar is not None:
            progress_bar.progress(100)
    return return_code == 0, log_path


def load_combined_frame(pool_lower: str) -> Optional[pd.DataFrame]:
    merged_path = REPO_ROOT / "data" / f"{pool_lower}.pkl"
    if not merged_path.exists():
        return None
    return pd.read_pickle(merged_path)


def render_log_output(container, text: str):
    safe_text = html.escape(text or "Waiting for output…")
    container.markdown(
        f"""
        <div class="log-window"><pre>{safe_text}</pre></div>
        """,
        unsafe_allow_html=True,
    )


def update_log_output(container, lines: List[str]):
    snippet = "\n".join(lines)
    st.session_state[LOG_STATE_KEY] = snippet
    render_log_output(container, snippet)

def apply_layout_styles():
    st.markdown(
        """
        <style>
        :root {
            --surface-base: #080b13;
            --surface-card: #101522;
            --surface-muted: #141a2b;
            --border-soft: rgba(255, 255, 255, 0.04);
            --border-strong: rgba(255, 255, 255, 0.08);
            --text-muted: #97a3c1;
            --accent: #5b8ef1;
        }
        .stApp {
            background: radial-gradient(circle at 20% 20%, #0b1120 0%, #05070d 60%);
            color: #f5f7ff;
        }
        .stApp main .block-container {
            max-width: 1100px;
            padding: 3rem 3rem 3.25rem;
            background: var(--surface-base);
            border-radius: 28px;
            border: 1px solid var(--border-strong);
            color: #f5f7ff;
        }
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4 {
            color: #f7f8ff;
        }
        .stAlert {
            background: rgba(91, 142, 241, 0.08);
            border-radius: 14px;
            border: 1px solid var(--border-soft);
        }
        .stButton>button {
            border-radius: 14px;
            font-weight: 600;
            border: 1px solid rgba(91, 142, 241, 0.5);
            background: linear-gradient(135deg, #5b8ef1, #6987ff);
            color: #fdfdff;
        }
        .hero-card {
            background: var(--surface-card);
            border: 1px solid var(--border-soft);
            border-radius: 24px;
            padding: 1.75rem 2rem;
            margin: 1rem 0 2rem;
        }
        .hero-card h2 {
            margin: 0.2rem 0 0.6rem;
            font-size: 1.8rem;
        }
        .hero-card__eyebrow {
            font-size: 0.78rem;
            letter-spacing: 0.24em;
            text-transform: uppercase;
            color: var(--text-muted);
        }
        .hero-card__pills {
            display: flex;
            flex-wrap: wrap;
            gap: 0.45rem;
            margin-top: 0.9rem;
        }
        .hero-card__pill {
            background: var(--surface-muted);
            border-radius: 999px;
            padding: 0.25rem 0.9rem;
            font-size: 0.82rem;
            color: #cad3f2;
            border: 1px solid var(--border-soft);
        }
        .metric-card {
            background: var(--surface-card);
            border: 1px solid var(--border-soft);
            border-radius: 16px;
            padding: 0.85rem 1rem;
            height: 100%;
        }
        .metric-card span {
            display: block;
            font-size: 1rem;
            color: #f3f4ff;
        }
        .metric-card small {
            color: var(--text-muted);
        }
        div[data-testid="stVerticalBlock"]:has(.section-card__heading) {
            background: var(--surface-card);
            border: 1px solid var(--border-soft);
            border-radius: 22px;
            padding: 1.3rem 1.6rem 1.5rem;
            margin-bottom: 1.4rem;
            box-shadow: 0 12px 35px rgba(3, 8, 20, 0.35);
        }
        .section-card__heading {
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 1rem;
        }
        .section-card__heading h3 {
            margin: 0;
            color: #f5f7ff;
            font-size: 1.02rem;
        }
        .section-card__heading p {
            margin: 0.15rem 0 0;
            color: var(--text-muted);
            font-size: 0.9rem;
        }
        .section-card__icon {
            font-size: 1.4rem;
            line-height: 1;
        }
        .stTextInput input,
        .stTextArea textarea,
        .stNumberInput input,
        .stSelectbox div[data-baseweb="select"] > div,
        .stMultiSelect div[data-baseweb="select"] > div {
            background-color: var(--surface-muted);
            border-radius: 12px;
            border: 1px solid var(--border-soft);
            color: #f2f5ff;
        }
        .stProgress > div > div {
            background: linear-gradient(90deg, #3d6ae6, #64a2ff);
        }
        .stMultiSelect [data-baseweb="tag"] {
            background: rgba(91, 142, 241, 0.18) !important;
            border: 1px solid rgba(91, 142, 241, 0.35) !important;
            color: #d6dcff !important;
        }
        .stMultiSelect [data-baseweb="tag"] span {
            color: inherit !important;
        }
        .log-window {
            background: var(--surface-muted);
            border: 1px solid var(--border-soft);
            border-radius: 14px;
            padding: 1rem;
            font-family: "JetBrains Mono", "SFMono-Regular", Consolas, monospace;
            font-size: 0.85rem;
            line-height: 1.4;
            height: 320px;
            overflow-y: auto;
            white-space: pre-wrap;
        }
        .log-window pre {
            margin: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_hero_section():
    st.markdown(
        """
        <div class="hero-card">
            <div class="hero-card__eyebrow" style="font-size:1.15rem;">   Uniswap v3 data downloader</div>
            <p>
                Collect Swap/Mint/Burn events via subgraph-based harvesting and package the results.
                Configure TheGraph subgraph endpoint and RPC URLs, choose a pool and time window, 
                then let the harvester produce ready-to-analyze datasets complete with checkpoints, logs, and metadata.
                Uses efficient GraphQL queries for event fetching with configurable pagination and batching.
            </p>
            <div class="hero-card__pills">
                <span class="hero-card__pill">1 · Configure subgraph & RPC</span>
                <span class="hero-card__pill">2 · Select pool + timeframe</span>
                <span class="hero-card__pill">3 · Export curated artifacts</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # col1, col2, col3 = st.columns(3)
    # col1.markdown(
    #     "<div class='metric-card'><span>Swap · Mint · Burn</span><small>Event coverage</small></div>",
    #     unsafe_allow_html=True,
    # )
    # col2.markdown(
    #     "<div class='metric-card'><span>Origin & Gas</span><small>Optional metadata fetch</small></div>",
    #     unsafe_allow_html=True,
    # )
    # col3.markdown(
    #     "<div class='metric-card'><span>Chunks + Pickles</span><small>Artifacts per run</small></div>",
    #     unsafe_allow_html=True,
    # )


@contextmanager
def section_card(title: str, subtitle: Optional[str] = None, icon: str = "") -> Iterator[None]:
    container = st.container()
    with container:
        icon_html = f"<span class='section-card__icon'>{icon}</span>" if icon else ""
        container.markdown(
            f"""
            <div class="section-card__heading">
                {icon_html}
                <div>
                    <h3>{title}</h3>
                    {f"<p>{subtitle}</p>" if subtitle else ""}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        yield


def filter_selected_columns(
    df: pd.DataFrame,
    selected_columns: Sequence[str],
) -> Tuple[pd.DataFrame, List[str]]:
    available = [col for col in selected_columns if col in df.columns]
    missing = [col for col in selected_columns if col not in df.columns]
    if not available:
        available = list(df.columns)
    filtered = df[available].copy()
    return filtered, missing


def organize_artifacts(
    run_dir: Path,
    pool_lower: str,
    chunk_dir: Path,
    combined_df: Optional[pd.DataFrame],
) -> Dict[str, Optional[str]]:
    outputs: Dict[str, Optional[str]] = {"raw_chunks": None, "merged_pickle": None}
    if chunk_dir.exists():
        target_chunks = run_dir / "outputs" / "chunks" / pool_lower
        if target_chunks.exists():
            shutil.rmtree(target_chunks)
        target_chunks.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(chunk_dir), target_chunks)
        outputs["raw_chunks"] = str(target_chunks)
    if combined_df is not None:
        merged_dir = run_dir / "outputs" / "merged"
        merged_dir.mkdir(parents=True, exist_ok=True)
        merged_path = merged_dir / f"{pool_lower}.pkl"
        combined_df.to_pickle(merged_path)
        outputs["merged_pickle"] = str(merged_path)
    return outputs


def prepare_preview_dataframe(df: pd.DataFrame, max_rows: int = 200) -> pd.DataFrame:
    """Return a sanitized preview dataframe safe for Streamlit rendering."""
    preview = df.head(max_rows).copy()

    def normalize(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, Integral):
            py_val = int(value)
            if py_val.bit_length() > 60:
                return str(py_val)
            return py_val
        return value

    return preview.applymap(normalize)


def main():
    st.set_page_config(page_title="Uniswap v3 Downloader", layout="centered")
    apply_layout_styles()
    # st.title("Uniswap v3 data downloader")

    default_start = parse_default_timestamp(DEFAULTS["start_ts"], datetime.now(timezone.utc) - pd.Timedelta(days=1))
    default_end = parse_default_timestamp(DEFAULTS["end_ts"], datetime.now(timezone.utc))

    render_hero_section()

    with section_card(
        "Pool selection",
        "Use curated presets with token labels or paste any pool contract address.",
        "🧊",
    ):
        preset_options = ["Custom"] + sorted(POOL_METADATA.keys())
        preset_choice = st.selectbox("Pool preset", preset_options, index=0)
        pool_display = ""
        if preset_choice != "Custom":
            addresses = POOL_METADATA[preset_choice]["addresses"]
            pool_addr_input = st.selectbox("Pool address", addresses, index=0)
            tokens = POOL_METADATA[preset_choice].get("tokens")
            if tokens:
                pool_display = f"{preset_choice} ({'/'.join(tokens)})"
        else:
            pool_addr_input = st.text_input(
                "Pool address",
                value=str(DEFAULTS["pool_addr"]),
                placeholder="0x...",
            )

    with section_card(
        "Time window (UTC)",
        "Timestamps are interpreted as UTC and converted to block ranges automatically.",
        "⏱️",
    ):
        col_start, col_end = st.columns(2)
        with col_start:
            start_date = st.date_input(
                "Start date",
                value=default_start.date(),
                key="start_date_input",
            )
            start_time = st.time_input(
                "Start time",
                value=default_start.time().replace(tzinfo=None),
                key="start_time_input",
            )
        with col_end:
            end_date = st.date_input(
                "End date",
                value=default_end.date(),
                key="end_date_input",
            )
            end_time = st.time_input(
                "End time",
                value=default_end.time().replace(tzinfo=None),
                key="end_time_input",
            )

    with section_card(
        "Columns to include in final dataset",
        "Tailor the merged pickle to match your analytics notebook or downstream pipeline.",
        "🧱",
    ):
        selected_columns = st.multiselect(
            "Pick as many columns as you need",
            options=ALL_COLUMNS,
            default=DEFAULT_COLUMN_SELECTION,
        )
        if set(selected_columns) & GAS_COLUMNS:
            st.warning(
                "Gas-related columns require transaction receipt lookups and can dramatically extend runtime.",
                icon="⚠️",
            )
        else:
            st.info("Gas lookup is skipped unless one of the gas columns is selected.", icon="💡")

    with section_card(
        "Destination",
        "Each run creates a branded folder with configs, logs, chunked raw data, merged dataset, and metadata.",
        "📁",
    ):
        default_dest = REPO_ROOT / "downloads"
        dest_folder = st.text_input(
            "Destination folder",
            value=str(default_dest),
            placeholder="/path/to/output",
            help="A new run directory will be provisioned inside this folder.",
        )

    with section_card(
        "Data sources & advanced options",
        "Configure RPC endpoints, subgraph URL, and fine-tune performance parameters.",
        "⚙️",
    ):
        rpc_text = st.text_area(
            "JSON-RPC endpoints",
            value="\n".join(DEFAULTS["json_rpc_urls"]),
            height=100,
            help="Combine multiple HTTPS RPC URLs for resiliency — failures are retried across providers.",
        )
        
        graph_url = st.text_input(
            "Subgraph endpoint",
            value=str(DEFAULTS.get("graph_url", "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3")),
            placeholder="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3",
            help="TheGraph subgraph URL for fetching Uniswap v3 events.",
        )
        
        st.markdown("---")
        st.markdown("**Advanced tuning**")
        st.info(
            """
            **Performance parameters:** Adjust these to balance speed, memory usage, and API load.
            - **Subgraph page size**: Events per GraphQL query (max 1000). Larger = fewer API calls, more memory.
            - **Chunk events**: Events buffered before disk flush. Larger = fewer writes, more memory.
            - **Batch receipt size**: Receipts fetched in parallel for gas data. Larger = faster, more RPC load.
            - **Legacy options** (chunk size blocks, parallel workers): May be used by alternative fetchers.
            """,
            icon="ℹ️"
        )
        col_adv1, col_adv2, col_adv3 = st.columns(3)
        with col_adv1:
            chunk_size = st.number_input(
                "Chunk size (blocks)",
                min_value=50,
                max_value=10_000,
                step=50,
                value=int(DEFAULTS.get("chunk_size_blocks", 200)),
                help="Max block span per log query before auto-splitting.",
            )
        with col_adv2:
            parallel_workers = st.number_input(
                "Parallel workers",
                min_value=1,
                max_value=64,
                step=1,
                value=int(DEFAULTS.get("parallel_workers", 8)),
                help="Number of threads used for parsing chunks.",
            )
        with col_adv3:
            batch_receipt_size = st.number_input(
                "Batch size for metadata RPC",
                min_value=10,
                max_value=500,
                step=10,
                value=int(DEFAULTS.get("batch_receipt_size", 100)),
                help="Number of receipts/origins fetched per batch.",
            )
        
        col_adv4, col_adv5 = st.columns(2)
        with col_adv4:
            subgraph_page_size = st.number_input(
                "Subgraph page size",
                min_value=1,
                max_value=1000,
                step=50,
                value=int(DEFAULTS.get("subgraph_page_size", 1000)),
                help="Number of events fetched per GraphQL query (max 1000).",
            )
        with col_adv5:
            chunk_events = st.number_input(
                "Chunk events (flush frequency)",
                min_value=100,
                max_value=100_000,
                step=1000,
                value=int(DEFAULTS.get("chunk_events", 10000)),
                help="Number of events to buffer before flushing to disk.",
            )
        st.caption("Increase chunk size cautiously — overly large ranges are auto-split but may stress RPC providers.")

    with section_card(
        "Launch run",
        "Validate the configuration and kick off the harvesting pipeline.",
        "🚀",
    ):
        run_button = st.button("Start download", type="primary", use_container_width=True)

    if not run_button:
        return

    st.session_state[LOG_STATE_KEY] = ""

    rpc_urls = [url.strip() for url in rpc_text.splitlines() if url.strip()]
    errors: List[str] = []
    if not rpc_urls:
        errors.append("Provide at least one JSON-RPC URL.")
    
    # Validate graph_url
    graph_url_clean = graph_url.strip() if graph_url else ""
    if not graph_url_clean:
        errors.append("Provide a valid Graph URL.")
    
    try:
        pool_checksum = Web3.to_checksum_address(pool_addr_input.strip())
    except ValueError:
        errors.append("Enter a valid pool address (0x-prefixed, 42 chars).")
        pool_checksum = DEFAULTS["pool_addr"]
    start_iso, start_dt = isoformat_utc(start_date, start_time)
    end_iso, end_dt = isoformat_utc(end_date, end_time)
    if end_dt <= start_dt:
        errors.append("End time must be after the start time.")
    dest_path = Path(dest_folder).expanduser()
    try:
        dest_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        errors.append(f"Unable to create destination folder: {exc}")
    if errors:
        for err in errors:
            st.error(err)
        return

    pool_lower = pool_checksum.lower()
    slug = slugify(f"{pool_lower}_{start_iso}_{end_iso}")
    final_run_dir = ensure_unique(dest_path / slug)
    tmp_run_dir = Path(tempfile.mkdtemp(prefix=f"{slug}_", dir=str(dest_path)))
    runtime_out_dir = tmp_run_dir / "runtime" / "chunks" / pool_lower
    checkpoint_path = tmp_run_dir / "runtime" / "checkpoint.json"
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    skip_gas_data = not (set(selected_columns) & GAS_COLUMNS)
    compute_price_column = "price" in selected_columns
    
    config_payload = {
        "graph_url": graph_url_clean,
        "json_rpc_urls": rpc_urls,
        "pool_addr": pool_checksum,
        "start_ts": start_iso,
        "end_ts": end_iso,
        "chunk_size_blocks": int(chunk_size),
        "parallel_workers": int(parallel_workers),
        "batch_receipt_size": int(batch_receipt_size),
        "subgraph_page_size": int(subgraph_page_size),
        "chunk_events": int(chunk_events),
        "checkpoint_path": str(checkpoint_path),
        "out_dir": str(runtime_out_dir),
        "skip_gas_data": skip_gas_data,
        "compute_price_column": compute_price_column,
    }

    config_path = write_config_file(tmp_run_dir, config_payload)
    st.info(f"Config saved to {config_path}", icon="📝")
    log_placeholder = st.empty()
    render_log_output(log_placeholder, "")
    with st.spinner("Harvesting on-chain data… this can take a while."):
        progress_bar = st.progress(0)
        success, log_path = execute_fetch(log_placeholder, config_path, {}, progress_bar=progress_bar)

    render_log_output(log_placeholder, st.session_state.get(LOG_STATE_KEY, ""))

    if not success:
        st.error("Run aborted because data_fetch_subgraph.py exited with an error.")
        return

    try:
        combined_df = load_combined_frame(pool_lower)
        if combined_df is None or combined_df.empty:
            st.warning("No combined pickle produced. Check the logs for details.")

        if combined_df is not None:
            combined_df, missing_columns = filter_selected_columns(combined_df, selected_columns)
        else:
            missing_columns = []
        artifacts = organize_artifacts(tmp_run_dir, pool_lower, runtime_out_dir, combined_df)

        metadata = {
            "pool_address": pool_checksum,
            "pool_label": pool_display or preset_choice,
            "start": start_iso,
            "end": end_iso,
            "rpc_urls": rpc_urls,
            "skip_gas_data": skip_gas_data,
            "compute_price_column": compute_price_column,
            "selected_columns": selected_columns,
            "missing_columns": missing_columns,
            "total_rows": int(len(combined_df)) if combined_df is not None else 0,
            "artifacts": {
                **artifacts,
                "selected_dataset": artifacts.get("merged_pickle"),
            },
            "logs": str(log_path),
            "output_folder": str(final_run_dir),
        }
        metadata_path = tmp_run_dir / "metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as fh:
            json.dump(metadata, fh, indent=2)

        shutil.move(str(tmp_run_dir), final_run_dir)
        st.success(f"Run folder ready → {final_run_dir}")
        if combined_df is not None and not combined_df.empty:
            preview_df = prepare_preview_dataframe(combined_df)
            shown_rows = len(preview_df)
            total_rows = len(combined_df)
            st.markdown("#### Downloaded dataset preview")
            st.dataframe(
                preview_df,
                use_container_width=True,
                height=min(520, 90 + shown_rows * 26),
            )
            st.caption(f"Showing first {shown_rows:,} of {total_rows:,} rows.")
    except Exception as exc:
        st.error(f"Run packaging failed: {exc}")
        st.info(f"Temporary results kept at {tmp_run_dir} for debugging.")
        return


if __name__ == "__main__":
    main()
