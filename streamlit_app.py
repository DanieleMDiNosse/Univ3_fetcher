import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import streamlit as st
import yaml
from web3 import Web3

from utils import POOL_METADATA

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "data_fetch_config.yml"

DEFAULTS: Dict[str, object] = {
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
}

if DEFAULT_CONFIG_PATH.exists():
    try:
        with open(DEFAULT_CONFIG_PATH, "r", encoding="utf-8") as fh:
            from_file = yaml.safe_load(fh) or {}
        DEFAULTS.update({k: from_file.get(k, DEFAULTS[k]) for k in DEFAULTS})
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
    command = [sys.executable, "data_fetch.py"]

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
            snippet = "\n".join(lines)
            log_placeholder.code(snippet or "Waiting for output…", language="text")
            if progress_bar is not None:
                match = PROGRESS_PATTERN.match(stripped)
                if match:
                    pct = float(match.group(1))
                    last_pct = max(0.0, min(100.0, pct))
                    progress_bar.progress(
                        int(last_pct),
                        text=f"Downloading on-chain data… {last_pct:.1f}%",
                    )
    return_code = process.wait()
    if return_code != 0:
        log_placeholder.error("data_fetch.py failed — check log output above.")
        if progress_bar is not None:
            progress_bar.progress(int(last_pct), text="Run failed — see logs.")
    else:
        log_placeholder.success("data_fetch.py finished successfully.")
        if progress_bar is not None:
            progress_bar.progress(100, text="Data download complete.")
    return return_code == 0, log_path


def load_combined_frame(pool_lower: str) -> Optional[pd.DataFrame]:
    merged_path = REPO_ROOT / "data" / f"{pool_lower}.pkl"
    if not merged_path.exists():
        return None
    return pd.read_pickle(merged_path)

def apply_layout_styles():
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at top, #030712 0%, #101828 55%, #050b18 100%);
            color: #e2e8f0;
        }
        .stApp main .block-container {
            max-width: 1200px;
            padding: 2.5rem 3rem 3rem 3rem;
            background: #0f172a;
            border-radius: 26px;
            border: 1px solid rgba(59, 130, 246, 0.2);
            box-shadow: 0 40px 80px rgba(2, 6, 23, 0.55);
            color: #e2e8f0;
        }
        .stMarkdown h1, .stMarkdown h2, .stMarkdown h3, .stMarkdown h4 {
            color: #f0f9ff;
        }
        .stAlert {
            background: rgba(59, 130, 246, 0.15);
            border-radius: 16px;
        }
        .stButton>button {
            border-radius: 999px;
            font-weight: 600;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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


def main():
    st.set_page_config(page_title="Uniswap v3 Downloader", layout="centered")
    apply_layout_styles()
    st.title("Uniswap v3 data downloader")
    st.caption("Collect Swap/Mint/Burn events via the existing harvesting script and package the results.")

    default_start = parse_default_timestamp(DEFAULTS["start_ts"], datetime.now(timezone.utc) - pd.Timedelta(days=1))
    default_end = parse_default_timestamp(DEFAULTS["end_ts"], datetime.now(timezone.utc))

    st.subheader("JSON-RPC endpoints")
    rpc_text = st.text_area(
        "Paste one or more RPC URLs (one per line)",
        value="\n".join(DEFAULTS["json_rpc_urls"]),
        height=120,
    )

    st.subheader("Pool selection")
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

    st.subheader("Time window (UTC)")
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

    st.subheader("Columns to include in final dataset")
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

    st.subheader("Destination")
    default_dest = REPO_ROOT / "downloads"
    dest_folder = st.text_input(
        "Destination folder (a new run directory will be created inside this path)",
        value=str(default_dest),
        placeholder="/path/to/output",
    )

    st.subheader("Advanced parameters")
    col_adv1, col_adv2, col_adv3 = st.columns(3)
    with col_adv1:
        chunk_size = st.number_input(
            "Chunk size (blocks)",
            min_value=50,
            max_value=10_000,
            step=50,
            value=int(DEFAULTS["chunk_size_blocks"]),
        )
    with col_adv2:
        parallel_workers = st.number_input(
            "Parallel workers",
            min_value=1,
            max_value=64,
            step=1,
            value=int(DEFAULTS["parallel_workers"]),
        )
    with col_adv3:
        batch_receipt_size = st.number_input(
            "Batch size for metadata RPC",
            min_value=10,
            max_value=500,
            step=10,
            value=int(DEFAULTS["batch_receipt_size"]),
        )

    run_button = st.button("Start download", type="primary")
    log_placeholder = st.empty()

    if not run_button:
        return

    rpc_urls = [url.strip() for url in rpc_text.splitlines() if url.strip()]
    errors: List[str] = []
    if not rpc_urls:
        errors.append("Provide at least one JSON-RPC URL.")
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
        "json_rpc_urls": rpc_urls,
        "pool_addr": pool_checksum,
        "start_ts": start_iso,
        "end_ts": end_iso,
        "chunk_size_blocks": int(chunk_size),
        "parallel_workers": int(parallel_workers),
        "batch_receipt_size": int(batch_receipt_size),
        "checkpoint_path": str(checkpoint_path),
        "out_dir": str(runtime_out_dir),
        "skip_gas_data": skip_gas_data,
        "compute_price_column": compute_price_column,
    }

    config_path = write_config_file(tmp_run_dir, config_payload)
    st.info(f"Config saved to {config_path}", icon="📝")
    progress_bar = st.progress(0, text="Initializing download…")

    with st.spinner("Harvesting on-chain data… this can take a while."):
        success, log_path = execute_fetch(log_placeholder, config_path, {}, progress_bar=progress_bar)

    if not success:
        st.error("Run aborted because data_fetch.py exited with an error.")
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
        # st.code(str(final_run_dir), language="text")
    except Exception as exc:
        st.error(f"Run packaging failed: {exc}")
        st.info(f"Temporary results kept at {tmp_run_dir} for debugging.")
        return


if __name__ == "__main__":
    main()
