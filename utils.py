import os
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from scipy import stats
from scipy.stats import norm
from tqdm import tqdm
from web3 import Web3

DEFAULT_COLUMN_ALIAS_GROUPS: List[Sequence[str]] = [
    {"block_number", "blockNumber"},
    {"log_index", "logIndex"},
    {"event", "Event", "eventType", "event_type"},
    {"tx_hash", "txHash", "transactionHash", "transaction_hash", "hash"},
    {"sqrtPriceX96", "sqrt_price_x96", "sqrtPriceX96_event"},
    {"amount", "amount0", "amount1", "amountUSD"},
    {"origin"},
    {"recipient"},
    {"sender"},
    {"tick", "tick_event"},
    {"timestamp", "datetime"},
    {"token0"},
    {"token1"},
    {"pool_address", "poolAddress"},
    {"gas_price", "gasPrice"},
    {"gas_used", "gasUsed"},
    {"month"},
    {"owner"},
    {"tickLower", "tick_lower"},
    {"tickUpper", "tick_upper"},
]

_COLUMN_ALIAS_LOOKUP: Dict[str, List[str]] = {}
for group in DEFAULT_COLUMN_ALIAS_GROUPS:
    union = list(dict.fromkeys([*(group), *[name.lower() for name in group]]))
    for alias in union:
        _COLUMN_ALIAS_LOOKUP[alias.lower()] = union


def pick_column(
    df: pd.DataFrame, *candidates: str, required: bool = True
) -> Optional[str]:
    """
    Resolve a column name from a set of candidates, supporting case-insensitive
    matches and a configurable alias table.
    """
    extended: List[str] = []
    for cand in candidates:
        if cand not in extended:
            extended.append(cand)
        alias_group = _COLUMN_ALIAS_LOOKUP.get(cand.lower())
        if alias_group:
            for alias in alias_group:
                if alias not in extended:
                    extended.append(alias)

    for cand in extended:
        if cand in df.columns:
            return cand

    lower_map = {col.lower(): col for col in df.columns}
    for cand in extended:
        col = lower_map.get(cand.lower())
        if col is not None:
            return col

    if required:
        raise KeyError(f"None of the columns {candidates} found in dataframe.")
    return None


def read_dataframe(path: Path) -> pd.DataFrame:
    """Load a DataFrame from path supporting pickle, parquet, and CSV formats."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")
    suffix = path.suffix.lower()
    suffixes = [s.lower() for s in path.suffixes]
    if suffix in {".pkl", ".pickle"}:
        return pd.read_pickle(path)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    if ".csv" in suffixes or suffix in {".csv", ".txt"} or ".csv" in suffix:
        return pd.read_csv(path)
    if suffix in {".gz", ".bz2"} and any(s in {".csv", ".txt"} for s in suffixes[:-1]):
        return pd.read_csv(path)
    raise ValueError(f"Unsupported dataset format for {path}")

# ---------------------------------------------------------------------
# Pool metadata helpers
# ---------------------------------------------------------------------

POOL_METADATA: Dict[str, Dict[str, object]] = {
    "USDC-WETH": {
        "addresses": [
            "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
            "0xe0554a476a092703abdb3ef35c80e0d76d32939f",
            "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8",
        ],
        "tokens": ("usdc", "weth"),
        "decimals": (6, 18),
    },
    "DAI-USDC": {
        "addresses": [
            "0x5777d92f208679db4b9778590fa3cab3ac9e2168",
            "0x6c6bc977e13df9b0de53b251522280bb72383700",
        ],
        "tokens": ("dai", "usdc"),
        "decimals": (18, 6),
    },
    "LINK-WETH": {
        "addresses": [
            "0xa6cc3c2531fdaa6ae1a3ca84c2855806728693e8",
            "0x3a0f221ea8b150f3d3d27de8928851ab5264bb65",
            "0x5d4f3c6fa16908609bac31ff148bd002aa6b8c83",
        ],
        "tokens": ("link", "weth"),
        "decimals": (18, 18),
    },
    "MNT-WETH": {
        "addresses": ["0xf4c5e0f4590b6679b3030d29a84857f226087fef"],
        "tokens": ("mnt", "weth"),
        "decimals": (18, 18),
    },
    "UNI-WETH": {
        "addresses": [
            "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
            "0xfaa318479b7755b2dbfdd34dc306cb28b420ad12",
            "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
        ],
        "tokens": ("uni", "weth"),
        "decimals": (18, 18),
    },
    "USDC-USDT": {
        "addresses": [
            "0x3416cf6c708da44db2624d63ea0aaef7113527c6",
            "0x7858e59e0c01ea06df3af3d20ac7b0003275d4bf",
        ],
        "tokens": ("usdc", "usdt"),
        "decimals": (6, 6),
    },
    "USDe-USDT": {
        "addresses": ["0x435664008f38b0650fbc1c9fc971d0a3bc2f1e47"],
        "tokens": ("usde", "usdt"),
        "decimals": (18, 6),
    },
    "WBTC-LBTC": {
        "addresses": ["0x87428a53e14d24ab19c6ca4939b4df93b8996ca9"],
        "tokens": ("wbtc", "lbtc"),
        "decimals": (8, 8),
    },
    "WBTC-USDC": {
        "addresses": [
            "0x9a772018fbd77fcd2d25657e5c547baff3fd7d16",
            "0x99ac8ca7087fa4a2a1fb6357269965a2014abc35",
        ],
        "tokens": ("wbtc", "usdc"),
        "decimals": (8, 6),
    },
    "WBTC-USDT": {
        "addresses": ["0x9db9e0e53058c89e5b94e29621a205198648425b"],
        "tokens": ("wbtc", "usdt"),
        "decimals": (8, 6),
    },
    "WBTC-WETH": {
        "addresses": [
            "0x4585fe77225b41b697c938b018e2ac67ac5a20c0",
            "0xcbcdf9626bc03e24f779434178a73a0b4bad62ed",
            "0xe6ff8b9a37b0fab776134636d9981aa778c4e718",
        ],
        "tokens": ("wbtc", "weth"),
        "decimals": (8, 18),
    },
    "WETH-USDT": {
        "addresses": [
            "0x11b815efb8f581194ae79006d24e0d814b7697f6",
            "0x4e68ccd3e89f51c3074ca5072bbac773960dfa36",
            "0xc5af84701f98fa483ece78af83f11b6c38aca71d",
            "0xc7bbec68d12a0d1830360f8ec58fa599ba1b0e9b",
        ],
        "tokens": ("weth", "usdt"),
        "decimals": (18, 6),
    },
    "WETH-weETH": {
        "addresses": [
            "0x202a6012894ae5c288ea824cbc8a9bfb26a49b93",
            "0x7a415b19932c0105c82fdb6b720bb01b0cc2cae3",
        ],
        "tokens": ("weth", "weeth"),
        "decimals": (18, 18),
    },
    "wstETH-WETH": {
        "addresses": [
            "0x109830a1aaad605bbf02a9dfa7b0b92ec2fb7daa",
            "0xd340b57aacdd10f96fc1cf10e15921936f41e29c",
        ],
        "tokens": ("wsteth", "weth"),
        "decimals": (18, 18),
    },
}

TOKEN_DECIMALS = {
    "usdc": 6,
    "usdt": 6,
    "dai": 18,
    "weth": 18,
    "link": 18,
    "mnt": 18,
    "uni": 18,
    "usde": 18,
    "wbtc": 8,
    "lbtc": 8,
    "weeth": 18,
    "wsteth": 18,
}

ADDRESS_TO_PAIR = {}
for pair_name, meta in POOL_METADATA.items():
    for addr in meta["addresses"]:
        ADDRESS_TO_PAIR[addr.lower()] = (pair_name, meta["decimals"])


def normalize_address(address: Optional[str]) -> Optional[str]:
    if address is None:
        return None
    return address.strip().lower()


def find_pair_name(pool_address: str) -> Optional[str]:
    normalized = normalize_address(pool_address)
    for name, meta in POOL_METADATA.items():
        addresses = [normalize_address(addr) for addr in meta["addresses"]]
        if normalized in addresses:
            return name
    return None


def token_decimals_for_pair(pair_name: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not pair_name:
        return (None, None)
    meta = POOL_METADATA.get(pair_name)
    if not meta:
        return (None, None)
    token0, token1 = meta.get("tokens", (None, None))
    if token0 is None or token1 is None:
        return (None, None)
    return (TOKEN_DECIMALS.get(token0), TOKEN_DECIMALS.get(token1))


def load_pool_events(pool_address: str, manual_path: Optional[Path] = None, data_dir: Path = Path("data")) -> pd.DataFrame:
    pool_address = normalize_address(pool_address)
    path = Path(manual_path) if manual_path else data_dir / f"{pool_address}.pkl"
    if path.exists():
        df = read_dataframe(path).copy()
    else:
        candidate_dir = data_dir / f"univ3_{pool_address}"
        processed = sorted(candidate_dir.glob("uniswap-v3-*_processed.pkl"))
        if not processed:
            raise FileNotFoundError(f"Unable to locate events dataset for pool {pool_address}.")
        df = read_dataframe(processed[0]).copy()
    df["pool_address"] = pool_address
    df["pool_address"] = df["pool_address"].astype(str).str.lower()
    return df


def load_pool_mev(pool_address: str, manual_path: Optional[Path], base_dir: Path, filename: str) -> pd.DataFrame:
    pool_address = normalize_address(pool_address)
    path = Path(manual_path) if manual_path else base_dir / f"univ3_{pool_address}" / filename
    if not path.exists():
        return pd.DataFrame()
    df = read_dataframe(path).copy()
    df["pool_address"] = pool_address
    return df

def data_loader(pool_address, reverse_token_order=True, rpc_url='https://eth.llamarpc.com/sk_llama_f4a74110d652ea9d6ed4eb7491d7bb7d'):
    token0, decimals0, token1, decimals1, fee_rate = get_token_names_and_fee(pool_address, rpc_url='https://eth.llamarpc.com/sk_llama_f4a74110d652ea9d6ed4eb7491d7bb7d', verbose=True)
    print(f'Token0: {token0}, Decimals0: {decimals0}, Token1: {token1}, Decimals1: {decimals1}, Fee: {fee_rate}, Reverse: {reverse_token_order}')
    paths_processed_data = f'data/{pool_address}/uniswap-v3-{token0}_{token1}_{fee_rate}_processed_.pkl'
    all_exist = os.path.exists(paths_processed_data)
    if all_exist:
        print('All Uniswap processed data files exist. Loading them...')
        all_events = pd.read_pickle(f'data/{pool_address}/uniswap-v3-{token0}_{token1}_{fee_rate}_processed.pkl').sort_values(['block_number', 'log_index'])
    else:
        print('Some Uniswap processed data files do not exist. Running the processing...')
        with open(f'data/{pool_address}/{token0}_{token1}_{fee_rate}.pickle', 'rb') as f:
            all_events = pickle.load(f)
        if all_events.index.name == 'timestamp':  # Check if 'timestamp' is the index name
            all_events['timestamp'] = all_events.index  # Create a new column from the index
            all_events['timestamp'] = pd.to_datetime(all_events['timestamp']).dt.tz_localize('UTC').dt.tz_convert('Europe/Rome')  # Convert to datetime
        elif 'timestamp' in all_events.columns:  # Check if 'timestamp' is a column
            all_events['timestamp'] = pd.to_datetime(all_events['timestamp']).dt.tz_localize('UTC').dt.tz_convert('Europe/Rome')  # Convert to datetime
        else:
            print("No 'timestamp' column or index found in this DataFrame.")
            
        print(f'Original lenght : {len(all_events)}')
        try:
            all_events['price'] = sqrtPriceX96_to_price(all_events['sqrt_price_x96'].astype(float), decimals0, decimals1, reverse=reverse_token_order)
        except Exception as e:
            print(f'Error: {e}')
            print('Probably the sqrt_price_x96 column does not exist since the file has been already preprocessed.')
        
        # Add sender and receiver columns if the file exists
        try:
            senders_receivers = pd.read_pickle(f'data/{pool_address}/results_tmp_fetching_wallets.pkl')
            senders_receivers = pd.DataFrame(senders_receivers, columns=['tx_hash', 'sender', 'receiver']).drop_duplicates(subset='tx_hash', keep='first')
            all_events = all_events.merge(senders_receivers, on='tx_hash', how='left')
        except Exception as e:
            print(f'Error: {e}')
            print('Skipping senders and receivers loading...')

        all_events['amount0'] = all_events[f'amount0'].astype(float)
        all_events['amount1'] = all_events[f'amount1'].astype(float)
        all_events['liquidity'] = all_events['liquidity'].astype(float)
        all_events['log_index'] = all_events['log_index'].astype(int)
        all_events['block_number'] = all_events['block_number'].astype(int)
        all_events['tick'] = all_events['tick'].apply(lambda x: int(x) if not pd.isna(x) else x)
        all_events['tick_lower'] = all_events['tick_lower'].apply(lambda x: int(x) if not pd.isna(x) else x)
        all_events['tick_upper'] = all_events['tick_upper'].apply(lambda x: int(x) if not pd.isna(x) else x)
        
        print(f'Renaming amount0 and amount1 columns to {token0}_amount0 and {token1}_amount1...')
        all_events = all_events.rename(columns={'amount0': f'{token0}_amount0', 'amount1': f'{token1}_amount1'})
        print('Renaming amount column to liquidity_added_removed...')
        all_events = all_events.rename(columns={'amount': 'liquidity_added_removed'})
        print('Renaming liquidity column to virtual_liquidity...')
        all_events = all_events.rename(columns={'liquidity': 'virtual_liquidity'})
        print('Setting timestamp to datetime format')
        all_events['timestamp'] = pd.to_datetime(all_events['timestamp'])
        print('Dropping columns with all missing values...')
        all_events = all_events.dropna(axis=1, how='all')
        all_events = all_events.reset_index(drop=True)

        try:
            with open(f'data/{pool_address}/gas_fees.pkl', 'rb') as f:
                gas_fees = pickle.load(f)
            print("Loaded gas fees from file")
            gas_fees.rename(columns={'event': 'Event'}, inplace=True)
            all_events = all_events.merge(gas_fees, how='left', on=['tx_hash', 'Event'])
            all_events['total_usd_gas_fee'] = all_events['total_gas_fee'] * all_events['price']
        except Exception as e:
            print(f'Error: {e}')
            print('Skipping gas fees loading...')
        print(f'New lenght: {len(all_events)}')

    print('Sorting first by block_number and then by log_index...')
    all_events = all_events.sort_values(['block_number', 'log_index'])
    all_events = all_events.drop_duplicates()
    print('Creating events dataframes...')
    mints = all_events[all_events['Event'] == 'Mint'].copy()
    burns = all_events[all_events['Event'] == 'Burn'].copy()
    swaps = all_events[all_events['Event'].isin(['Swap_X2Y', 'Swap_Y2X'])].copy()

    all_events.to_pickle(f'data/{pool_address}/uniswap-v3-{token0}_{token1}_{fee_rate}_processed.pkl')
    print('Data processing completed.')

    return all_events, mints, burns, swaps

def get_token_names_and_fee(pool_address, rpc_url='https://mainnet.infura.io/v3/5f38fb376e0548c8a828112252a6a588', verbose=False):
    if verbose: print('Fetching token names and fee...')
    # Initialize Web3
    web3 = Web3(Web3.HTTPProvider(rpc_url))

    # Convert to checksum address
    pool_address = Web3.to_checksum_address(pool_address)

    # Uniswap v3 pool contract ABI (including `token0`, `token1`, and `fee`)
    pool_abi = [
        {
            "constant": True,
            "inputs": [],
            "name": "token0",
            "outputs": [{"name": "", "type": "address"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [],
            "name": "token1",
            "outputs": [{"name": "", "type": "address"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [],
            "name": "fee",
            "outputs": [{"name": "", "type": "uint24"}],
            "type": "function",
        },
    ]

    # Create the contract instance
    pool_contract = web3.eth.contract(address=pool_address, abi=pool_abi)

    # Query token0, token1, and fee
    token0_address = pool_contract.functions.token0().call()
    token1_address = pool_contract.functions.token1().call()
    fee_rate_bps = pool_contract.functions.fee().call()

    # ERC20 token contract ABI for symbol and decimals
    erc20_abi = [
        {
            "constant": True,
            "inputs": [],
            "name": "symbol",
            "outputs": [{"name": "", "type": "string"}],
            "type": "function",
        },
        {
            "constant": True,
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "type": "function",
        },
    ]

    # Get token0 and token1 symbols and decimals
    token0_contract = web3.eth.contract(address=token0_address, abi=erc20_abi)
    token0_symbol = token0_contract.functions.symbol().call()
    token0_decimals = token0_contract.functions.decimals().call()

    token1_contract = web3.eth.contract(address=token1_address, abi=erc20_abi)
    token1_symbol = token1_contract.functions.symbol().call()
    token1_decimals = token1_contract.functions.decimals().call()

    # Convert fee from bps to percentage
    fee_rate_percent = str(fee_rate_bps / 100)

    return token0_symbol.lower(), int(token0_decimals), token1_symbol.lower(), int(token1_decimals), fee_rate_percent.split('.')[0]

def sqrtPriceX96_to_price(sqrtPrice_X96, decimals0, decimals1, reverse=True):
    '''Function to convert sqrt(price) to price'''
    price = (sqrtPrice_X96/2**96)**2 / (10 ** (decimals1 - decimals0))
    if reverse:
        return 1 / price
    else:
        return price
    
def is_contract(address):
    code = web3.eth.get_code(Web3.to_checksum_address(address))
    print(code)
    return code != b''

def rescale_data(matrix, interval=(-1, 1)):
    """
    Scales each row of the input NxM numpy array to the specified range [a, b].

    Parameters:
    matrix (numpy.ndarray): The NxM array to scale.
    interval (tuple): A tuple (a, b) specifying the target interval. Default is (-1, 1).

    Returns:
    numpy.ndarray: The scaled array with rows in the specified range [a, b].
    """
    a, b = interval  # Unpack the target interval
    row_min = matrix.min(axis=1, keepdims=True)  # Minimum of each row
    row_max = matrix.max(axis=1, keepdims=True)  # Maximum of each row
    # print(row_min, row_max)
    
    # Handle the case where row_min equals row_max to avoid division by zero
    scaled_matrix = np.where(
        row_max != row_min,
        (b - a) * (matrix - row_min) / (row_max - row_min) + a,
        (a + b) / 2  # If all elements in a row are the same, scale to the midpoint of [a, b]
    )
    return scaled_matrix

def process_montlhy_data(token0, token1, monthly_data_folder, chain='arbitrum'):
    all_paths = os.listdir(monthly_data_folder)
    all_months = set([path.split('_')[1].split('.')[0] for path in all_paths])

    swaps = pd.DataFrame()
    for month in tqdm(all_months, desc=f'Loading data swap for {token0}-{token1}'):
        path = f'{monthly_data_folder}/swaps_{month}.pkl'
        with open(path, 'rb') as f:
            data = pickle.load(f)
            data = data[(data['token0'] == token0) & (data['token1'] == token1)]
            swaps = pd.concat([swaps, data])

    columns_to_float = ['amount0', 'amount1', 'amountUSD']
    columns_to_int = ['block_number', 'logIndex', 'tick', 'gas_price', 'gas_used']
    columns_to_drop = ['month', 'timestamp']

    swaps = swaps.sort_values('timestamp')
    swaps = swaps.reset_index(drop=True)
    swaps = swaps.drop(columns=columns_to_drop)
    swaps[columns_to_float] = swaps[columns_to_float].astype(float)
    swaps[columns_to_int] = swaps[columns_to_int].astype(int)
    swaps.to_parquet(f'data/{chain}/{token0}_{token1}_swaps.parquet')
    print(f'Swaps data for {token0}-{token1} saved at data/{chain}/{token0}_{token1}_swaps.parquet')

    mints = pd.DataFrame()
    for month in tqdm(all_months, desc=f'Loading mint data for {token0}-{token1}'):
        path = f'{monthly_data_folder}/mints_{month}.pkl'
        with open(path, 'rb') as f:
            data = pickle.load(f)
            data = data[(data['token0'] == token0) & (data['token1'] == token1)]
            mints = pd.concat([mints, data])

    columns_to_float = ['amount', 'amount0', 'amount1', 'amountUSD']
    columns_to_int = ['block_number', 'logIndex', 'tickLower', 'tickUpper', 'gas_price', 'gas_used']
    columns_to_drop = ['month', 'timestamp']

    mints = mints.sort_values('timestamp')
    mints = mints.reset_index(drop=True)
    mints = mints.drop(columns=columns_to_drop)
    mints[columns_to_float] = mints[columns_to_float].astype(float)
    mints[columns_to_int] = mints[columns_to_int].astype(int)
    mints.to_parquet(f'data/{chain}/{token0}_{token1}_mints.parquet')
    print(f'Mints data for {token0}-{token1} saved at data/{chain}/{token0}_{token1}_mints.parquet')

    burns = pd.DataFrame()
    for month in tqdm(all_months, desc=f'Loading data burn for {token0}-{token1}'):
        path = f'{monthly_data_folder}/burns_{month}.pkl'
        with open(path, 'rb') as f:
            data = pickle.load(f)
            data = data[(data['token0'] == token0) & (data['token1'] == token1)]
            burns = pd.concat([burns, data])

    burns = burns.sort_values('timestamp')
    burns = burns.reset_index(drop=True)
    burns = burns.drop(columns=columns_to_drop)
    burns[columns_to_float] = burns[columns_to_float].astype(float)
    burns[columns_to_int] = burns[columns_to_int].astype(int)
    burns.to_parquet(f'data/{chain}/{token0}_{token1}_burns.parquet')
    print(f'Burns data for {token0}-{token1} saved at data/{chain}/{token0}_{token1}_burns.parquet')

    return swaps, mints, burns

def fit_Intertimes_distributions(data, pool_address, quantity):
    data = data.sort_values(by=['block_number', 'logIndex'])
    # data['event_time'] = np.arange(len(data))
    # Fit interarrival times with multiple distributions
    fig, ax = plt.subplots(2, 2, figsize=(16, 10), tight_layout=True)
    interarrival_times = {}
    fit_results = {}

    for i, event in enumerate([['mint'], ['burn'], ['swap_x2y'], ['swap_y2x']]):
        df = data[data['event'].isin(event)]
        timestamps = df[quantity]
        inter_times = timestamps.diff().dropna()
        inter_times = inter_times[inter_times > 0]
        # if 'swap' in event:
        #     event[0] = 'swap'
        interarrival_times[event[0]] = inter_times
        
        # Prepare x for plotting PDFs
        x = np.linspace(min(inter_times), max(inter_times), 1000)

        # Initialize fit results for the event
        fit_results[event[0]] = {}

        # Fit and evaluate each distribution
        distributions = {
            'lognormal': stats.lognorm,
            'exponential': stats.expon,
            'weibull': stats.weibull_min,
            'pareto': stats.pareto,
            'gamma': stats.gamma,
            #'powerlaw': stats.powerlaw,
            #'generalized_pareto': stats.genpareto,
        }
        num_params = {
            'lognormal': 3,
            'exponential': 2,
            'weibull': 3,
            'pareto': 2,
            'gamma': 2,
            #'powerlaw': 2,
            #'generalized_pareto': 3
        }

        best_loglikelihood = -float('inf')
        best_dist_name = None
        best_pdf = None

        for dist_name, dist in distributions.items():
            if dist_name == 'lognormal' or dist_name == 'weibull':  # Use floc=0 for specific distributions
                params = dist.fit(inter_times, floc=0)
            else:
                params = dist.fit(inter_times)
            
            # Calculate PDF and log-likelihood
            pdf = dist.pdf(x, *params)
            log_like = log_likelihood(inter_times, dist.pdf, *params)
            aic, bic = calculate_aic_bic(log_like, num_params[dist_name], len(inter_times))
            
            # Store results
            fit_results[event[0]][dist_name] = {'log_likelihood': log_like, 'aic': aic, 'bic': bic}

            # Track the best distribution based on BIC
            if log_like > best_loglikelihood:
                best_loglikelihood = log_like
                best_dist_name = dist_name
                best_pdf = pdf

        # Plot histogram and the best PDF
        ax[i//2, i%2].hist(inter_times, bins='sturges', density=True, stacked=True, label='Empirical PDF', alpha=0.7)
        ax[i//2, i%2].plot(x, best_pdf, label=f'Best Fit: {best_dist_name.capitalize()}', lw=1.5, alpha=0.6, color='red')
        ax[i//2, i%2].set_title(f'{event[0]} {quantity} distribution')
        ax[i//2, i%2].set_yscale('log')
        # ax[i//2, i%2].set_xscale('log')
        # ax[i//2, i%2].set_ylim(1e-7, 1)
        ax[i//2, i%2].legend()

    # Finalize and save the plot
    # plt.suptitle(f'Empirical distribution of interarrival times')
    plt.savefig(f'images/arbitrum/{quantity}_fits.pdf', dpi=300, bbox_inches='tight')
    plt.show()

    # Create a MultiIndex DataFrame for fit results
    columns = pd.MultiIndex.from_product(
        [fit_results.keys(), ['log_likelihood', 'aic', 'bic']],
        names=["event", "Metric"]
    )
    data = []
    for dist in distributions.keys():
        row = []
        for event in fit_results:
            row.extend([
                fit_results[event][dist]['log_likelihood'],
                fit_results[event][dist]['aic'],
                fit_results[event][dist]['bic']
            ])
        data.append(row)

    df = pd.DataFrame(data, columns=columns, index=distributions.keys())
    print(df)

def complete_autocorr(data, folder_data, folder_image, max_lag, n_steps_boot_idd, n_steps_boot_block, block_size, name, log=True, only_iid=True, reload=False):
    os.makedirs(folder_data, exist_ok=True)
    os.makedirs(folder_image, exist_ok=True)

    if only_iid:
        actual_acf, bootstrap_array = bootstrap_iid_autocorr(data, n_steps_boot_idd, max_lag)

        results = [actual_acf, bootstrap_array]
        with open(f'{folder_data}/{name}_bootstrap_iid_autocorr.pkl', 'wb') as f:
            pickle.dump(results, f)
    else:
        actual_acf, bootstrap_array = bootstrap_iid_autocorr(data, n_steps_boot_idd, max_lag)

        with open(f'{folder_data}/{name}_bootstrap_iid_autocorr.pkl', 'wb') as f:
            pickle.dump(bootstrap_array, f)

        blocks = bootstrap_blocks(data, block_size=block_size, n_bootstrap=n_steps_boot_idd, method='cbb')
        print('Computing ACF and CI...')
        actual_acf, block_bootstrapped_acf_array = bootstrap_dependent_autocorr(data, n_steps_boot_block, max_lag, blocks)

        results = [actual_acf, block_bootstrapped_acf_array]
        with open(f'{folder_data}/acf_{name}_{max_lag}_{n_steps_boot_block}.pkl', 'wb') as f:
            pickle.dump(results, f)
        
        lower_bounds, upper_bounds = bca_confint_vectorized(np.array(actual_acf)[1:], np.array(block_bootstrapped_acf_array)[:, 1:], alpha=0.05)

    lower_bound_iid = np.percentile(bootstrap_array, 2.5, axis=0)
    upper_bound_iid = np.percentile(bootstrap_array, 97.5, axis=0)

    plt.figure(figsize=(12, 6))
    plt.plot(range(1, max_lag + 1), actual_acf[1:], label='Empirical ACF', color='blue', alpha=0.7)
    if not only_iid: plt.fill_between(range(len(actual_acf[1:])), lower_bounds, upper_bounds, color='gray', alpha=0.5, label='95% BCa CI')
    plt.fill_between(range(len(actual_acf)-1), lower_bound_iid[1:], upper_bound_iid[1:], color='red', alpha=0.5, label='White Noise Region')
    # plt.plot(np.arange(1, nlags+1), 10**intercept*np.arange(1, nlags+1)**slope, linestyle='--', color='black', label=fr'Power Law Fit, $\beta=${np.abs(slope):.2f}')
    plt.title(f'Autocorrelation of {name}')
    plt.xlabel('Lag')
    plt.ylabel('Autocorrelation')
    if log:
        plt.yscale('log')
        plt.xscale('log')
    # plt.xlim(-3, max_lag)
    # plt.ylim(1e-3, 0.15)
    plt.grid(True)
    plt.legend()
    plt.savefig(f"{folder_image}/acf_{name}.pdf", format="pdf", bbox_inches='tight', dpi=300)

    if only_iid: 
        return actual_acf,  [lower_bound_iid, upper_bound_iid]
    else:
        actual_acf, [lower_bounds, upper_bounds], [lower_bound_iid, upper_bound_iid]

def tick_to_price(tick, decimals0, decimals1, reverse_token_order):
    '''Function to convert tick to price'''
    raw_price = 1.0001 ** tick

    # for usdc/weth pool, token0 is usdc and token1 is weth. decimals0 is 6 and decimals1 is 18. reverse_token_order is True
    if reverse_token_order:
        return (1) / raw_price / (10 ** (decimals0 - decimals1))
    else:
        return raw_price / (10 ** (decimals1 - decimals0))

def plot_inter_event_times_vs_std(data, event, window=100):
    # Select only the event of interest
    data = data[data['event'].isin(event)]

    volume = None
    if event[0] == 'swap_x2y':
        volume = np.log(data['amount0'])
    elif event[0] == 'swap_y2x':
        volume = np.log(data['amount1'])
    
    # Calculate interval event times
    inter_times = data['event_time'].diff().dropna()

    # Calculate rolling standard deviation
    rolling_std = data['price'].rolling(window).std()

     # scatter plot of inter_times vs rolling_std
    plt.figure(figsize=(12, 6))
    if volume is None:
        scatter = plt.scatter(rolling_std[:-1], inter_times, alpha=0.5, s=10)
    else:
        scatter = plt.scatter(rolling_std[:-1], inter_times, alpha=0.5, s=10, c=volume[:-1], cmap='viridis', vmin=volume.min(), vmax=volume.max())
    plt.title(f'Inter-event times vs. Rolling Standard Deviation ({window} window)')
    plt.xlabel('Rolling Standard Deviation')
    plt.ylabel('Inter-event times')
    plt.grid(True)

    # Add colorbar
    cbar = plt.colorbar(scatter)
    cbar.set_label('Log Volume')

    # # Adjust layout and show plot
    plt.show()

def log_likelihood(data, pdf_function, *params):
    return np.sum(np.log(pdf_function(data, *params)))

def calculate_aic_bic(log_likelihood, num_params, num_data_points):
    aic = -2 * log_likelihood + 2 * num_params
    bic = -2 * log_likelihood + num_params * np.log(num_data_points)
    return aic, bic

def transition_probabilities(all_events, shift):
    """
    Calculates transition probabilities among burn, mint, swap, and swap_pct events,
    using a single-pass approach for better performance.
    """
    all_events = all_events.ffill()
    # Map underlying raw events to a high-level "event" label
    # (In-place creation of a new column is typically more efficient than repeated subsetting)
    event_map = {
        'burn': 'burn',
        'mint': 'mint',
        'swap_x2y': 'swap',
        'swap_y2x': 'swap'
    }
    all_events['event'] = all_events['event'].map(event_map)

    # Compute price change only once
    # (If your data is already sorted by [block_number, logIndex], skip resorting before diff)
    all_events['tick_diff'] = all_events['tick'].diff()

    # Identify which swap rows changed price ("swap_pct")
    mask_swap_price_change = (
        all_events['event'].str.contains('swap') & (all_events['tick_diff'] != 0)
    )
    all_events.loc[mask_swap_price_change, 'event'] = 'swap_pct'

    # Identify which swap rows did not change price ("swap")
    mask_swap_no_price_change = (
        all_events['event'].str.contains('swap') & (all_events['tick_diff'] == 0)
    )
    all_events.loc[mask_swap_no_price_change, 'event'] = 'swap'

    # Shift the event column to get the next event in the sequence
    all_events['next_event'] = all_events['event'].shift(-shift)

    # Count transitions between events
    transition_counts = all_events.groupby(['event', 'next_event']).size().unstack(fill_value=0)

    # Convert counts to probabilities (row-wise normalization)
    transition_probabilities = transition_counts.div(transition_counts.sum(axis=1), axis=0)

    # Compute unconditional probabilities (how often each event occurs overall)
    event_counts = all_events['event'].value_counts()
    unconditional_probabilities = event_counts / event_counts.sum()

    # Combine into a single DataFrame
    probabilities = transition_probabilities.copy()
    probabilities.loc['Unconditional'] = unconditional_probabilities

    return shift, probabilities

def plot_transition_probabilities(prob_dict, shifts):
    # Extract the transitions for plotting 
    unconditional_burn = prob_dict[1].loc['Unconditional', 'burn'] 
    unconditional_mint = prob_dict[1].loc['Unconditional', 'mint'] 
    unconditional_swap_zero = prob_dict[1].loc['Unconditional', 'swap'] 
    unconditional_swap_ch = prob_dict[1].loc['Unconditional', 'swap_pct'] 
    
    burn_to_burn = [prob_dict[shift].loc['burn', 'burn'] for shift in shifts] 
    burn_to_mint = [prob_dict[shift].loc['burn', 'mint'] for shift in shifts] 
    burn_to_swap_zero = [prob_dict[shift].loc['burn', 'swap'] for shift in shifts] 
    burn_to_swap_ch = [prob_dict[shift].loc['burn', 'swap_pct'] for shift in shifts] 
    mint_to_burn = [prob_dict[shift].loc['mint', 'burn'] for shift in shifts] 
    mint_to_mint = [prob_dict[shift].loc['mint', 'mint'] for shift in shifts] 
    mint_to_swap_zero = [prob_dict[shift].loc['mint', 'swap'] for shift in shifts] 
    mint_to_swap_ch = [prob_dict[shift].loc['mint', 'swap_pct'] for shift in shifts] 
    swap_zero_to_burn = [prob_dict[shift].loc['swap', 'burn'] for shift in shifts] 
    swap_zero_to_mint = [prob_dict[shift].loc['swap', 'mint'] for shift in shifts] 
    swap_zero_to_swap_zero = [prob_dict[shift].loc['swap', 'swap'] for shift in shifts]
    swap_zero_to_swap_ch = [prob_dict[shift].loc['swap', 'swap_pct'] for shift in shifts] 
    swap_ch_to_mint = [prob_dict[shift].loc['swap_pct', 'mint'] for shift in shifts] 
    swap_ch_to_burn = [prob_dict[shift].loc['swap_pct', 'burn'] for shift in shifts] 
    swap_ch_to_swap_zero = [prob_dict[shift].loc['swap_pct', 'swap'] for shift in shifts] 
    swap_ch_to_swap_ch = [prob_dict[shift].loc['swap_pct', 'swap_pct'] for shift in shifts] 
    
    # Plot the results with gray markers for p-values > 0.05 
    fig, ax = plt.subplots(4, 4, figsize=(21, 12), sharex=True) 
    
    for i, (transition, title) in enumerate([ 
        (burn_to_burn, 'burn to burn'),
        (burn_to_mint, 'burn to mint'),
        (burn_to_swap_zero, 'burn to swap zero'),
        (burn_to_swap_ch, 'burn to swap ch'),
        (mint_to_burn, 'mint to burn'),
        (mint_to_mint, 'mint to mint'),
        (mint_to_swap_zero, 'mint to swap zero'),
        (mint_to_swap_ch, 'mint to swap ch'),
        (swap_zero_to_burn, 'swap zero to burn'),
        (swap_zero_to_mint, 'swap zero to mint'),
        (swap_zero_to_swap_zero, 'swap zero to swap zero'), 
        (swap_zero_to_swap_ch, 'swap zero to swap ch'), 
        (swap_ch_to_burn, 'swap ch to burn'), 
        (swap_ch_to_mint, 'swap ch to mint'), 
        (swap_ch_to_swap_zero, 'swap ch to swap zero'), 
        (swap_ch_to_swap_ch, 'swap ch to swap ch') ]): 
        row, col = divmod(i, 4) # divmod returns the quotient and the remainder of the division of the first argument by the second
        ax[row, col].plot(shifts, transition, marker='o', linestyle='-', color=sns.color_palette()[row], alpha=0.6, label=title)
        if 'to burn' in title: 
            ax[row, col].axhline(unconditional_burn, color=sns.color_palette()[row], linestyle='--', label='Unconditional '+title.split(' ')[-1], alpha=0.5) 
        elif 'to mint' in title: 
            ax[row, col].axhline(unconditional_mint, color=sns.color_palette()[row], linestyle='--', label='Unconditional '+title.split(' ')[-1], alpha=0.5) 
        elif 'to swap zero' in title: 
            ax[row, col].axhline(unconditional_swap_zero, color=sns.color_palette()[row], linestyle='--', label='Unconditional '+title.split(' ')[-1], alpha=0.5)
        elif 'to swap ch' in title: 
            ax[row, col].axhline(unconditional_swap_ch, color=sns.color_palette()[row], linestyle='--', label='Unconditional '+title.split(' ')[-1], alpha=0.5)
        
        ax[row, col].set_xscale('log')
        ax[row, col].legend()

    fig.supxlabel('event Time') 

    #plt.savefig(f'stf_tr_probs_{pool_name}.pdf', format='pdf', bbox_inches='tight', dpi=300)
    plt.show()

def distribution_events(data, name, window):
    '''The following function evaluate the mean percentage of events in each interval of length 'window' across all the days in the dataset. Then, it evaluates the 95% confidence interval for these means.

    Specifically, if $i$ represent the $i$-th interval of lenght window, the corresponding mean is
    $$\bar{x} = \frac{1}{n} \sum_{j=1}^n x_{ij}$$

    where $j$ runs over the events in the interval $i$.

    Then, the standard error SEM over the mean is
    $$SEM_i = \frac{\sigma_i}{\sqrt{n}}$$

    The confidence intervals are computed using the t-distribution to account for finite sample size. The procedure involves evaluating the critical values $t_{\alpha/2, n-1}$ using a t distribution with $n-1$ degrees of freedom. Then
    $$CI_i = \bar{x}_i \cdot t_{\alpha/2, n-1} \cdot SEM_i$$'''

    n_days = len(data['datetime'].dt.date.unique())
    data_sorted = data.set_index('datetime')
        
    # Resample to window-minute intervals, count events, and group by date
    data_window_grouped = data_sorted.resample(window).size()
    print(f'data_window_grouped: {data_window_grouped.shape}')
    # check NaN values
    if data_window_grouped.isnull().values.any():
        print(f'There are NaN values in the data_window_grouped resampled. This means that sometimes there have not been any events in the provided time window. '
              f'I will fill these NaN values with 0. '
              f'Total NaN values: {data_window_grouped.isnull().sum().sum()}')
        data_window_grouped = data_window_grouped.fillna(0)

    data_window_grouped = data_window_grouped.groupby(data_window_grouped.index.date).apply(lambda x: x / x.sum() * 100)  # Convert to percentages

    # Extract the time of day for each interval
    data_window_grouped.index = data_window_grouped.index.get_level_values(1).time  # Use time of day as index

    # Group by time of day and compute mean and confidence interval
    data_grouped = data_window_grouped.groupby(data_window_grouped.index)

    data_mean = data_grouped.mean()
    data_sem = data_grouped.sem()  # Standard error of the mean
    confidence_interval = stats.t.ppf(0.975, data_grouped.count() - 1) * data_sem  # 95% CI

    # Convert time objects to strings for plotting
    data_mean.index = data_mean.index.astype(str)
    
    # Plot the mean and 95% confidence interval
    plt.figure(figsize=(13, 6))
    plt.errorbar(data_mean.index, data_mean, yerr=confidence_interval, fmt='o', capsize=5, label='Mean with 95% CI', alpha=0.7)
    plt.title(f'Mean daily percentage of {name} events per {window}-minute interval')
    plt.xlabel('Time')
    plt.ylabel('Percentage')
    plt.xticks(rotation=90)
    plt.grid(True)
    plt.show()

    group_data_list = []
    # Iterate through the groups
    for _, group in data_grouped:
        # Ensure the group has the same length
        group_data_list.append(group.values[:n_days-1]) # trimmed up to 364 because some group have 365 values
    
    # Trim the group_data_list to the minimum length
    min_length = min(len(group) for group in group_data_list)
    max_length = max(len(group) for group in group_data_list)
    print(f'[Distribution of events] Minimum length: {min_length}, Maximum length: {max_length}')
    group_data_list = [group[:min_length] for group in group_data_list]

    # Stack all groups into a 2D numpy array
    group_data_list = np.vstack(group_data_list)

    return data_mean, confidence_interval, group_data_list

def distribution_values(data, amount_column, time_column, name, window):
    '''
    This function calculates the mean percentage of the values in the specified column 
    (`amount_column`) for each interval of length `window` across all the days in the dataset. 
    It then computes the 95% confidence interval for these means.
    
    Parameters:
    - data: DataFrame containing the data with a time_column column and the specified column.
    - time_column: The column name containing the timestamps.
    - amount_column: The column whose values will be analyzed.
    - name: The name to use in the plot title.
    - window: Resampling interval (e.g., '30T' for 30 minutes).
    '''

    n_days = len(data[time_column].dt.date.unique())
    data_sorted = data.set_index(time_column)
    
    # Resample to the specified interval and sum the values in the amount column
    data_resampled = data_sorted[amount_column].resample(window).sum()
    data_resampled = data_resampled.groupby(data_resampled.index.date).apply(
        lambda x: x / x.sum() * 100  # Convert to percentages
    )
    
    # Extract the time of day for each interval
    data_resampled.index = data_resampled.index.get_level_values(1).time  # Use time of day as index

    # Group by time of day and compute mean and confidence interval
    data_grouped = data_resampled.groupby(data_resampled.index)
    data_mean = data_grouped.mean()
    data_sem = data_grouped.sem()  # Standard error of the mean
    confidence_interval = stats.t.ppf(0.975, data_grouped.count() - 1) * data_sem  # 95% CI

    # Convert time objects to strings for plotting
    data_mean.index = data_mean.index.astype(str)
    
    # Plot the mean and 95% confidence interval
    plt.figure(figsize=(13, 6))
    plt.errorbar(data_mean.index, data_mean, yerr=confidence_interval, fmt='o', capsize=5, label='Mean with 95% CI', alpha=0.7)
    plt.title(f'Mean daily percentage of {name} per {window}-minute interval')
    plt.xlabel('Time')
    plt.ylabel(f'Percentage')
    plt.xticks(rotation=90)
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    group_data_list = []
    # Iterate through the groups
    for _, group in data_grouped:
        # Ensure the group has the same length (if necessary, pad or truncate values)
        group_data_list.append(group.values[:n_days-1]) # trimmed up to 364 because some group have 365 values
    # Stack all groups into a 2D numpy array
    group_data_list = np.vstack(group_data_list)

    return data_mean, confidence_interval, group_data_list

def intraday_volatility_analysis_by_day(data, time_column, price_column, time_window='30min'):
    data[time_column] = pd.to_datetime(data[time_column]).dt.tz_localize('UTC').dt.tz_convert('Europe/Rome')
    data_sorted = data.set_index(time_column)
    
    # Ensure the data is sorted by the time index
    data_sorted = data_sorted.sort_index()
    
    # Calculate log returns
    data_sorted['log_return'] = np.log(data_sorted[price_column]).diff()
    
    # Extract the date to group by day
    data_sorted['date'] = data_sorted.index.date
    
    # Define the rolling window size in periods
    if 'min' in time_window:
        window_minutes = int(time_window.replace('min', ''))
        freq_minutes = (data_sorted.index.to_series().diff().dt.total_seconds().mean() / 60) or 1
        window_periods = max(1, int(window_minutes / freq_minutes))
        print(f"Using a rolling window of {window_periods} periods for {time_window} intervals.")
    else:
        raise ValueError("Only minute-based windows are supported in this configuration.")
    
    # Calculate rolling standard deviation of log returns for each day
    # daily_rolling_volatilities will n_days array representing the rolling volatilities for each day
    daily_rolling_volatilities = []
    unique_dates = data_sorted['date'].unique()
    for date in unique_dates:
        # Select only data for the current date
        daily_data = data_sorted[data_sorted['date'] == date]
        daily_rolling_vol = daily_data['log_return'].rolling(window_periods).std()
        daily_rolling_volatilities.append(daily_rolling_vol.values)
    
    # Align arrays to the same length (shorter arrays padded with NaN at the end)
    max_length = max(len(vol) for vol in daily_rolling_volatilities) # extract the maximum length of the rolling volatilitites over all the days
    aligned_volatilities = np.array([
        np.pad(vol, (0, max_length - len(vol)), constant_values=np.nan)
        for vol in daily_rolling_volatilities
    ])
    
    # Calculate mean and 95% confidence intervals across days
    mean_volatility = np.nanmean(aligned_volatilities, axis=0)
    std_volatility = np.nanstd(aligned_volatilities, axis=0)
    n_days = np.sum(~np.isnan(aligned_volatilities), axis=0)
    sem_volatility = std_volatility / np.sqrt(n_days)
    ci_95 = 1.96 * sem_volatility
    
    # Plot mean volatility with confidence intervals
    plt.figure(figsize=(10, 6))
    plt.fill_between(range(len(mean_volatility)), 
                     mean_volatility - ci_95, 
                     mean_volatility + ci_95, 
                     alpha=0.2, label='95% Confidence Interval')
    plt.plot(mean_volatility, label='Mean Volatility', color='blue')
    plt.title('Mean Intraday Rolling Volatility with 95% CI')
    plt.xlabel('Time Index in Rolling Window')
    plt.ylabel('Volatility')
    plt.legend()
    plt.grid(True)
    plt.show()
    
    # Return results
    result = pd.DataFrame({
        'mean_volatility': mean_volatility,
        'ci_95_lower': mean_volatility - ci_95,
        'ci_95_upper': mean_volatility + ci_95
    })
    return result

def intraday_bin_volatility_analysis(data, time_column, price_column, pool_address, time_window='30min', name='Uniswap'):

    data[time_column] = pd.to_datetime(data[time_column])
    data_sorted = data.set_index(time_column)
    
    # Ensure the data is sorted by the time index
    data_sorted = data_sorted.sort_index()
    
    # Calculate log returns
    data_sorted['log_return'] = np.log(data_sorted[price_column]).diff()
    
    # Define the bins for each day
    freq_minutes = pd.to_timedelta(time_window)
    # Create bins for each minute of the day
    bins = pd.timedelta_range(start='0 days', end='1 days', freq=freq_minutes, closed='left')
    bin_labels = [str(bin_time)[7:] for bin_time in bins]  # Keep only the time part (HH:MM:SS)

    # Extract the time of day to bin log returns
    # pd.to_timedelta is used to convert these time strings into timedelta objects 
    # representing the duration since midnight for each record.
    data_sorted['time_of_day'] = pd.to_timedelta(data_sorted.index.time.astype(str))
    data_sorted['bin'] = pd.cut(
        data_sorted['time_of_day'],
        bins=bins,
        right=False,
        labels=bin_labels[:-1]
    )
    
    # Group by date and bin, then calculate volatility within each bin
    data_sorted['date'] = data_sorted.index.date
    grouped = data_sorted.groupby(['date', 'bin'])['log_return']
    bin_volatility = grouped.std().reset_index()
    
    # Pivot to create a matrix where rows are days and columns are bins
    volatility_matrix = bin_volatility.pivot(index='date', columns='bin', values='log_return')
    
    # Calculate mean, std, SEM, and 95% CI across days for each bin
    mean_volatility = volatility_matrix.mean(axis=0)
    std_volatility = volatility_matrix.std(axis=0)
    n_days = volatility_matrix.count(axis=0)
    sem_volatility = std_volatility / np.sqrt(n_days)
    ci_95 = 1.96 * sem_volatility
    
    # Plot mean volatility with error bars for 95% CI
    plt.figure(figsize=(10, 6))
    plt.errorbar(
        mean_volatility.index,
        mean_volatility.values,
        yerr=ci_95.values,
        fmt='o',
        capsize=5,
        label='Mean Volatility with 95% CI'
    )
    plt.xticks(rotation=90)
    plt.title(f'Average volatility on {name} for  each {time_window} bin')
    plt.xlabel('Time Bin')
    plt.ylabel('Volatility')
    plt.grid(True)
    plt.legend()
    plt.savefig(f'images/arbitrum/{name}_intraday_volatility.pdf', bbox_inches='tight', dpi=300)
    plt.show()
    
    # Return result as a DataFrame
    result = pd.DataFrame({
        'mean_volatility': mean_volatility,
        'ci_95_lower': mean_volatility - ci_95,
        'ci_95_upper': mean_volatility + ci_95
    })
    return result, volatility_matrix

def bca_confint_vectorized(theta_hat, theta_b, alpha=0.05):
    """
    Compute the Bias-Corrected and Accelerated (BCa) bootstrap confidence intervals for vector statistics.

    Parameters:
    theta_hat : array-like, shape (n_lags+1,)
        The estimates from the original data.
    theta_b : array-like, shape (B, n_lags+1)
        Array of bootstrap estimates.
    alpha : float, optional
        Significance level (default=0.05 for 95% confidence intervals).

    Returns:
    lower_bounds : array-like, shape (n_lags+1,)
        The lower bounds of the BCa confidence intervals.
    upper_bounds : array-like, shape (n_lags+1,)
        The upper bounds of the BCa confidence intervals.
    """
    theta_b = np.array(theta_b)
    theta_hat = np.array(theta_hat)
    B, n_lags = theta_b.shape

    # Bias-correction factor z0 for each lag
    p = np.mean(theta_b < theta_hat, axis=0)
    # Ensure p is within (0,1)
    p = np.clip(p, 1e-6, 1 - 1e-6)
    z0 = norm.ppf(p)

    # Acceleration constant a for each lag
    mu_b = np.mean(theta_b, axis=0)
    sigma_b = np.std(theta_b, axis=0, ddof=1)
    # Avoid division by zero
    sigma_b_safe = np.where(sigma_b == 0, 1e-6, sigma_b)
    skewness = np.mean(((theta_b - mu_b) / sigma_b_safe) ** 3, axis=0)
    a = skewness / 6.0

    # Compute adjusted alpha levels
    z_alpha1 = norm.ppf(alpha / 2)
    z_alpha2 = norm.ppf(1 - alpha / 2)

    z0_z_alpha1 = z0 + z_alpha1
    z0_z_alpha2 = z0 + z_alpha2

    denom1 = 1 - a * z0_z_alpha1
    denom2 = 1 - a * z0_z_alpha2

    # Avoid division by zero in denom1 and denom2
    denom1_safe = np.where(denom1 == 0, 1e-6, denom1)
    denom2_safe = np.where(denom2 == 0, 1e-6, denom2)

    alpha1_star = norm.cdf(z0 + z0_z_alpha1 / denom1_safe)
    alpha2_star = norm.cdf(z0 + z0_z_alpha2 / denom2_safe)

    # Handle potential numerical issues
    alpha1_star = np.clip(alpha1_star, 1e-6, 1 - 1e-6)
    alpha2_star = np.clip(alpha2_star, 1e-6, 1 - 1e-6)

    # Compute BCa confidence intervals for each lag
    lower_bounds = np.array([
        np.percentile(theta_b[:, i], 100 * alpha1_star[i]) for i in range(n_lags)
    ])
    upper_bounds = np.array([
        np.percentile(theta_b[:, i], 100 * alpha2_star[i]) for i in range(n_lags)
    ])

    return lower_bounds, upper_bounds

def bootstrap_blocks(series, block_size, n_bootstrap, method='mbb'):
    if method == 'mbb':
        return moving_block_bootstrap(series, block_size, n_bootstrap)
    elif method == 'sbb':
        return stationary_block_bootstrap(series, block_size, n_bootstrap)
    elif method == 'cbb':
        return circular_block_bootstrap(series, block_size, n_bootstrap)
    else:
        raise ValueError(f"Unknown method: {method}")

def moving_block_bootstrap(series, block_size, n_bootstrap):
    """Generate bootstrap samples using Moving Block Bootstrap (MBB)."""
    n = len(series)
    blocks = [series[i:i+block_size] for i in range(n - block_size + 1)]
    samples = []
    for _ in range(n_bootstrap):
        sample = np.concatenate([blocks[np.random.randint(0, len(blocks))] 
                                 for _ in range(n // block_size + 1)])[:n]
        samples.append(sample)
    return np.array(samples)

def stationary_block_bootstrap(series, block_size, n_bootstrap):
    """Generate bootstrap samples using Stationary Block Bootstrap (SBB)."""
    n = len(series)
    samples = []
    for _ in range(n_bootstrap):
        sample = []
        i = np.random.randint(0, n)
        while len(sample) < n:
            length = np.random.geometric(1 / block_size)
            sample.extend(series[i:i+length])
            i = np.random.randint(0, n)  # Restart at random position
        samples.append(np.array(sample[:n]))
    return np.array(samples)

def circular_block_bootstrap(series, block_size, n_bootstrap):
    """Generate bootstrap samples using Circular Block Bootstrap (CBB)."""
    n = len(series)
    extended_series = np.concatenate([series, series[:block_size-1]])  # Circular extension
    blocks = [extended_series[i:i+block_size] for i in range(n)]
    samples = []
    for _ in range(n_bootstrap):
        sample = np.concatenate([blocks[np.random.randint(0, len(blocks))] 
                                 for _ in range(n // block_size + 1)])[:n]
        samples.append(sample)
    return np.array(samples)

def compute_bias_and_variance(data, block_lengths, autocorr_fun, lag):
    """
    Computes bias and variance for different block lengths using bootstrap.
    
    Parameters:
        data (np.ndarray): The time series or dependent data.
        block_lengths (list): List of block lengths to evaluate.
        statistic_fn (function): Statistic function to compute (e.g., np.mean).

    Returns:
        bias_estimates (np.ndarray): Estimated biases for each block length.
        variance_estimates (np.ndarray): Estimated variances for each block length.
    """
    n = len(data) # Length of the input data
    if n == 0:
        raise ValueError("Input data is empty. Please provide non-empty data.")

    bias_estimates = [] # Initialize list to store bias estimates for each block length
    variance_estimates = [] # Initialize list to store variance estimates for each block length

    for block_length in tqdm(block_lengths, desc='Block Lengths'):
        # Skip invalid block lengths
        if block_length >= n:
            continue

        # Generate block bootstrap samples
        num_blocks = n // block_length
        bootstrap_samples = []
        for _ in range(1000):  # Perform bootstrap resampling 1000 times
            sample = []
            for _ in range(num_blocks):  # Create `num_blocks` blocks
                start = np.random.randint(0, n - block_length + 1)  # Random valid start index
                end = start + block_length  # Compute end index
                sample.append(data[start:end])  # Append the block
            bootstrap_samples.append(np.concatenate(sample))  # Concatenate blocks for the sample

        # Skip if no valid bootstrap samples were generated
        if len(bootstrap_samples) == 0:
            continue

        # Compute the bootstrap statistics for the current block length
        bootstrap_stats = [autocorr_fun(sample, lag) for sample in bootstrap_samples]
        
        # # Estimate the bias as the difference between the mean bootstrap statistic and the original statistic
        original_stat = autocorr_fun(data, lag) # Compute the original statistic
        bias = np.mean(bootstrap_stats) - original_stat # Calculate the bias
        bias_estimates.append(bias) # Append the bias estimate to the list

        # Estimate the variance of the bootstrap statistics
        variance = np.var(bootstrap_stats, ddof=1) # Use unbiased variance estimator
        variance_estimates.append(variance) # Append the variance estimate to the list

        # Check if no valid block lengths were processed
        if len(bias_estimates) == 0 or len(variance_estimates) == 0:
            raise ValueError("No valid blocks could be processed. Check your data and block lengths.")

    return np.array(bias_estimates), np.array(variance_estimates)

def lahiri_plugin_block_size(data, autocorr_fun, block_lengths, lag, alpha=1):
    """
    Implements Lahiri's plug-in method for selecting the optimal block size.

    Parameters:
        data (np.ndarray): The time series or dependent data.
        statistic_fn (function): Statistic function to compute (e.g., np.mean).
        block_lengths (list): List of candidate block lengths.
        alpha (float): Smoothness parameter for bias term.

    Returns:
        optimal_block_size (float): The estimated optimal block size.
    """
    # Compute bias and variance estimates for the candidate block lengths
    bias, variance = compute_bias_and_variance(data, block_lengths, autocorr, lag)

    # Compute mean squared error (MSE) contributions for each block length
    mse_contributions = (bias**2) / (variance + 1e-8)  # Add small constant to avoid division by zero

    # Find the block length with the minimum MSE contribution
    optimal_block_index = np.argmin(mse_contributions)  # Index of the optimal block length
    optimal_block_size = block_lengths[optimal_block_index]  # Corresponding block length

    return optimal_block_size, bias, variance

def find_best_fit_distribution(data):
    """
    Fits multiple distributions to the data and selects the best fit based on log-likelihood.

    Parameters:
        data (array-like): Raw data to fit distributions to.
        candidate_distributions (list): List of scipy.stats distributions to test.

    Returns:
        dict: A dictionary with the best distribution, its parameters, and the log-likelihood.
    """
    best_fit = {
        "distribution": None,
        "parameters": None,
        "log_likelihood": -np.inf,
    }

    # List of candidate distributions
    candidate_distributions = [
        stats.lognorm,
        stats.gamma,
        stats.weibull_min,
        stats.pareto,
        stats.expon,
    ]

    for dist in candidate_distributions:
        try:
            # Fit the distribution to the data
            params = dist.fit(data)

            # Calculate the log-likelihood
            log_likelihood = np.sum(dist.logpdf(data, *params))

            # Check if this is the best fit so far
            if log_likelihood > best_fit["log_likelihood"]:
                best_fit["distribution"] = dist
                best_fit["parameters"] = params
                best_fit["log_likelihood"] = log_likelihood

        except Exception as e:
            print(f"Could not fit {dist.name}: {e}")

    return best_fit

def conditioned_autocorrelation(df, S, feature='log_returns', max_lag=20, verbose=True):
    """
    Compute the autocorrelation of the 'returns' column conditioned on the fact that 
    between time t and t+tau there are no rows whose 'tx_hash' is in S.
    
    Parameters:
        df (pd.DataFrame): DataFrame with at least two columns: 'returns' and 'tx_hash'
        S (set): A set of transaction hashes (strings or numbers) indicating events.
        max_lag (int): The maximum lag tau for which to compute the autocorrelation.
        
    Returns:
        pd.Series: A series with index tau=1,...,max_lag and the corresponding 
                   autocorrelation coefficient.
    """
    # Make a copy so we don't modify the original DataFrame.
    df = df.copy()
    
    # Create a boolean column that is True if the tx_hash is in S.
    df['is_event'] = df['tx_hash'].isin(S)
    
    # Compute the cumulative sum of events.
    # This will help us quickly check if there was an event between two rows.
    cumsum = df['is_event'].cumsum().to_numpy()
    
    # Extract the returns as a NumPy array.
    feat = df[feature].to_numpy()
    n = len(feat)
    
    # Dictionary to store the autocorrelation for each lag tau.
    acf = {}
    
    # Loop over lags.
    for tau in tqdm(range(1, max_lag + 1), desc="Computing ACF", disable=not verbose):
        # For each starting index t, we want to use the pair (t, t+tau) if and only if
        # there were no events in the rows between t and t+tau.
        #
        # We can check this using the cumulative sum:
        # For t from 0 to n - tau - 1, the condition is:
        #    cumsum[t+tau-1] - cumsum[t] == 0
        #
        # For tau==1 the “interval” is empty so we always take the pair.
        if tau == 1:
            valid = np.ones(n - tau, dtype=bool)
        else:
            valid = (cumsum[tau - 1:n - 1] - cumsum[:n - tau]) == 0
        
        # If we have at least two valid pairs, compute the Pearson correlation.
        if valid.sum() > 1:
            ret_t     = feat[:n - tau][valid]
            ret_tau   = feat[tau:][valid]
            corrcoef = np.corrcoef(ret_t, ret_tau)[0, 1]
        else:
            corrcoef = np.nan  # Not enough points to compute correlation.
        
        acf[tau] = corrcoef
    
    # Return as a pandas Series with lag as the index.
    return pd.Series(acf, name="conditioned_acf")

def echo_swaps_detection(swaps):
    # Assuming swaps is your DataFrame and that it is sorted by 'blockNumber'
    block_numbers_arr = swaps['blockNumber'].values
    origin_arr        = swaps['origin'].values
    event_arr         = swaps['eventType'].values
    amount0_arr       = swaps['amount0'].values
    tx_hash_arr       = swaps['transactionHash'].values

    # Create arrays that compare consecutive rows (only up to len-1)
    same_block  = block_numbers_arr[:-1] == block_numbers_arr[1:]
    same_origin = origin_arr[:-1] == origin_arr[1:]
    same_tx_hash = tx_hash_arr[:-1] == tx_hash_arr[1:]
    # Check if the event pairs are one of the two valid combinations
    valid_events = ((event_arr[:-1] == 'swap_x2y') & (event_arr[1:] == 'swap_y2x')) | \
                ((event_arr[:-1] == 'swap_y2x') & (event_arr[1:] == 'swap_x2y'))

    # Combine conditions to create a mask for valid consecutive rows
    mask = same_block & same_origin & valid_events# & same_tx_hash

    # Use the mask to select the appropriate values
    selected_block_numbers = block_numbers_arr[:-1][mask]
    selected_origins       = origin_arr[:-1][mask]
    selected_amount0       = amount0_arr[:-1][mask]
    selected_amount0_next  = amount0_arr[1:][mask]
    selected_tx_hash_1     = tx_hash_arr[:-1][mask]
    selected_tx_hash_2     = tx_hash_arr[1:][mask]

    # Build the swap_amounts and tx_hashes lists
    swap_amounts = [[a, b] for a, b in zip(selected_amount0, selected_amount0_next)]
    # tx_hashes    = [[h1, h2] for h1, h2 in zip(selected_tx_hash_1, selected_tx_hash_2)]

    # Create the resulting DataFrame
    echo_swaps = pd.DataFrame({
        'block_number': selected_block_numbers,
        'origin': selected_origins,
        'swap_amounts': swap_amounts,
        'tx_hashes': selected_tx_hash_1
    })
    return echo_swaps
