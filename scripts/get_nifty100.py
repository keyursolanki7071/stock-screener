import requests
import pandas as pd
import gzip
from io import BytesIO

# ---------------------------------------
# STEP 1: Fetch NIFTY 100 from NSE CSV
# ---------------------------------------

def get_nifty100_symbols():
    url = "https://archives.nseindia.com/content/indices/ind_nifty100list.csv"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    df = pd.read_csv(BytesIO(response.content))

    symbols = df['Symbol'].tolist()
    return symbols


# ---------------------------------------
# STEP 2: Download Upstox Instrument Master
# ---------------------------------------

def get_upstox_instruments():
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"

    response = requests.get(url)
    response.raise_for_status()

    compressed_file = BytesIO(response.content)

    with gzip.open(compressed_file, 'rt') as f:
        df = pd.read_csv(f)

    return df


# ---------------------------------------
# STEP 3: Map NIFTY 100 with Upstox
# ---------------------------------------

def map_nifty100_with_upstox():
    print("Fetching NIFTY 100 symbols from NSE CSV...")
    nifty100_symbols = get_nifty100_symbols()

    # Clean NSE symbols
    nifty100_symbols = [s.strip().upper() for s in nifty100_symbols]

    print(f"Total NIFTY 100 stocks fetched: {len(nifty100_symbols)}")

    print("Downloading Upstox instrument master...")
    instruments_df = get_upstox_instruments()

    # Clean Upstox symbols
    instruments_df['clean_symbol'] = (
        instruments_df['tradingsymbol']
        .str.upper()
        .str.strip()
    )

    # Correct filter for equity
    equity_df = instruments_df[
        (instruments_df['exchange'] == 'NSE_EQ') &
        (instruments_df['instrument_type'] == 'EQUITY')
    ]

    print(f"Total NSE equity instruments: {len(equity_df)}")

    filtered_df = equity_df[
        equity_df['clean_symbol'].isin(nifty100_symbols)
    ]

    print(f"Matched stocks with Upstox instruments: {len(filtered_df)}")

    # Debug unmatched if any
    unmatched = set(nifty100_symbols) - set(filtered_df['clean_symbol'])
    if unmatched:
        print("\n⚠ Unmatched symbols:")
        print(unmatched)

    return filtered_df




# ---------------------------------------
# MAIN
# ---------------------------------------

if __name__ == "__main__":
    nifty100_upstox_df = map_nifty100_with_upstox()

    output_file = "nifty100_upstox_instruments.csv"
    nifty100_upstox_df.to_csv(output_file, index=False)

    print(f"\n✅ File saved successfully: {output_file}")
    print("\nSample Output:")
    print(nifty100_upstox_df[['tradingsymbol', 'instrument_key']].head())
