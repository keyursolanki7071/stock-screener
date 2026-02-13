import pandas as pd

INSTRUMENT_FILE = "stock_list.csv"

def get_instrument_key(symbol):

    df = pd.read_csv(INSTRUMENT_FILE)

    row = df[df["tradingsymbol"] == symbol]

    if row.empty:
        print(f"Instrument not found for {symbol}")
        return None

    return row.iloc[0]["instrument_key"]

def get_symbol_list():
    df = pd.read_csv(INSTRUMENT_FILE)
    return df["tradingsymbol"].tolist()
