import pandas as pd
from datetime import datetime, timedelta
from services.upstox_data import load_stock_data
from services.instrument_mapper import get_instrument_key
from services.instrument_mapper import get_symbol_list
from services.db_data_loader import load_stock_data as load_db_stock_data

CAPITAL = 100000
RISK_PER_TRADE = 0.01
SCAN_DATE = (datetime.today()).strftime("%Y-%m-%d")
SYMBOLS = get_symbol_list()
today = datetime.today()
start_date = (today - timedelta(days=500)).strftime("%Y-%m-%d")

print("Start Date: ", start_date)
print("End Date: ", SCAN_DATE)

def run_daily_scan():

    entries = []
    exits = []

    # ===== Load Nifty =====
    nifty_key = "NSE_INDEX|Nifty 50"
    nifty_df = load_stock_data(nifty_key, start_date, SCAN_DATE)

    if nifty_df is None or len(nifty_df) < 300:
        print("Nifty data insufficient.")
        return [], []

    nifty_df["ema_200"] = nifty_df["close"].ewm(span=200).mean()
    nifty_latest = nifty_df.iloc[-1]

    market_ok = nifty_latest["close"] > nifty_latest["ema_200"]

    # ===== Loop Stocks =====
    for symbol in SYMBOLS:

        # instrument_key = get_instrument_key(symbol)

        # if instrument_key is None:
        #     continue

        df = load_db_stock_data(symbol, start_date, SCAN_DATE)
        if df is None or len(df) < 300:
            continue

        # Indicators
        df["ema_200"] = df["close"].ewm(span=200).mean()
        df["hh_20"] = df["high"].rolling(20).max().shift(1)
        df["ll_10"] = df["low"].rolling(10).min().shift(1)
        df["vol_ma_20"] = df["volume"].rolling(20).mean()

        latest = df.iloc[-1]

        # ===== ENTRY CONDITIONS =====
        trend_condition = latest["close"] > latest["ema_200"]
        breakout_condition = latest["close"] > latest["hh_20"] * 1.005
        volume_condition = latest["volume"] > 1.5 * latest["vol_ma_20"]

        if market_ok and trend_condition and breakout_condition and volume_condition:

            entry = latest["close"]
            stop = latest["ll_10"]

            if pd.isna(stop):
                continue

            risk = entry - stop

            if risk <= 0:
                continue

            risk_amount = CAPITAL * RISK_PER_TRADE
            qty = int(risk_amount / risk)

            if qty > 0:
                entries.append({
                    "symbol": symbol,
                    "entry": round(entry, 2),
                    "stop": round(stop, 2),
                    "qty": qty
                })

        # ===== EXIT CONDITION =====
        if latest["close"] < latest["ll_10"]:
            exits.append(symbol)

    return entries, exits


if __name__ == "__main__":

    entries, exits = run_daily_scan()

    print(f"\n===== BREAKOUT SCAN FOR {SCAN_DATE}=====")

    print("\n--- ENTRY SIGNALS ---")
    if not entries:
        print("No entries today.")
    for e in entries:
        print(f"{e['symbol']} | Entry: {e['entry']} | Stop: {e['stop']} | Qty: {e['qty']}")

    print("\n--- EXIT FROM STOCKS ---")
    if not exits:
        print("No exits today.")
    for symbol in exits:
        print(symbol)
