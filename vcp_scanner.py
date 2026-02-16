import pandas as pd
import numpy as np
from sqlalchemy import text
from config.database import engine
from services.instrument_mapper import get_symbol_list


CAPITAL = 100000
RISK_PER_TRADE = 0.01

SCAN_DATE = None  # Set manually like "2026-02-12" or leave None for latest


# ==========================================
# LOAD STOCK DATA FROM DB
# ==========================================

def load_stock_data(symbol):

    query = """
        SELECT date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol = :symbol
        ORDER BY date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={"symbol": symbol})

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df


# ==========================================
# APPLY VCP LOGIC
# ==========================================

def apply_vcp_logic(df):

    df = df.copy()

    df["ema_200"] = df["close"].ewm(span=200).mean()
    df["ema_50"] = df["close"].ewm(span=50).mean()

    df["hh_20"] = df["high"].rolling(20).max().shift(1)
    df["ll_10"] = df["low"].rolling(10).min().shift(1)
    df["vol_ma_20"] = df["volume"].rolling(20).mean()

    # ATR
    df["tr"] = np.maximum(
        df["high"] - df["low"],
        np.maximum(
            abs(df["high"] - df["close"].shift(1)),
            abs(df["low"] - df["close"].shift(1))
        )
    )
    df["atr_14"] = df["tr"].rolling(14).mean()
    df["atr_mean_50"] = df["atr_14"].rolling(50).mean()

    trend = (
        (df["close"] > df["ema_200"]) &
        (df["ema_50"] > df["ema_200"]) &
        (df["ema_50"] > df["ema_50"].shift(5))
    )

    contraction = df["atr_14"] < df["atr_mean_50"] * 0.8

    breakout = (
        (df["close"] > df["hh_20"]) &
        (df["volume"] > 1.5 * df["vol_ma_20"])
    )

    df["entry"] = trend & contraction & breakout

    df["stop"] = df["ll_10"]
    df["risk"] = df["close"] - df["stop"]
    df["target"] = df["close"] + 2 * df["risk"]

    df.loc[df["risk"] <= 0, "entry"] = False

    return df


# ==========================================
# SCREENER
# ==========================================

def run_vcp_scan():

    symbols = get_symbol_list()

    today_entries = []

    for symbol in symbols:

        df = load_stock_data(symbol)

        if df is None or len(df) < 300:
            continue

        df = apply_vcp_logic(df)
        if SCAN_DATE:
            if pd.to_datetime(SCAN_DATE) not in df.index:
                continue
            latest = df.loc[pd.to_datetime(SCAN_DATE)]
        else:
            latest = df.iloc[-1]
            
        if not latest["entry"]:
            continue

        entry_price = latest["close"]
        stop = latest["stop"]
        risk = entry_price - stop

        if risk <= 0:
            continue

        risk_amount = CAPITAL * RISK_PER_TRADE
        qty = int(risk_amount / risk)

        if qty <= 0:
            continue

        today_entries.append({
            "symbol": symbol,
            "entry_price": round(entry_price, 2),
            "stop": round(stop, 2),
            "target": round(latest["target"], 2),
            "qty": qty
        })

    return today_entries


# ==========================================

if __name__ == "__main__":

    entries = run_vcp_scan()

    print("\n===== VCP ENTRY SIGNALS =====")

    if not entries:
        print("No signals today.")
    else:
        for e in entries:
            print(
                f"{e['symbol']} | Entry: {e['entry_price']} | "
                f"Stop: {e['stop']} | Target: {e['target']} | Qty: {e['qty']}"
            )
