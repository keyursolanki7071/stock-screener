import pandas as pd
import numpy as np
from sqlalchemy import text
from config.database import engine
from services.instrument_mapper import get_symbol_list


INITIAL_CAPITAL = 100000
RISK_PER_TRADE = 0.01
MAX_PORTFOLIO_RISK = 0.04

START_DATE = "2015-01-01"
END_DATE = "2026-02-13"


# ==========================================
# LOAD STOCK DATA FROM DB
# ==========================================

def load_stock_data(symbol):

    query = """
        SELECT date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol = :symbol
        AND date BETWEEN :start AND :end
        ORDER BY date
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params={
            "symbol": symbol,
            "start": START_DATE,
            "end": END_DATE
        })

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
# PREPARE MASTER DATA
# ==========================================

def prepare_master():

    symbols = get_symbol_list()
    frames = []

    for symbol in symbols:

        print("Loading:", symbol)

        df = load_stock_data(symbol)

        if df is None or len(df) < 300:
            continue

        df = apply_vcp_logic(df)
        df["symbol"] = symbol

        frames.append(df)

    master = pd.concat(frames)
    master.sort_index(inplace=True)

    return master


# ==========================================
# PORTFOLIO BACKTEST ENGINE
# ==========================================

def run_backtest():

    master = prepare_master()

    capital = INITIAL_CAPITAL
    equity_curve = []
    trades = []
    open_positions = {}

    unique_dates = sorted(master.index.unique())

    for current_date in unique_dates:

        daily_data = master.loc[current_date]

        if isinstance(daily_data, pd.Series):
            daily_data = daily_data.to_frame().T

        # --------- EXIT ---------
        for symbol in list(open_positions.keys()):

            row = daily_data[daily_data["symbol"] == symbol]

            if row.empty:
                continue

            row = row.iloc[0]
            pos = open_positions[symbol]

            exit_price = None

            if row["low"] <= pos["stop"]:
                exit_price = pos["stop"]

            elif row["high"] >= pos["target"]:
                exit_price = pos["target"]

            if exit_price is not None:
                pnl = (exit_price - pos["entry"]) * pos["qty"]
                capital += pnl

                R = pnl / pos["risk_amount"]
                trades.append(R)

                del open_positions[symbol]

        # --------- ENTRY ---------
        current_risk = sum(pos["risk_amount"] for pos in open_positions.values())

        for _, row in daily_data.iterrows():

            symbol = row["symbol"]

            if symbol in open_positions:
                continue

            if not row["entry"]:
                continue

            if current_risk >= capital * MAX_PORTFOLIO_RISK:
                break

            entry = row["close"]
            stop = row["stop"]
            risk = row["risk"]

            if pd.isna(stop) or risk <= 0:
                continue

            risk_amount = capital * RISK_PER_TRADE
            qty = risk_amount / risk

            open_positions[symbol] = {
                "entry": entry,
                "stop": stop,
                "target": row["target"],
                "qty": qty,
                "risk_amount": risk_amount
            }

            current_risk += risk_amount

        equity_curve.append(capital)

        if capital <= 0:
            break

    return capital, trades, equity_curve


# ==========================================
# RESULTS
# ==========================================

def print_results(final_capital, trades, equity_curve):

    trades = np.array(trades)

    if len(trades) == 0:
        print("No trades.")
        return

    win_rate = np.mean(trades > 0)
    profit_factor = abs(trades[trades > 0].sum() / trades[trades < 0].sum())
    total_return = (final_capital / INITIAL_CAPITAL - 1) * 100

    eq = pd.Series(equity_curve)
    dd = (eq - eq.cummax()) / eq.cummax()
    max_dd = dd.min() * 100

    print("\n===== VCP PORTFOLIO RESULTS =====")
    print("Trades:", len(trades))
    print("Final Capital:", round(final_capital, 2))
    print("Win Rate:", round(win_rate, 2))
    print("Profit Factor:", round(profit_factor, 2))
    print("Total Return %:", round(total_return, 2))
    print("Max Drawdown %:", round(max_dd, 2))


# ==========================================

if __name__ == "__main__":

    final_capital, trades, equity_curve = run_backtest()
    print_results(final_capital, trades, equity_curve)
