import pandas as pd
import numpy as np
from services.upstox_data import load_stock_data
from services.instrument_mapper import get_instrument_key
from services.instrument_mapper import get_symbol_list


SYMBOLS = get_symbol_list()
INITIAL_CAPITAL = 100000
RISK_PER_TRADE = 0.01
MAX_PORTFOLIO_RISK = 0.04

START_DATE = "2015-01-01"
END_DATE = "2026-02-12"


# ======================================
# PREPARE MASTER DATA
# ======================================

def prepare_master():

    frames = []

    # ----- Load Nifty -----
    nifty_key = "NSE_INDEX|Nifty 50"
    nifty_df = load_stock_data(nifty_key, START_DATE, END_DATE)

    nifty_df["ema_200"] = nifty_df["close"].ewm(span=200).mean()
    nifty_df["market_ok"] = nifty_df["close"] > nifty_df["ema_200"]

    # ----- Load Stocks -----
    for symbol in SYMBOLS:

        instrument_key = get_instrument_key(symbol)

        if instrument_key is None:
            continue

        df = load_stock_data(instrument_key, START_DATE, END_DATE)

        if df is None or len(df) < 300:
            continue

        df["ema_200"] = df["close"].ewm(span=200).mean()
        df["hh_20"] = df["high"].rolling(20).max().shift(1)
        df["ll_10"] = df["low"].rolling(10).min().shift(1)
        df["vol_ma_20"] = df["volume"].rolling(20).mean()

        df["symbol"] = symbol

        # Merge Nifty regime
        df = df.merge(
            nifty_df[["market_ok"]],
            left_index=True,
            right_index=True,
            how="left"
        )

        frames.append(df)

    master = pd.concat(frames)
    master.sort_index(inplace=True)

    return master


# ======================================
# BACKTEST ENGINE
# ======================================

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

        # ==========================
        # PROCESS EXITS
        # ==========================
        for symbol in list(open_positions.keys()):

            if symbol not in daily_data["symbol"].values:
                continue

            row = daily_data[daily_data["symbol"] == symbol].iloc[0]
            pos = open_positions[symbol]

            entry = pos["entry"]
            stop = pos["stop"]
            qty = pos["qty"]
            risk_per_share = pos["risk"]

            exit_price = None

            # Stop hit intraday
            if row["low"] <= stop:
                exit_price = stop

            # Exit rule
            elif row["close"] < row["ll_10"]:
                exit_price = row["close"]

            if exit_price is not None:

                pnl = (exit_price - entry) * qty
                capital += pnl

                R = pnl / (risk_per_share * qty)
                trades.append(R)

                del open_positions[symbol]

        # ==========================
        # PROCESS ENTRIES
        # ==========================
        current_risk = sum(pos["risk_amount"] for pos in open_positions.values())

        for _, row in daily_data.iterrows():

            symbol = row["symbol"]

            if symbol in open_positions:
                continue

            if not row["market_ok"]:
                continue

            # if current_risk >= capital * MAX_PORTFOLIO_RISK:
            #     break

            # Entry Conditions
            if row["close"] <= row["ema_200"]:
                continue

            if row["close"] <= row["hh_20"] * 1.005:
                continue

            if row["volume"] <= 1.5 * row["vol_ma_20"]:
                continue

            stop = row["ll_10"]

            if pd.isna(stop):
                continue

            entry = row["close"]
            risk = entry - stop

            if risk <= 0:
                continue

            risk_amount = capital * RISK_PER_TRADE
            qty = risk_amount / risk

            open_positions[symbol] = {
                "entry": entry,
                "stop": stop,
                "qty": qty,
                "risk_amount": risk_amount,
                "risk": risk
            }

            current_risk += risk_amount

        equity_curve.append(capital)

    return capital, trades, equity_curve


# ======================================
# RESULTS
# ======================================

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
    max_drawdown = dd.min() * 100

    print("\n===== BREAKOUT PORTFOLIO (UPSTOX DATA) =====")
    print("Trades:", len(trades))
    print("Final Capital: ", final_capital)
    print("Win Rate:", round(win_rate, 2))
    print("Profit Factor:", round(profit_factor, 2))
    print("Total Return %:", round(total_return, 2))
    print("Max Drawdown %:", round(max_drawdown, 2))


if __name__ == "__main__":

    final_capital, trades, equity_curve = run_backtest()
    print_results(final_capital, trades, equity_curve)
