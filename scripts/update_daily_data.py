from datetime import datetime, timedelta
from sqlalchemy import text
from config.database import engine
from services.upstox_data import load_stock_data
from services.instrument_mapper import get_symbol_list, get_instrument_key


def get_last_date(symbol):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(date) FROM daily_prices
            WHERE symbol = :symbol
        """), {"symbol": symbol}).fetchone()

    return result[0]


def store_to_db(symbol, df):

    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO daily_prices 
                (symbol, date, open, high, low, close, volume)
                VALUES (:symbol, :date, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, date) DO NOTHING
            """), {
                "symbol": symbol,
                "date": row.name.date(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row["volume"])
            })
        conn.commit()


def main():

    symbols = get_symbol_list()

    today = datetime.today().strftime("%Y-%m-%d")

    for symbol in symbols:

        print("Updating:", symbol)

        last_date = get_last_date(symbol)

        if last_date is None:
            print("No data found. Run full loader first.")
            continue

        start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")

        instrument_key = get_instrument_key(symbol)
        if not instrument_key:
            continue

        df = load_stock_data(instrument_key, start_date, today)

        if df is None or df.empty:
            continue

        store_to_db(symbol, df)


if __name__ == "__main__":
    main()
