from services.upstox_data import load_stock_data
from services.instrument_mapper import get_symbol_list, get_instrument_key
from sqlalchemy import text
from config.database import engine

START_DATE = "2015-01-01"
END_DATE = "2026-02-13"


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

    for symbol in symbols:

        print("Loading:", symbol)

        instrument_key = get_instrument_key(symbol)

        if not instrument_key:
            continue

        df = load_stock_data(instrument_key, START_DATE, END_DATE)

        if df is None or df.empty:
            continue

        store_to_db(symbol, df)


if __name__ == "__main__":
    main()
