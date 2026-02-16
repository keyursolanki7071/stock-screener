import pandas as pd
from sqlalchemy import text
from config.database import engine


def load_stock_data(symbol, start_date=None, end_date=None):

    query = """
        SELECT date, open, high, low, close, volume
        FROM daily_prices
        WHERE symbol = :symbol
    """

    if start_date:
        query += " AND date >= :start_date"
    if end_date:
        query += " AND date <= :end_date"

    query += " ORDER BY date"

    params = {"symbol": symbol}
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
        
    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn, params=params)

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df
