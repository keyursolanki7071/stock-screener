import requests
import pandas as pd
from datetime import datetime, timedelta

ACCESS_TOKEN = "ACCESSTOKEN"
BASE_URL = "https://api.upstox.com/v2/historical-candle"

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}


def fetch_chunk(instrument_key, start_date, end_date):

    url = f"{BASE_URL}/{instrument_key}/day/{end_date}/{start_date}"

    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print("Error:", response.json())
        return None

    data = response.json()["data"]["candles"]

    if not data:
        return None

    df = pd.DataFrame(data, columns=[
        "date", "open", "high", "low", "close", "volume", "unknown"
    ])

    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    df = df[["open", "high", "low", "close", "volume"]]
    df.sort_index(inplace=True)

    return df


def load_stock_data(instrument_key, start_date, end_date):

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    all_data = []

    while start < end:

        chunk_end = min(start + timedelta(days=365), end)

        df_chunk = fetch_chunk(
            instrument_key,
            start.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        )

        if df_chunk is not None:
            all_data.append(df_chunk)

        start = chunk_end + timedelta(days=1)

    if not all_data:
        return None

    final_df = pd.concat(all_data)
    final_df = final_df[~final_df.index.duplicated(keep="first")]
    final_df.sort_index(inplace=True)

    return final_df
