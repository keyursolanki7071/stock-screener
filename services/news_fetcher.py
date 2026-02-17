import feedparser
import urllib.parse

def fetch_news(symbol, max_items=10):

    query = f"{symbol} stock India"
    encoded_query = urllib.parse.quote(query)
    url = f"https://news.google.com/rss/search?q={encoded_query}"

    feed = feedparser.parse(url)

    headlines = []

    for entry in feed.entries[:max_items]:
        headlines.append(entry.title)
    return headlines
