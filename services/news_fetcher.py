import feedparser
import urllib.parse
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


def extract_text_from_html(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    paragraphs = soup.find_all("p")
    text = " ".join([p.get_text(strip=True) for p in paragraphs])

    return text


def fetch_news(symbol, max_items=5):
    query = f"{symbol} stock India"
    encoded_query = urllib.parse.quote(query)
    rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"

    feed = feedparser.parse(rss_url)
    news_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        })

        for idx, entry in enumerate(feed.entries[:max_items], start=1):
            try:
                # Open Google link → get redirected publisher URL
                page.goto(entry.link, timeout=60000)
                real_url = page.url

                # Now load actual article
                page.goto(real_url, timeout=60000)
                page.wait_for_load_state("networkidle")

                html = page.content()
                full_text = extract_text_from_html(html)

            except Exception:
                full_text = ""

            news_data.append({
                "index": idx,
                "title": entry.title,
                "source": entry.get("source", {}).get("title", "Unknown"),
                "content": full_text[:2000]
            })

        browser.close()

    return news_data

def format_for_gpt(symbol, articles):
    formatted = f"Stock: {symbol}\n\nNews Articles:\n\n"

    for article in articles:
        formatted += (
            f"Article {article['index']}:\n"
            f"Title: {article['title']}\n"
            f"Source: {article['source']}\n"
            f"Content:\n{article['content']}\n\n"
        )

    return formatted