from openai import OpenAI
from services.news_fetcher import format_for_gpt

client = OpenAI(api_key="API_KEY")


def analyze_sentiment(symbol, headlines):
    prompt_data = format_for_gpt(symbol, headlines)
    if not headlines:
        return {
            "score": 0,
            "summary": "No recent news",
            "decision": "NORMAL"
        }

    prompt = f"""
You are a professional financial sentiment analyst specializing in equity markets.

You will receive structured stock news data including:
- Source
- Title
- Full or partial article content

Important Rules:
- Base analysis ONLY on the provided text.
- Do NOT assume information not present.
- If multiple articles are provided, identify dominant themes.
- Earnings results, guidance changes, regulatory action, fraud, management commentary, debt issues, or major contracts should significantly impact sentiment.
- Repeated negative or positive themes increase conviction.

Scoring Framework:
+1.0 = Extremely bullish (major earnings beat, strong guidance, large contract win)
+0.5 = Moderately positive
0.0 = Neutral / mixed
-0.5 = Moderately negative
-1.0 = Extremely bearish (fraud, bankruptcy risk, major earnings miss, regulatory ban)

Decision Rules:
score >= 0.6 → BUY_STRONG
0.3 to 0.59 → BUY_NORMAL
0.05 to 0.29 → BUY_WEAK
-0.04 to 0.04 → AVOID
< -0.05 → AVOID

Return ONLY valid JSON.
No markdown.
No extra explanation.

{prompt_data}

Return JSON in this format:

{{
  "score": float (-1 to +1),
  "decision": "BUY_STRONG | BUY_NORMAL | BUY_WEAK | AVOID",
  "reason": "Concise explanation of dominant sentiment drivers",
  "risks": "Key downside or uncertainty factors",
  "summary": "2-line overall sentiment summary"
}}
"""


    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"}
    )

    return eval(response.choices[0].message.content)
