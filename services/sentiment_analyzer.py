from openai import OpenAI

client = OpenAI(api_key="KEY")


def analyze_sentiment(symbol, headlines):

    if not headlines:
        return {
            "score": 0,
            "summary": "No recent news",
            "decision": "NORMAL"
        }

    prompt = f"""
You are a financial sentiment analyst.

Stock: {symbol}

Recent Headlines:
{headlines}

Return ONLY valid JSON (no markdown).

Format:
{{
  "score": float between -1 and +1,
  "decision": "BUY_STRONG, BUY_NORMAL, BUY_WEAK, AVOID",
  "reason": "Why this decision?",
  "risks": "Any key risks or negative factors",
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
