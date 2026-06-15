"""NVIDIA news + lexicon-based sentiment.

News comes from Finnhub's company-news endpoint (structured, reliable - no scraping).
Sentiment is computed locally with VADER (headline + summary), so no premium API is needed.
Results are cached briefly to avoid hammering the news API.
"""
import time
from datetime import date, timedelta

import requests

from helpers import config

_SYMBOL = "NVDA"
_NEWS_URL = "https://finnhub.io/api/v1/company-news"
_CACHE = {}  # key -> (fetched_at_epoch, payload)
_CACHE_TTL_SEC = 600  # 10 minutes

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    _analyzer = SentimentIntensityAnalyzer()
except Exception:  # pragma: no cover - dependency missing
    _analyzer = None


def _label(compound):
    if compound >= 0.05:
        return "positive"
    if compound <= -0.05:
        return "negative"
    return "neutral"


def _score_text(text):
    if not text or _analyzer is None:
        return 0.0
    return round(_analyzer.polarity_scores(text)["compound"], 4)


def build_news_response(days=7, limit=40):
    """Return {articles, summary, fetchedAt}. Each article carries a VADER sentiment score+label."""
    days = max(1, min(int(days or 7), 30))
    limit = max(1, min(int(limit or 40), 100))
    cache_key = f"{days}:{limit}"
    cached = _CACHE.get(cache_key)
    now = time.time()
    if cached and (now - cached[0]) < _CACHE_TTL_SEC:
        return cached[1]

    today = date.today()
    params = {
        "symbol": _SYMBOL,
        "from": (today - timedelta(days=days)).isoformat(),
        "to": today.isoformat(),
        "token": config.FINNHUB_TOKEN,
    }
    resp = requests.get(_NEWS_URL, params=params, timeout=15)
    resp.raise_for_status()
    raw = resp.json()
    if not isinstance(raw, list):
        raw = []

    # Newest first, dedupe by headline, cap to limit.
    raw.sort(key=lambda a: a.get("datetime", 0), reverse=True)
    seen = set()
    articles = []
    pos = neg = neu = 0
    compound_sum = 0.0
    for a in raw:
        headline = (a.get("headline") or "").strip()
        if not headline or headline.lower() in seen:
            continue
        seen.add(headline.lower())
        summary = (a.get("summary") or "").strip()
        compound = _score_text(f"{headline}. {summary}")
        label = _label(compound)
        if label == "positive":
            pos += 1
        elif label == "negative":
            neg += 1
        else:
            neu += 1
        compound_sum += compound
        articles.append({
            "headline": headline,
            "summary": summary,
            "source": a.get("source") or "",
            "url": a.get("url") or "",
            "image": a.get("image") or "",
            "category": a.get("category") or "",
            "datetime": int(a.get("datetime") or 0) * 1000,  # ms for the frontend
            "sentiment": label,
            "sentimentScore": compound,
        })
        if len(articles) >= limit:
            break

    n = len(articles)
    avg = round(compound_sum / n, 4) if n else 0.0
    summary = {
        "count": n,
        "positive": pos,
        "negative": neg,
        "neutral": neu,
        "avgScore": avg,
        # 0-100 friendly score (50 = neutral).
        "score100": round((avg + 1) / 2 * 100, 1),
        "overall": _label(avg),
        "analyzerAvailable": _analyzer is not None,
    }
    payload = {
        "articles": articles,
        "summary": summary,
        "fetchedAt": int(now * 1000),
        "rangeDays": days,
    }
    _CACHE[cache_key] = (now, payload)
    return payload
