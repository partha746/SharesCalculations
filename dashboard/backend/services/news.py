"""NVIDIA news: curation, finance-aware sentiment, and sentiment word frequencies.

Finnhub's company-news feed for NVDA is noisy — a large share of what it returns only
mentions NVIDIA in passing (or not at all) and is really about another company. So each
article is scored for relevance to NVIDIA and anything below a floor is dropped.

Sentiment is VADER, with two corrections for financial text:
  * a finance lexicon, so "beat", "downgrade", "curbs" and friends carry the weight they
    actually have in market copy rather than VADER's everyday reading;
  * the headline dominates the score, because summaries are often boilerplate that drags
    an otherwise clear headline toward neutral.
"""
import re
import time
from collections import defaultdict
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

# VADER's scale is roughly -4..+4. These either add finance vocabulary it lacks or
# re-weight words whose everyday sense misleads in market copy.
_FINANCE_LEXICON = {
    # Results and guidance
    "beat": 2.2, "beats": 2.2, "topped": 2.0, "tops": 2.0, "outperform": 2.3,
    "miss": -2.2, "missed": -2.2, "misses": -2.2, "shortfall": -2.3, "underperform": -2.3,
    "guidance": 0.0, "forecast": 0.0,
    # Ratings
    "upgrade": 2.5, "upgraded": 2.5, "upgrades": 2.5, "overweight": 1.8, "bullish": 2.6,
    "downgrade": -2.5, "downgraded": -2.5, "downgrades": -2.5, "underweight": -1.8, "bearish": -2.6,
    # Price action
    "surge": 2.6, "surges": 2.6, "surged": 2.6, "soar": 2.8, "soars": 2.8, "soared": 2.8,
    "rally": 2.2, "rallies": 2.2, "rallied": 2.2, "jumps": 1.8, "climbs": 1.5, "rebound": 1.8,
    "plunge": -2.8, "plunges": -2.8, "plunged": -2.8, "slump": -2.4, "slumped": -2.4,
    "tumble": -2.4, "tumbled": -2.4, "selloff": -2.2, "slide": -1.6, "slides": -1.6,
    "correction": -1.5, "pullback": -1.4,
    # Demand and supply
    "demand": 1.4, "backlog": 1.2, "sellout": 1.8, "ramp": 1.3, "expansion": 1.5,
    "breakthrough": 2.4, "record": 1.8, "milestone": 1.6,
    "glut": -2.0, "oversupply": -2.0, "shortage": -1.4, "bottleneck": -1.6, "delay": -1.6,
    "delayed": -1.6, "delays": -1.6, "cancellation": -2.2, "cancelled": -2.0,
    # Regulatory and legal
    "lawsuit": -2.0, "sued": -2.0, "probe": -1.9, "investigation": -1.8, "antitrust": -1.8,
    "ban": -2.4, "banned": -2.4, "curbs": -2.0, "restrictions": -1.8, "sanctions": -2.2,
    "tariff": -1.6, "tariffs": -1.6, "export": 0.0, "subpoena": -2.0, "fine": -1.2,
    # Corporate
    "layoffs": -2.2, "buyback": 1.6, "dividend": 1.2, "partnership": 1.6, "contract": 1.2,
    "acquisition": 1.0, "invests": 1.5, "investment": 1.2,
    # Words whose plain-English weight misleads here. "bad cloud infrastructure is
    # selling" is a bullish demand story; VADER read it as strongly negative.
    "bad": -0.8, "crazy": 0.0, "insane": 0.0, "extreme": 0.0, "kill": 0.0, "killer": 0.5,
    "crush": 1.2, "crushed": 1.2, "monster": 0.8, "beast": 0.8, "hot": 0.8, "cheap": 0.5,
}

if _analyzer is not None:
    _analyzer.lexicon.update(_FINANCE_LEXICON)

# NVIDIA itself. Word boundaries so "nvidia's" matches but longer words do not.
_NVDA_RE = re.compile(r"\b(nvidia|nvidia's|nvda|jensen huang)\b", re.I)

# Subjects that genuinely move NVDA, and the label shown as a topic chip.
_TOPICS = [
    ("Earnings", r"\b(earnings|revenue|quarterly results|q[1-4] (?:20\d\d|results)|guidance|eps|beat|miss)\b"),
    ("Data centre", r"\b(data ?cent(?:er|re)s?|hyperscaler|cloud capex|server|rack|cluster)\b"),
    ("AI chips", r"\b(blackwell|hopper|rubin|h100|h200|gb200|gb300|b200|grace|cuda|gpu|accelerator)\b"),
    ("China / export", r"\b(china|chinese|export control|export curb|beijing|tariff|sanction)\b"),
    ("Supply chain", r"\b(tsmc|foundry|hbm|sk hynix|micron|samsung|packaging|cowos|supply)\b"),
    ("Competition", r"\b(amd|intel|broadcom|custom silicon|tpu|trainium|competitor)\b"),
    ("Analyst view", r"\b(price target|analyst|rating|upgrade|downgrade|overweight|initiat(?:e|ed) coverage)\b"),
    ("Regulatory", r"\b(antitrust|regulator|lawsuit|probe|investigation|doj|ftc|sec filing)\b"),
    ("Partnerships", r"\b(partnership|deal|contract|agreement|collaborat|invests? in)\b"),
]
_TOPIC_RES = [(label, re.compile(pat, re.I)) for label, pat in _TOPICS]

# Retail-bait formats that carry no news. These are about NVIDIA but say nothing new.
_CLICKBAIT_RE = re.compile(
    r"("
    r"\b\d+\s+(?:best|top|great|amazing|reasons?|stocks?|things?|ways?)\b"
    r"|\bshould you (?:buy|sell|own)\b"
    r"|\bis it too late\b"
    r"|\bhere'?s (?:why|how|what)\b.*\b(?:millionaire|rich|retire)\b"
    r"|\bif you (?:invest|had invested|put)\b.*\$"
    r"|\bmy top\b|\bprediction:|\bbetter buy\b|\bmotley fool\b"
    r"|\bwhere will .* be in \d+ years?\b"
    r"|\bcould .* make you a millionaire\b"
    r"|\bstocks? to (?:buy|watch) (?:now|today|this)\b"
    r")",
    re.I,
)

# Generic market-wrap pieces that list many tickers.
_MARKET_WRAP_RE = re.compile(
    r"\b(stock market today|market wrap|premarket|pre-market movers|movers?:|"
    r"dow (?:jones )?(?:leads|falls|rises)|s&p 500 (?:today|closes)|"
    # Ticker-roundup formats: NVIDIA is one name in a list, not the subject.
    r"and more stocks|stocks that explain|winners and losers|biggest movers)\b",
    re.I,
)
# Four or more comma-separated capitalised names before the verb is the same roundup
# format under a different headline style.
_TICKER_LIST_RE = re.compile(r"([A-Z][A-Za-z.&'-]+,\s+){3,}")

_MIN_RELEVANCE_DEFAULT = 0.45


# Wider than VADER's usual +/-0.05. Market copy is mildly upbeat by default, and at 0.05
# almost everything landed in "positive", which made the split meaningless.
_NEUTRAL_BAND = 0.15


def _label(compound):
    if compound >= _NEUTRAL_BAND:
        return "positive"
    if compound <= -_NEUTRAL_BAND:
        return "negative"
    return "neutral"


def _raw_score(text):
    if not text or _analyzer is None:
        return 0.0
    return _analyzer.polarity_scores(text)["compound"]


def _score_article(headline, summary):
    """Headline carries most of the weight; summaries are often boilerplate that pulls a
    clear headline back toward neutral."""
    head = _raw_score(headline)
    body = _raw_score(summary) if summary else 0.0
    return round(head * 0.75 + body * 0.25 if summary else head, 4)


def _topics_for(text):
    return [label for label, rx in _TOPIC_RES if rx.search(text)]


def _relevance(headline, summary, topics):
    """0..1 estimate of how much this article is actually about NVIDIA.

    The dominant signal is whether NVIDIA appears in the headline: Finnhub returns many
    articles that merely name-check it in the body while being about someone else.
    """
    in_headline = bool(_NVDA_RE.search(headline))
    in_summary = bool(_NVDA_RE.search(summary or ""))

    if in_headline:
        score = 0.70
    elif in_summary:
        score = 0.28
    else:
        score = 0.05

    # Each on-topic subject adds a little, capped so a keyword-stuffed piece cannot
    # outrank a plain headline that is squarely about NVIDIA.
    score += min(len(topics), 3) * 0.07

    if _CLICKBAIT_RE.search(headline):
        score -= 0.45
    if _MARKET_WRAP_RE.search(headline) or _TICKER_LIST_RE.search(headline):
        score -= 0.35
    # A headline naming another company first is usually that company's story.
    if not in_headline and in_summary:
        score -= 0.05

    return round(max(0.0, min(1.0, score)), 3)


def _normalise_headline(headline):
    """Key for near-duplicate detection: syndicated copy differs only in punctuation,
    casing and trailing source tags."""
    text = re.sub(r"[^a-z0-9 ]+", " ", headline.lower())
    words = [w for w in text.split() if len(w) > 2]
    return " ".join(words[:9])


_TOKEN_RE = re.compile(r"[a-z][a-z'-]{2,}")
# Sentiment-bearing in general English but meaningless — or wrong — as cloud terms here.
# "smh" is the semiconductor ETF ticker; VADER reads it as "shaking my head".
_CLOUD_STOPWORDS = {
    "stock", "stocks", "share", "shares", "market", "nvidia", "nvda", "company",
    "smh", "argued", "arguing", "argue", "critical", "cut", "cuts",
}


def _word_cloud(articles, max_words=90):
    """Frequencies of sentiment-bearing words across the curated set.

    Only words carrying a VADER valence are counted, so the cloud shows the emotional
    vocabulary of the coverage rather than its nouns. Each word's polarity comes from the
    lexicon, which is what lets the view split positive from negative.
    """
    if _analyzer is None:
        return []
    lexicon = _analyzer.lexicon
    counts = defaultdict(int)
    for a in articles:
        text = "{} {}".format(a.get("headline", ""), a.get("summary", "")).lower()
        # Per-article set: one article repeating a word should not dominate the cloud.
        for token in set(_TOKEN_RE.findall(text)):
            if token in _CLOUD_STOPWORDS:
                continue
            valence = lexicon.get(token)
            if valence is None or abs(valence) < 0.5:
                continue
            counts[token] += 1

    words = [
        {
            "text": w,
            "count": c,
            "valence": round(float(lexicon[w]), 2),
            "sentiment": "positive" if lexicon[w] > 0 else "negative",
        }
        for w, c in counts.items()
    ]
    # Most frequent first, then strongest sentiment, so ties break toward vivid words.
    words.sort(key=lambda x: (-x["count"], -abs(x["valence"])))
    return words[:max_words]


def build_news_response(days=7, limit=40, min_relevance=None):
    """Return {articles, summary, wordCloud, fetchedAt}, curated down to NVIDIA-relevant
    coverage. `articles` is sorted by relevance then recency."""
    days = max(1, min(int(days or 7), 30))
    limit = max(1, min(int(limit or 40), 100))
    floor = _MIN_RELEVANCE_DEFAULT if min_relevance is None else max(0.0, min(float(min_relevance), 1.0))

    cache_key = "{}:{}:{}".format(days, limit, floor)
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

    raw.sort(key=lambda a: a.get("datetime", 0), reverse=True)
    seen_keys = set()
    articles = []
    considered = 0
    below_floor = 0
    for a in raw:
        headline = (a.get("headline") or "").strip()
        if not headline:
            continue
        key = _normalise_headline(headline)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        considered += 1

        summary = (a.get("summary") or "").strip()
        blob = "{} {}".format(headline, summary)
        topics = _topics_for(blob)
        relevance = _relevance(headline, summary, topics)
        if relevance < floor:
            below_floor += 1
            continue

        compound = _score_article(headline, summary)
        articles.append({
            "headline": headline,
            "summary": summary,
            "source": a.get("source") or "",
            "url": a.get("url") or "",
            "image": a.get("image") or "",
            "category": a.get("category") or "",
            "datetime": int(a.get("datetime") or 0) * 1000,  # ms for the frontend
            "sentiment": _label(compound),
            "sentimentScore": compound,
            "relevance": relevance,
            "topics": topics,
        })

    if floor <= 0:
        # "Show everything" means the feed as it arrives — newest first. Keeping the
        # relevance sort here would return the same top slice as the curated view and
        # make the toggle look broken.
        articles.sort(key=lambda x: -x["datetime"])
    else:
        # Most relevant first; recency breaks ties so a stale but on-point piece cannot
        # permanently outrank today's news.
        articles.sort(key=lambda x: (-x["relevance"], -x["datetime"]))
    # The cloud reads every article that cleared the relevance bar, not just the page's
    # worth we display, so word counts are high enough to give the sizes real spread.
    cloud_source = articles
    articles = articles[:limit]

    pos = sum(1 for a in articles if a["sentiment"] == "positive")
    neg = sum(1 for a in articles if a["sentiment"] == "negative")
    neu = len(articles) - pos - neg

    # Relevance-weighted: a headline squarely about NVIDIA should move the needle more
    # than one that merely mentions it.
    weight_sum = sum(a["relevance"] for a in articles)
    avg = round(sum(a["sentimentScore"] * a["relevance"] for a in articles) / weight_sum, 4) if weight_sum else 0.0

    summary_block = {
        "count": len(articles),
        "positive": pos,
        "negative": neg,
        "neutral": neu,
        "avgScore": avg,
        # 0-100 friendly score (50 = neutral).
        "score100": round((avg + 1) / 2 * 100, 1),
        "overall": _label(avg),
        "analyzerAvailable": _analyzer is not None,
        # Removed by the relevance floor specifically — not the extra articles that
        # merely fell past the page limit, which the UI must not describe as off-topic.
        "considered": considered,
        "filteredOut": below_floor,
        "minRelevance": floor,
    }
    payload = {
        "articles": articles,
        "summary": summary_block,
        "wordCloud": _word_cloud(cloud_source),
        "fetchedAt": int(now * 1000),
        "rangeDays": days,
    }
    _CACHE[cache_key] = (now, payload)
    return payload
