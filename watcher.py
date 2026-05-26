"""
watcher.py — TSX/TSXV intraday press release signal system

Based on the strategy: small caps (<$300M market cap) with limited analyst coverage,
releasing news DURING market hours (9:30–16:00 ET). Speed of analysis and execution
is the edge — the market takes time to price these in. Hold 1–3 days, NOT intraday.

Catalysts monitored:
  • Earnings vs consensus (beats / misses)
  • Drill results (oil, gas, gold, copper, silver)
  • Forward guidance updates
  • Extraordinary events: NCIB, dividend increases, bought deals, acquisitions
  • BNN Market Call analyst picks (Eric Nuttall energy picks, etc.)

When a signal fires:
  • macOS desktop notification (ticker, signal, price)
  • Terminal summary with signal, score, keywords, and hold suggestion
  • Entry logged to data/signals/signals.log

How it works:
  • Polls GlobeNewswire Canada + Accesswire RSS every 5 min during market hours
  • Monitors BNN Market Call podcast feed for analyst top picks
  • Checks CIRO halt page before alerting — no signal if stock is halted
  • Zero API keys, ~80 RSS requests/day — no rate limit risk
  • Seen URLs persisted to disk — no duplicate alerts across restarts

Usage:
    python3 watcher.py              # run until 4:00pm ET
    python3 watcher.py --test       # fire a test notification and exit
    python3 watcher.py --url <URL>  # manually score any press release URL
    python3 watcher.py --all-hours  # skip market-hours guard (for testing)
    python3 watcher.py --verbose    # show every RSS item checked

Install (once):
    pip3 install feedparser yfinance requests beautifulsoup4
"""
from __future__ import annotations

import argparse, json, os, re, subprocess, time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import feedparser
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────────────
EASTERN      = ZoneInfo("America/Toronto")
MARKET_OPEN  = (9, 30)
MARKET_CLOSE = (16, 0)
POLL_SECS    = 300       # 5 minutes — ~78 requests/day per feed

SEEN_FILE    = Path("data/signals/seen_urls.json")
LOG_FILE     = Path("data/signals/signals.log")
CACHE_FILE   = Path("data/signals/halt_cache.json")
PR_LOG_DIR   = Path("data/signals/press_releases")  # stores raw PR text for future backtest
TG_CONFIG    = Path("data/signals/telegram_config.json")

# ── RSS feeds ──────────────────────────────────────────────────────────────────
PRESS_RELEASE_FEEDS = [
    "https://www.globenewswire.com/RssFeed/country/Canada",
    # Accesswire RSS removed — feed went dead May 2026
]

# BNN Market Call podcast — analyst top picks (Eric Nuttall energy, etc.)
BNN_MARKET_CALL_FEED = "https://omny.fm/shows/market-call/playlists/podcast.rss"

# CIRO (formerly IIROC) halt page — scraped before each alert
CIRO_HALT_URL = "https://www.ciro.ca/newsroom/halts-and-resumptions"

# ── Small-cap ticker universe (<$300M market cap, limited analyst coverage) ───
# Strategy only applies to small caps — large/mid caps have too much coverage.
# Add tickers here as you discover new names. All should be TSX or TSXV.
TICKERS = [
    # ── TSX Small Cap Energy (<$300M, limited analyst coverage) ──────────────
    "PRQ.TO",    # Perpetual Energy       ~$283M
    "BNE.TO",    # Bonterra Energy        ~$228M
    "KEI.TO",    # Kelt Exploration       ~$247M
    "PNE.TO",    # Pine Cliff Energy      ~$235M
    "JOY.TO",    # Journey Energy         ~$335M  (borderline — active quarterly earnings)

    # ── TSXV Energy ──────────────────────────────────────────────────────────
    "HME.V",     # Hemisphere Energy      ~$257M  quarterly earnings, active driller
    "ALV.V",     # Alvopetro Energy       ~$328M  Brazil-focused, quarterly results
    "PCQ.V",     # Pacific Coal Resources ~$14M
    "TAO.V",     # Tidewater Renewables   ~$20M   micro-cap, watch liquidity
    "PUL.V",     # Pulse Oil              ~$9M    micro-cap, watch liquidity

    # ── TSX Small Cap Mining ─────────────────────────────────────────────────
    "GMX.TO",    # Gold Mountain Mining   ~$138M
    "ORV.TO",    # Orvana Minerals        ~$245M  gold/copper/silver — all three metals

    # ── TSXV Mining — Gold/Silver ─────────────────────────────────────────────
    "SAG.V",     # Strikepoint Gold       — cited in original strategy conversation
    "AHR.V",     # American Helium        ~$223M
    "AGX.V",     # Argo Gold              ~$206M
    "GSP.V",     # Gossan Resources       ~$74M
    "AZM.V",     # Azimut Exploration     ~$71M   active explorer, frequent drill news
    "BHS.V",     # Bayhorse Silver        ~$29M
    # GLD.V removed — D+1 avg -1.19%, 29% win rate (worst in universe)
    # GSP.V removed — 19% win rate during-market, consistently negative

    # ── TSXV Mining — Copper ─────────────────────────────────────────────────
    "KDK.V",     # Kodiak Copper          ~$85M   active drill program
    "SURG.V",    # Surge Copper           ~$215M  active drill program
    "CUU.V",     # Copper Fox Metals      ~$352M  (borderline — active Schaft Creek project)

    # ── TSXV / TSX Other Small Cap ───────────────────────────────────────────
    "RVX.TO",    # Resverlogix            ~$30M
    "USCU.V",    # US Copper              ~$25M
    "ABR.V",     # Aberdeen International ~$17M
    "MCS.V",     # Miners Capital         ~$13M
    "AFM.V",     # Alphamin Resources     ~$1.6B  (large TSXV — limited TSX coverage)
]

# ── Company name → ticker (used to match press release headlines) ──────────────
COMPANY_NAMES = {
    # TSX Energy
    "PRQ.TO":  "Perpetual Energy",
    "BNE.TO":  "Bonterra Energy",
    "KEI.TO":  "Kelt Exploration",
    "PNE.TO":  "Pine Cliff Energy",
    "JOY.TO":  "Journey Energy",
    # TSXV Energy
    "HME.V":   "Hemisphere Energy",
    "ALV.V":   "Alvopetro Energy",
    "PCQ.V":   "Pacific Coal Resources",
    "TAO.V":   "Tidewater Renewables",
    "PUL.V":   "Pulse Oil",
    # TSX Mining
    "GMX.TO":  "Gold Mountain Mining",
    "ORV.TO":  "Orvana Minerals",
    "RVX.TO":  "Resverlogix",
    # TSXV Mining — Gold/Silver
    "SAG.V":   "Strikepoint Gold",
    "AHR.V":   "American Helium",
    "AGX.V":   "Argo Gold",
    "GSP.V":   "Gossan Resources",
    "AZM.V":   "Azimut Exploration",
    "BHS.V":   "Bayhorse Silver",
    "GLD.V":   "Goldstrike Resources",
    # TSXV Mining — Copper
    "KDK.V":   "Kodiak Copper",
    "SURG.V":  "Surge Copper",
    "CUU.V":   "Copper Fox Metals",
    # TSXV Other
    "USCU.V":  "US Copper",
    "ABR.V":   "Aberdeen International",
    "MCS.V":   "Miners Capital",
    "AFM.V":   "Alphamin Resources",
}

SECTOR_MAP = {
    "PRQ.TO": "Energy", "BNE.TO": "Energy", "KEI.TO": "Energy",
    "PNE.TO": "Energy", "JOY.TO": "Energy", "HME.V":  "Energy",
    "ALV.V":  "Energy", "PCQ.V":  "Energy", "TAO.V":  "Energy",
    "PUL.V":  "Energy",
}
# All others default to "Mining" (see get_sector())

def get_sector(ticker: str) -> str:
    return SECTOR_MAP.get(ticker, "Mining")


# ── Keyword banks ───────────────────────────────────────────────────────────────

EARNINGS_KW = [
    "quarterly results", "annual results", "financial results",
    "q1 ", "q2 ", "q3 ", "q4 ",
    "fourth quarter", "third quarter", "second quarter", "first quarter",
    "earnings per share", "net income", "revenue", "operating cash flow",
    "funds from operations", "cash flow from operations", "net earnings",
    "adjusted earnings",
]

# Drill results: oil/gas + gold/copper/silver (both commodity types per strategy)
DRILL_KW = [
    # Mining / exploration
    "drill results", "assay results", "drill program", "intercepts",
    "mineralization", "resource estimate", "reserve estimate",
    "grams per tonne", "g/t", "metres of", "meters of",
    "hole ", "intersection", "drill hole", "drill intercept",
    # Oil & gas specific
    "well results", "production test", "flow rate", "barrels per day",
    "boe/d", "mcf/d", "completion results", "spud",
]

GUIDANCE_KW = [
    "guidance", "outlook", "forecast", "production target", "capex budget",
    "full year", "next quarter", "2025", "2026", "going forward",
    "raises guidance", "updates guidance", "revised guidance",
]

# Positive catalysts — earnings beats, guidance raises, extraordinary events
POSITIVE_KW = [
    # Earnings beats — explicit consensus language only (not standalone "beats")
    "beats consensus", "beat consensus", "exceeds expectations",
    "exceeds consensus", "surpasses consensus", "above consensus",
    "above expectations", "stronger than expected",
    # Record results — specific to financial/operational context
    "record quarter", "record revenue", "record production",
    "record cash flow", "record earnings", "record sales",
    "record throughput", "record output",
    # Guidance upgrades
    "raises guidance", "increases guidance", "raises production",
    "increases production guidance", "raises full-year",
    # Extraordinary events (per strategy: NCIB, dividend increases)
    "increases dividend", "raises dividend", "special dividend",
    "declares dividend", "normal course issuer bid", "ncib",
    "substantial issuer bid",
    # Deals / capital / M&A — specific phrases only (not standalone "acquisition")
    "bought deal", "binding agreement", "strategic review",
    "letter of intent", "definitive agreement", "merger agreement",
    "takeover bid", "signs agreement", "joint venture agreement",
    "partnership agreement", "completes acquisition", "successful acquisition",
    # Drill / resource positives
    "significant intercept", "high grade", "broad zone",
    "expands resource", "increases reserve",
    "intersects", "metres of", "meters of", "g/t au", "g/t gold",
    "cueq", "copper equivalent", "maiden resource", "initial resource",
    "mineralized", "visible gold", "new discovery",
    # Positive financials (implicit — no consensus language for small caps)
    "revenue increased", "revenue grew", "production increased",
    "cash flow increased", "income increased", "profit increased",
    "year-over-year increase", "quarter-over-quarter increase",
]

NEGATIVE_KW = [
    # Earnings misses — explicit
    "misses consensus", "miss consensus", "below consensus",
    "below expectations", "shortfall", "disappoints",
    "below plan", "below budget", "weaker than expected",
    # Implicit misses (small caps don't say "misses consensus")
    "net loss", "revenue declined", "revenue decreased", "revenue fell",
    "production declined", "production decreased", "cash flow declined",
    "year-over-year decrease", "year-over-year decline",
    "compared to a loss", "wider loss", "increased loss",
    # Guidance cuts
    "reduces guidance", "lowers guidance", "cuts guidance",
    "revised lower", "below previous guidance",
    # Capital events
    "suspends dividend", "eliminates dividend", "cuts dividend",
    # Writedowns / charges
    "impairment", "write-down", "write-off", "restructuring charge",
    # Operations — specific phrases only (not standalone "delays")
    "production shortfall", "cost overrun", "operational challenges",
    "force majeure", "production delays", "construction delays",
    "project delays", "trading halt",
    # Dilutive / bad M&A
    "dilutive acquisition", "failed acquisition", "acquisition falls through",
]

# BNN analyst pick keywords (for Market Call podcast episode titles/descriptions)
BNN_POSITIVE_KW = [
    "top pick", "best idea", "buy", "strong buy",
    "eric nuttall", "energy pick", "junior",
]


# ══════════════════════════════════════════════════════════════════════════════
# Claude AI press release analysis
# Optional — requires ANTHROPIC_API_KEY env var.  Falls back to keyword scoring.
# Uses claude-haiku-4-5 (cost-efficient for automated per-PR calls).
# System prompt is prompt-cached — only charged once per cache TTL (5 min).
# ══════════════════════════════════════════════════════════════════════════════

_CLAUDE_SYSTEM_PROMPT = """\
You are a specialized financial analyst for TSX and TSXV small-cap stocks (<$300M market cap).
Your task: read a press release and output a trading signal for a 1-3 day momentum hold.

STRATEGY CONTEXT
- Universe: TSX/TSXV small caps with limited analyst coverage (≤$300M market cap)
- A signal fires when the stock is already up ≥15% intraday (mining) or ≥10% (energy)
- Hold: 1-3 days NOT intraday — the edge is that small caps take days to fully price in news
- Backtest (1929 events): ≥15% mining D+1 avg +8.47%, win 63%

SIGNAL DEFINITIONS
- STRONG BUY  : Unambiguously positive catalyst with guidance upgrade, exceptional drill result,
                record financials, or major deal. High conviction.
- BUY         : Clear positive catalyst — beats expectations, solid drill intercept,
                M&A/deal announcement, record production/revenue, dividend increase.
- CAUTION     : Mixed signals, unclear catalyst, or boilerplate news with no financial substance.
- SKIP        : Negative catalyst — missed expectations, guidance cut, net loss, declining metrics,
                production delays, dilutive equity raise, trading halt.

SECTOR HEURISTICS
Mining (gold, silver, copper):
  - Positive: high-grade drill intercepts (g/t Au, CuEq%), wide mineralized zones, maiden/expanded
    resource estimate, M&A at premium, visible gold, new discovery
  - Negative: no significant intercepts, resource downgrade, failed drilling, mine closure
Energy (oil, gas):
  - Positive: production beat, earnings beat vs consensus, guidance raise, deal at premium
  - Negative: production miss, net loss, guidance cut, cost overrun, force majeure

OUTPUT FORMAT — return ONLY valid JSON, no markdown fences, no extra text:
{
  "signal": "BUY" | "STRONG BUY" | "CAUTION" | "SKIP",
  "confidence": 0.0-1.0,
  "reasoning": "One concise sentence explaining the primary catalyst or concern.",
  "key_numbers": {"metric_name": "value_with_unit"},
  "release_type": "earnings" | "drill" | "guidance" | "deal" | "other"
}"""

_anthropic_client = None


def _get_anthropic_client():
    """Lazy-initialize Anthropic client. Returns None if not configured."""
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return None
        try:
            import anthropic
            _anthropic_client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            return None
    return _anthropic_client


def analyze_with_claude(title: str, body: str | None, ticker: str, sector: str) -> dict | None:
    """
    Analyze a press release with Claude AI.

    Returns a dict with keys: signal, confidence, reasoning, key_numbers, release_type.
    Returns None if Claude is not configured (caller falls back to keyword scoring).

    System prompt is prompt-cached — first call in a 5-min window is ~$0.000025 (haiku input);
    subsequent cache hits cost ~$0.000003.  At 12 polls/hour that's ~$0.04/trading day.
    """
    client = _get_anthropic_client()
    if client is None:
        return None

    content = (
        f"Ticker: {ticker}\nSector: {sector}\n\n"
        f"Headline: {title}\n\n"
        + (f"Press release body:\n{body[:3000]}" if body else "(Headline only — body unavailable)")
    )

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=512,
            system=[
                {
                    "type": "text",
                    "text": _CLAUDE_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": content}],
        )
        text = response.content[0].text.strip()
        # Strip markdown fences if model adds them
        text = re.sub(r"```(?:json)?\n?", "", text).strip("`").strip()
        result = json.loads(text)
        # Validate required fields
        assert result.get("signal") in ("BUY", "STRONG BUY", "CAUTION", "SKIP")
        assert 0.0 <= float(result.get("confidence", 0)) <= 1.0
        result["confidence"] = float(result["confidence"])
        return result
    except Exception:
        return None


def _claude_to_analysis(claude: dict) -> dict:
    """Convert Claude result to the same shape as score_release() output."""
    return {
        "signal":       claude["signal"],
        "score":        {"STRONG BUY": 3, "BUY": 1, "CAUTION": 0, "SKIP": -2}[claude["signal"]],
        "release_type": claude.get("release_type", "other"),
        "has_guidance": claude.get("release_type") == "guidance",
        "pos_hits":     [],   # keyword hits not applicable for AI analysis
        "neg_hits":     [],
        # Extra Claude-only fields
        "ai_confidence": claude.get("confidence", 0.0),
        "ai_reasoning":  claude.get("reasoning", ""),
        "ai_key_numbers": claude.get("key_numbers", {}),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def is_market_hours(dt: datetime | None = None) -> bool:
    now = dt or datetime.now(EASTERN)
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE


# ══════════════════════════════════════════════════════════════════════════════
# Telegram notifications
# ══════════════════════════════════════════════════════════════════════════════

def _load_tg_config() -> dict | None:
    # 1. Local config file (Mac)
    if TG_CONFIG.exists():
        with open(TG_CONFIG) as f:
            return json.load(f)
    # 2. Environment variables (GitHub Actions / cloud)
    import os
    token   = os.environ.get("TELEGRAM_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        return {"token": token, "chat_id": int(chat_id)}
    return None


def _tg_escape(text: str) -> str:
    """Strip characters that break Telegram Markdown (V1): _, *, [, ], (, )"""
    return text.replace("_", " ").replace("*", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")


def send_telegram(text: str) -> None:
    """Send a message to Telegram. Silent fail if not configured."""
    cfg = _load_tg_config()
    if not cfg:
        return
    try:
        # Telegram Markdown V1 limit is 4096 chars; truncate safely
        if len(text) > 4000:
            text = text[:3997] + "..."
        requests.post(
            f"https://api.telegram.org/bot{cfg['token']}/sendMessage",
            data={"chat_id": cfg["chat_id"], "text": text, "parse_mode": "Markdown"},
            timeout=8,
        )
    except Exception:
        pass


def tg_signal(r: dict) -> None:
    """Format and send a signal as a Telegram message."""
    emoji = {"STRONG BUY": "🟢🟢", "BUY": "🟢", "BNN PICK": "📺", "CAUTION": "🟡", "SKIP": "🔴"}.get(r["signal"], "")
    intra = r.get("intraday_pct")
    price_str = f"${r['price']}" if r.get("price") else "n/a"
    move_str  = f" ({intra:+.1f}% intraday)" if intra is not None else ""
    pos = ", ".join(r["pos_hits"][:3]) or "none"

    safe_company = _tg_escape(r['company'])
    safe_title   = _tg_escape(r['title'][:100])

    lines = [
        f"{emoji} *{r['signal']}* — {r['ticker']}",
        f"{safe_company}",
        f"",
        f"*Price:* {price_str}{move_str}",
        f"*Type:* {r['release_type'].upper()}  |  Score: {r['score']:+d}",
    ]

    # AI reasoning block (when Claude analysis was used)
    if r.get("ai_used") and r.get("ai_reasoning"):
        conf = r.get("ai_confidence")
        conf_str = f" ({conf:.0%})" if conf is not None else ""
        safe_reason = _tg_escape(r["ai_reasoning"][:120])
        lines.append(f"*AI{conf_str}:* {safe_reason}")
        if r.get("ai_key_numbers"):
            nums = "  ".join(f"{k}: {v}" for k, v in list(r["ai_key_numbers"].items())[:3])
            lines.append(f"*Numbers:* {_tg_escape(nums)}")
    else:
        lines.append(f"*Positive:* {pos}")

    lines += [
        f"*Exit:* Green D+1 hold D+3 | Red D+1 cut",
        f"",
        safe_title,
        r.get("url", ""),
    ]
    send_telegram("\n".join(l for l in lines if l))


def tg_market_open(n_tickers: int) -> None:
    send_telegram(
        f"📈 *TSX Watcher — Market Open*\n"
        f"Watching {n_tickers} small-cap tickers\n"
        f"Polling GlobeNewswire + BNN Market Call every 5 min\n"
        f"Market hours: 9:30-16:00 ET"
    )


def tg_market_close(n_signals: int) -> None:
    if n_signals == 0:
        send_telegram("📉 *TSX Watcher — Market Closed*\nNo signals today.")
    else:
        send_telegram(
            f"📉 *TSX Watcher — Market Closed*\n"
            f"{n_signals} signal(s) fired today — check signals.log for details."
        )


def notify_macos(title: str, subtitle: str, message: str) -> None:
    def esc(s): return s.replace('"', "'").replace('\\', '\\\\')
    script = (
        f'display notification "{esc(message)}" '
        f'with title "{esc(title)}" '
        f'subtitle "{esc(subtitle)}" '
        f'sound name "Glass"'
    )
    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)
    except FileNotFoundError:
        pass


def load_seen() -> set:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    if SEEN_FILE.exists():
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    return set()


def save_seen(seen: set) -> None:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def log_signal(record: dict) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def log_press_release(ticker: str, event_date: str, title: str, body: str | None, url: str) -> None:
    """
    Save raw press release text to disk alongside every signal.
    This builds a ground-truth dataset for validating keyword scoring over time.
    File: data/signals/press_releases/{YYYY-MM-DD}_{ticker}_{slug}.json
    """
    if not body:
        return
    PR_LOG_DIR.mkdir(parents=True, exist_ok=True)
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower())[:40].strip("-")
    fname = PR_LOG_DIR / f"{event_date}_{ticker.replace('.', '')}_{slug}.json"
    record = {
        "ticker":     ticker,
        "event_date": event_date,
        "title":      title,
        "url":        url,
        "body":       body[:8000],
    }
    with open(fname, "w") as f:
        json.dump(record, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# CIRO halt check
# Strategy: "it really works as long as there is no trading halt"
# ══════════════════════════════════════════════════════════════════════════════

_halt_cache: dict = {}   # {ticker: bool} — refreshed each poll cycle


def refresh_halts(verbose: bool = False) -> None:
    """Scrape CIRO halt page and cache which tickers are currently halted."""
    global _halt_cache
    try:
        r = requests.get(
            CIRO_HALT_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if r.status_code != 200:
            if verbose:
                print(f"  CIRO halt page returned {r.status_code}")
            return
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True).upper()
        new_cache = {}
        for ticker in TICKERS:
            sym = ticker.replace(".TO", "").replace(".V", "")
            new_cache[ticker] = sym in text
        _halt_cache = new_cache
        halted = [t for t, h in new_cache.items() if h]
        if halted and verbose:
            print(f"  Currently halted: {halted}")
    except Exception as e:
        if verbose:
            print(f"  CIRO halt check failed: {e}")


def is_halted(ticker: str) -> bool:
    return _halt_cache.get(ticker, False)


# ══════════════════════════════════════════════════════════════════════════════
# Press release fetching
# ══════════════════════════════════════════════════════════════════════════════

FETCH_HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0", "Accept": "text/html,*/*"}
WIRE_DOMAINS  = ("globenewswire", "prnewswire", "businesswire", "accesswire", "newswire")


def fetch_body(url: str) -> str | None:
    try:
        r = requests.get(url, headers=FETCH_HEADERS, timeout=12, allow_redirects=True)
        if r.status_code != 200 or len(r.content) < 2000:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for sel in [
            lambda s: s.find("div", class_=lambda c: c and "article-body" in c),
            lambda s: s.find("div", class_="main-scroll-container"),
            lambda s: s.find("article"),
            lambda s: s.find("main"),
        ]:
            el = sel(soup)
            if el:
                text = re.sub(r"\s+", " ", el.get_text(separator=" ", strip=True))
                if len(text) > 200:
                    return text[:5000]
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Ticker matching
# ══════════════════════════════════════════════════════════════════════════════

def match_ticker(title: str, summary: str) -> str | None:
    text = (title + " " + summary).lower()

    # 1. Full company name substring
    for ticker, name in COMPANY_NAMES.items():
        if name.lower() in text:
            return ticker

    # 2. Explicit ticker symbol in text: (PRQ.TO), PRQ.V, etc.
    for pat in [r'\(([A-Z]{2,7})\.(?:TO|V)\)', r'\b([A-Z]{2,7})\.(?:TO|V)\b']:
        for m in re.finditer(pat, title + " " + summary):
            sym_to = m.group(1) + ".TO"
            if sym_to in TICKERS: return sym_to
            sym_v  = m.group(1) + ".V"
            if sym_v  in TICKERS: return sym_v

    return None


# ══════════════════════════════════════════════════════════════════════════════
# Signal scoring — fast keyword rules
# Speed is the edge. No ML. Runs in milliseconds.
# ══════════════════════════════════════════════════════════════════════════════

def score_release(title: str, body: str | None) -> dict:
    text = (title + " " + (body or "")).lower()

    # Classify release type
    if any(k in text for k in EARNINGS_KW):
        release_type = "earnings"
    elif any(k in text for k in DRILL_KW):
        release_type = "drill"
    elif any(k in text for k in GUIDANCE_KW):
        release_type = "guidance"
    else:
        release_type = "other"

    pos_hits     = [k for k in POSITIVE_KW if k in text]
    neg_hits     = [k for k in NEGATIVE_KW if k in text]
    has_guidance = any(k in text for k in GUIDANCE_KW)
    score        = len(pos_hits) - len(neg_hits)

    # Signal logic
    if score >= 2 and has_guidance:
        signal = "STRONG BUY"
    elif score > 0:
        signal = "BUY"
    elif score < 0:
        signal = "SKIP"
    else:
        signal = "CAUTION"

    return {
        "signal":       signal,
        "score":        score,
        "release_type": release_type,
        "has_guidance": has_guidance,
        "pos_hits":     pos_hits[:6],
        "neg_hits":     neg_hits[:6],
    }


# ══════════════════════════════════════════════════════════════════════════════
# BNN Market Call monitoring
# Strategy: "watch when Eric Nuttall is on BNN — stock will trade higher"
# Monitors the podcast RSS for episodes mentioning tracked tickers/sectors
# ══════════════════════════════════════════════════════════════════════════════

def check_bnn_feed(seen: set, verbose: bool = False) -> list[dict]:
    signals = []
    try:
        feed = feedparser.parse(BNN_MARKET_CALL_FEED)
    except Exception as e:
        if verbose:
            print(f"  BNN feed error: {e}")
        return []

    for entry in feed.entries:
        url     = entry.get("link", "") or entry.get("id", "")
        title   = entry.get("title", "")
        summary = (entry.get("summary", "") or "")

        if not url or url in seen:
            continue
        seen.add(url)

        text = (title + " " + summary).lower()

        # Look for tracked tickers mentioned by name in the episode
        ticker = match_ticker(title, summary)
        if ticker is None:
            # Also check for sector-level energy picks (Eric Nuttall pattern)
            if not any(k in text for k in BNN_POSITIVE_KW):
                continue
            # Sector-level alert — no specific ticker
            record_base = {
                "timestamp":    datetime.now(EASTERN).strftime("%Y-%m-%d %H:%M ET"),
                "ticker":       "SECTOR",
                "company":      "BNN Market Call",
                "sector":       "Energy" if "energy" in text else "Mining",
                "signal":       "BNN PICK",
                "score":        1,
                "release_type": "analyst_pick",
                "has_guidance": False,
                "pos_hits":     [k for k in BNN_POSITIVE_KW if k in text],
                "neg_hits":     [],
                "price":        None,
                "title":        title[:120],
                "url":          url,
                "hold_days":    "1–3",
                "source":       "BNN Market Call",
            }
            signals.append(record_base)
            continue

        if is_halted(ticker):
            if verbose:
                print(f"  Skipping {ticker} — currently halted")
            continue

        price = get_current_price(ticker)
        signals.append({
            "timestamp":    datetime.now(EASTERN).strftime("%Y-%m-%d %H:%M ET"),
            "ticker":       ticker,
            "company":      COMPANY_NAMES.get(ticker, ticker),
            "sector":       get_sector(ticker),
            "signal":       "BNN PICK",
            "score":        1,
            "release_type": "analyst_pick",
            "has_guidance": False,
            "pos_hits":     [k for k in BNN_POSITIVE_KW if k in text],
            "neg_hits":     [],
            "price":        price,
            "title":        title[:120],
            "url":          url,
            "hold_days":    "1–3",
            "source":       "BNN Market Call",
        })

    return signals


# ══════════════════════════════════════════════════════════════════════════════
# Live price
# ══════════════════════════════════════════════════════════════════════════════

# ── Move gate thresholds (stress-tested against 1929-event backtest_v2) ────────
# Test set (2024-2026) results by threshold:
#   ≥10%: D+1 +6.24%, win 58%   ≥14%: +8.47%, win 63%   ≥16%: +9.51%, win 66%
# Raised from 10% → 15% for mining after OOS validation.
# Energy keeps 10% (energy upgrade rule was removed — sector treated uniformly now).
# Upper cap at 40%: stocks up 40%+ intraday tend to REVERSE (close-entry D+1 = -9.5%).
MIN_INTRADAY_MOVE   = 0.15   # mining: ≥15% intraday required
MIN_INTRADAY_ENERGY = 0.10   # energy: ≥10% (slightly lower — sector still more consistent)
MAX_INTRADAY_MOVE   = 0.40   # cap: 40%+ intraday movers tend to reverse

# ── Dollar volume minimum ──────────────────────────────────────────────────────
# Stress test: 35% of backtest events were sub-$0.25 stocks — IBKR fee eats 1.75%/side.
# Require $50k daily dollar volume to ensure meaningful liquidity.
MIN_DOLLAR_VOL = 50_000

def get_price_data(ticker: str) -> dict | None:
    """Returns current price, today's open, intraday move %, and estimated dollar volume."""
    try:
        hist = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=True)
        if hist.empty:
            return None
        current  = round(float(hist["Close"].iloc[-1]), 2)
        open_px  = round(float(hist["Open"].iloc[0]), 2)
        intraday = (current - open_px) / open_px if open_px > 0 else 0.0
        # Estimate dollar volume: sum of (close * volume) across 1-min bars today
        dvol = float((hist["Close"] * hist["Volume"]).sum())
        return {
            "price":        current,
            "open":         open_px,
            "intraday_pct": round(intraday * 100, 2),
            "intraday_abs": round(abs(intraday) * 100, 2),
            "dollar_vol":   round(dvol, 0),
        }
    except Exception:
        return None


def get_current_price(ticker: str) -> float | None:
    d = get_price_data(ticker)
    return d["price"] if d else None


# ══════════════════════════════════════════════════════════════════════════════
# Press release RSS polling
# ══════════════════════════════════════════════════════════════════════════════

def poll_press_releases(seen: set, verbose: bool = False) -> list[dict]:
    signals = []

    for feed_url in PRESS_RELEASE_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
        except Exception as e:
            if verbose:
                print(f"  Feed error ({feed_url}): {e}")
            continue

        for entry in feed.entries:
            url     = entry.get("link", "")
            title   = entry.get("title", "")
            summary = entry.get("summary", "") or ""

            if not url or url in seen:
                continue
            seen.add(url)

            if verbose:
                print(f"    checking: {title[:65]}")

            ticker = match_ticker(title, summary)
            if ticker is None:
                continue

            # Skip if stock is currently halted
            # Strategy: "it really works as long as there is no trading halt"
            if is_halted(ticker):
                if verbose:
                    print(f"  ⚠ {ticker} is halted — skipping")
                continue

            # Fetch full article body from wire services
            body = None
            if any(d in url for d in WIRE_DOMAINS):
                body = fetch_body(url)

            # ── Signal analysis: Claude AI first, keyword scoring as fallback ─
            sector = get_sector(ticker)
            claude_result = analyze_with_claude(title, body or summary, ticker, sector)
            if claude_result is not None:
                analysis = _claude_to_analysis(claude_result)
                ai_used  = True
            else:
                analysis = score_release(title, body or summary)
                ai_used  = False

            # Log PR body to disk — builds ground-truth dataset for keyword backtest
            today_str = datetime.now(EASTERN).strftime("%Y-%m-%d")
            log_press_release(ticker, today_str, title, body or summary, url)

            # Ignore generic corporate boilerplate with no clear signal
            if analysis["release_type"] == "other" and abs(analysis["score"]) == 0:
                continue

            # ── Intraday move gate + liquidity filters ────────────────────────
            px = get_price_data(ticker)
            price        = px["price"]        if px else None
            intraday_pct = px["intraday_pct"] if px else None
            intraday_abs = px["intraday_abs"] if px else 0.0
            dollar_vol   = px["dollar_vol"]   if px else 0.0
            # sector already set above when calling analyze_with_claude

            # Dollar volume filter: skip illiquid names (<$50k today)
            # IBKR fee on sub-$0.25 stocks = 1.75%/side — erases the edge entirely
            if px and dollar_vol < MIN_DOLLAR_VOL:
                if verbose:
                    print(f"  ↷ {ticker} filtered — dollar vol ${dollar_vol:,.0f} < ${MIN_DOLLAR_VOL:,}")
                continue

            # Move gate — lower bound: confirm something real is happening
            # OOS test data: ≥15% mining → D+1 +8.47%, win 63%. ≥10% energy → D+1 +6.24%, win 58%.
            threshold = MIN_INTRADAY_ENERGY if sector == "Energy" else MIN_INTRADAY_MOVE
            if px and intraday_abs < threshold * 100:
                if verbose:
                    print(f"  ↷ {ticker} filtered — intraday {intraday_abs:.1f}% < {threshold*100:.0f}% gate")
                continue

            # Move cap — upper bound: 40%+ intraday movers reverse sharply
            # Stress test: close-entry D+1 avg = -9.53% for 40%+ movers
            if px and intraday_abs >= MAX_INTRADAY_MOVE * 100:
                if verbose:
                    print(f"  ↷ {ticker} filtered — intraday {intraday_abs:.1f}% ≥ {MAX_INTRADAY_MOVE*100:.0f}% reversal zone")
                continue

            # Signal (no sector upgrade — energy upgrade removed after OOS validation failed)
            # Energy win rate advantage disappears in 2024-2026 test set (54% vs 48%, CIs overlap)
            sig = analysis["signal"]

            # Rule 3: dynamic exit guidance
            # Asymmetry confirmed OOS: D+1 green → D+3 win 81%. D+1 red → D+3 win 16%.
            # Note: cutting losers at D+1 reduces variance but lowers total P&L vs simple D+3 hold.
            exit_rule = "Hold to D+3 if green at D+1 close. Cut at D+1 close if red."

            signals.append({
                "timestamp":    datetime.now(EASTERN).strftime("%Y-%m-%d %H:%M ET"),
                "ticker":       ticker,
                "company":      COMPANY_NAMES.get(ticker, ticker),
                "sector":       sector,
                "signal":       sig,
                "score":        analysis["score"],
                "release_type": analysis["release_type"],
                "has_guidance": analysis["has_guidance"],
                "pos_hits":     analysis["pos_hits"],
                "neg_hits":     analysis["neg_hits"],
                "ai_used":      ai_used,
                "ai_reasoning": analysis.get("ai_reasoning", ""),
                "ai_confidence": analysis.get("ai_confidence", None),
                "ai_key_numbers": analysis.get("ai_key_numbers", {}),
                "price":        price,
                "intraday_pct": intraday_pct,
                "dollar_vol":   dollar_vol,
                "title":        title,
                "url":          url,
                "hold_days":    "1–3",
                "exit_rule":    exit_rule,
                "source":       "press_release",
            })

    return signals


# ══════════════════════════════════════════════════════════════════════════════
# Output formatting
# ══════════════════════════════════════════════════════════════════════════════

COLORS = {
    "STRONG BUY": "\033[92m",
    "BUY":        "\033[32m",
    "BNN PICK":   "\033[96m",
    "CAUTION":    "\033[33m",
    "SKIP":       "\033[31m",
}
RESET = "\033[0m"

EMOJI = {
    "STRONG BUY": "🟢🟢",
    "BUY":        "🟢",
    "BNN PICK":   "📺",
    "CAUTION":    "🟡",
    "SKIP":       "🔴",
}


def print_signal(r: dict) -> None:
    color     = COLORS.get(r["signal"], "")
    intra     = r.get("intraday_pct")
    dvol      = r.get("dollar_vol")
    price_str = (
        f"  Price    : ${r['price']}  (intraday {intra:+.1f}%"
        + (f"  |  vol ${dvol:,.0f}" if dvol else "")
        + ")"
        if r.get("price") and intra is not None
        else (f"  Price    : ${r['price']}" if r.get("price") else "")
    )
    pos       = ", ".join(r["pos_hits"][:4]) or "none"
    neg       = ", ".join(r["neg_hits"][:4]) or "none"
    exit_rule = r.get("exit_rule", "Hold to D+3 if green at D+1 close. Cut at D+1 if red.")

    # AI analysis line
    if r.get("ai_used") and r.get("ai_reasoning"):
        conf = r.get("ai_confidence")
        conf_str = f" {conf:.0%}" if conf is not None else ""
        ai_line = f"  AI{conf_str}    : {r['ai_reasoning'][:100]}"
        if r.get("ai_key_numbers"):
            nums = "  |  ".join(f"{k}: {v}" for k, v in list(r["ai_key_numbers"].items())[:4])
            ai_line += f"\n  Numbers  : {nums}"
    else:
        ai_line = f"  Positive : {pos}\n  Negative : {neg}"

    print(f"""
{'='*65}
{color}▶  {r['signal']}{RESET}   {r['ticker']}  —  {r['company']}
  Time     : {r['timestamp']}
  Type     : {r['release_type'].upper()}   Sector: {r['sector']}
  Score    : {r['score']:+d}   Guidance: {'YES ✓' if r['has_guidance'] else 'no'}   {'🤖 AI' if r.get('ai_used') else '🔑 Keywords'}
{price_str}
{ai_line}
  Exit     : {exit_rule}
  Headline : {r['title'][:80]}
  Link     : {r['url']}
{'='*65}""")


def fire_notification(r: dict) -> None:
    emoji     = EMOJI.get(r["signal"], "")
    intra     = r.get("intraday_pct")
    price_str = f"${r['price']}" if r.get("price") else ""
    move_str  = f" ({intra:+.1f}% intraday)" if intra is not None else ""
    notify_macos(
        title    = f"{emoji} {r['signal']} — {r['ticker']}",
        subtitle = f"{r['company']}  |  {r['release_type'].upper()}  |  {price_str}{move_str}",
        message  = f"Exit: green D+1→hold D+3, red D+1→cut. {r['title'][:80]}",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Manual URL scorer
# ══════════════════════════════════════════════════════════════════════════════

def run_manual_url(url: str) -> None:
    print(f"Fetching: {url}\n")
    body   = fetch_body(url)
    title  = url.split("/")[-1].replace("-", " ")
    ticker = match_ticker(title, body or "")
    sector = get_sector(ticker) if ticker else "Mining"
    price  = get_current_price(ticker) if ticker else None

    print(f"Ticker   : {ticker or 'not matched — add to COMPANY_NAMES?'}")
    print(f"Price    : ${price}" if price else "Price    : n/a")

    # Try Claude first
    claude_result = analyze_with_claude(title, body, ticker or "?", sector)
    if claude_result:
        conf = claude_result.get("confidence", 0)
        print(f"\n🤖 Claude Analysis ({conf:.0%} confidence):")
        print(f"  Signal   : {claude_result['signal']}")
        print(f"  Type     : {claude_result.get('release_type', 'other')}")
        print(f"  Reasoning: {claude_result.get('reasoning', '')}")
        if claude_result.get("key_numbers"):
            for k, v in claude_result["key_numbers"].items():
                print(f"  {k}: {v}")
    else:
        print("\n🔑 Keyword Analysis (Claude not configured):")
        result = score_release(title, body)
        print(f"  Signal   : {result['signal']}  (score {result['score']:+d})")
        print(f"  Type     : {result['release_type']}")
        print(f"  Guidance : {'YES' if result['has_guidance'] else 'no'}")
        print(f"  Positive : {', '.join(result['pos_hits']) or 'none'}")
        print(f"  Negative : {', '.join(result['neg_hits']) or 'none'}")

    print(f"\nHold     : 1–3 days (NOT intraday)")
    if body:
        print(f"\nRelease preview (500 chars):\n{body[:500]}")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    p = argparse.ArgumentParser(
        description="TSX/TSXV small-cap press release watcher — signals during market hours"
    )
    p.add_argument("--test",      action="store_true", help="Fire a test notification and exit")
    p.add_argument("--all-hours", action="store_true", help="Ignore market hours check")
    p.add_argument("--url",       type=str,            help="Manually score a press release URL")
    p.add_argument("--verbose",   action="store_true", help="Show all RSS items checked")
    args = p.parse_args()

    if args.url:
        run_manual_url(args.url)
        return

    if args.test:
        print("Firing test notification...")
        notify_macos(
            title    = "🟢🟢 STRONG BUY — SAG.V",
            subtitle = "Strikepoint Gold  |  DRILL  |  Hold 1–3 days",
            message  = "$0.42 | Significant intercept: 18.5 g/t Au over 12.3m, raises guidance",
        )
        print("Done — check your notification centre.")
        return

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    seen = load_seen()

    print(f"TSX/TSXV Small-Cap Watcher  —  {datetime.now(EASTERN).strftime('%Y-%m-%d %H:%M ET')}")
    print(f"Watching {len(TICKERS)} small-cap tickers (<$300M market cap)")
    print(f"Feeds    : {len(PRESS_RELEASE_FEEDS)} press release + BNN Market Call")
    print(f"Interval : every {POLL_SECS // 60} min during market hours")
    print(f"Hours    : 9:30–16:00 ET, Mon–Fri  (--all-hours to override)")
    print(f"Log      : {LOG_FILE}")
    print(f"Telegram : {'configured ✓' if _load_tg_config() else 'not configured (run setup_telegram.py)'}")
    print(f"AI       : {'Claude claude-haiku-4-5 ✓ (prompt-cached)' if _get_anthropic_client() else 'not configured — using keyword scoring (set ANTHROPIC_API_KEY)'}")
    print(f"Hold     : 1–3 days (NOT intraday)\n")
    print(f"Strategy : Small caps, during-market releases, speed wins.\n")

    signals_today = 0
    market_open_notified = False

    try:
        while True:
            now = datetime.now(EASTERN)

            if not args.all_hours and not is_market_hours(now):
                if now.weekday() < 5 and now.hour >= 16:
                    print(f"\nMarket closed ({now.strftime('%H:%M ET')}). Watcher exiting.")
                    tg_market_close(signals_today)
                    break
                mins_to_open = max(0, (9 * 60 + 30) - (now.hour * 60 + now.minute))
                label = "weekend" if now.weekday() >= 5 else f"{mins_to_open}m to open"
                print(f"  [{now.strftime('%H:%M')}] Outside market hours ({label}). Sleeping...", end="\r")
                time.sleep(POLL_SECS)
                continue

            # Notify market open once per day
            if not market_open_notified:
                tg_market_open(len(TICKERS))
                market_open_notified = True

            print(f"  [{now.strftime('%H:%M')}] Polling...", end=" ", flush=True)

            # Refresh halt list before each poll cycle
            refresh_halts(verbose=args.verbose)

            signals = (
                poll_press_releases(seen, verbose=args.verbose)
                + check_bnn_feed(seen, verbose=args.verbose)
            )
            save_seen(seen)

            if signals:
                print(f"{len(signals)} signal(s)!")
                for r in signals:
                    print_signal(r)
                    fire_notification(r)
                    tg_signal(r)
                    log_signal(r)
                    signals_today += 1
            else:
                print("no new signals.")

            time.sleep(POLL_SECS)

    except KeyboardInterrupt:
        print("\nWatcher stopped.")
        save_seen(seen)


if __name__ == "__main__":
    main()
