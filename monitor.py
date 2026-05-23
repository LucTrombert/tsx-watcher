"""
monitor.py — Real-time TSX/TSXV small-cap event monitor with FinBERT.

The edge: speed. These two functions watch your predetermined ticker list and
fire the moment something happens during market hours — before the crowd reacts.

Two monitors (run together by default):

  1. RELEASE MONITOR  — polls Yahoo Finance news every 60 s. The instant a new
                         press release appears (earnings, drill results, guidance,
                         NCIB, dividend increase, Nuttall pick), FinBERT scores
                         the headline and prints an actionable signal.

  2. INTRADAY MONITOR — checks 5-min OHLCV every 2 min. When a ticker moves
                         more than MOVE_THRESHOLD % from its opening price,
                         FinBERT analyses the latest headline for context and
                         prints a signal.

Usage:
    python3 monitor.py                        # both monitors
    python3 monitor.py --mode release         # release monitor only
    python3 monitor.py --mode intraday        # intraday monitor only
    python3 monitor.py --move-threshold 2.0   # tighter intraday trigger

Add / remove tickers in tickers.py — no code changes needed.
"""

from __future__ import annotations

import argparse
import threading
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from tickers import TICKERS

# ---------------------------------------------------------------------------
# FinBERT
# ---------------------------------------------------------------------------

try:
    from transformers import pipeline as hf_pipeline
    _TRANSFORMERS_OK = True
except ImportError:
    _TRANSFORMERS_OK = False

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

ET = ZoneInfo("America/New_York")

MARKET_OPEN_MIN  = 9 * 60 + 30   # 09:30 ET
MARKET_CLOSE_MIN = 16 * 60        # 16:00 ET

RELEASE_POLL_INTERVAL   = 60      # seconds between news scans
INTRADAY_POLL_INTERVAL  = 120     # seconds between price scans
MOVE_THRESHOLD_PCT      = 3.0     # intraday % move from open that triggers
MIN_VOLUME_FOR_SHORT    = 4_000_000

FRESH_ARTICLE_WINDOW_MIN = 5      # only alert on articles ≤ this many minutes old

# ---------------------------------------------------------------------------
# FinBERT helpers
# ---------------------------------------------------------------------------

def load_finbert():
    """Load FinBERT once at startup. First run downloads ~400 MB."""
    if not _TRANSFORMERS_OK:
        print(
            "WARN  transformers not installed.\n"
            "      Run:  pip3 install transformers torch\n"
            "      Continuing without FinBERT (signals will show N/A)."
        )
        return None
    print("Loading FinBERT (ProsusAI/finbert) — first run downloads ~400 MB...")
    pipe = hf_pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        device=-1,          # CPU; set to 0 if you have a GPU
        top_k=1,
    )
    print("FinBERT ready.\n")
    return pipe


def analyze_text(text: str, pipe) -> tuple[str, float]:
    """
    Run FinBERT on text (truncated to 512 tokens).
    Returns (label, confidence): label is 'positive', 'negative', or 'neutral'.
    """
    if not pipe or not text.strip():
        return "neutral", 0.0
    try:
        result = pipe(text[:512])[0]
        # pipeline with top_k=1 returns a list; unwrap if needed
        if isinstance(result, list):
            result = result[0]
        return result["label"].lower(), round(result["score"], 4)
    except Exception as exc:
        print(f"  WARN FinBERT error: {exc}")
        return "neutral", 0.0


# ---------------------------------------------------------------------------
# Market hours
# ---------------------------------------------------------------------------

def is_market_hours() -> bool:
    now = datetime.now(tz=ET)
    if now.weekday() >= 5:          # Sat / Sun
        return False
    mins = now.hour * 60 + now.minute
    return MARKET_OPEN_MIN <= mins < MARKET_CLOSE_MIN


def now_et_str() -> str:
    return datetime.now(tz=ET).strftime("%H:%M:%S ET")


# ---------------------------------------------------------------------------
# Signal logic
# ---------------------------------------------------------------------------

SENTIMENT_EMOJI = {"positive": "🟢", "negative": "🔴", "neutral": "⚪"}

# ---------------------------------------------------------------------------
# Event classification
# Order matters — checked top to bottom, first match wins.
# ---------------------------------------------------------------------------

EVENT_KEYWORDS: dict[str, list[str]] = {
    # ── Unconditionally bullish corporate actions ──────────────────────────
    "ncib": [
        "ncib", "normal course issuer bid", "share repurchase", "buyback",
        "repurchase program",
    ],
    "dividend_increase": [
        "dividend increase", "raises dividend", "special dividend",
        "increased dividend", "distribution increase", "raises its dividend",
        "increases quarterly dividend",
    ],

    # ── Speed / analyst picks ──────────────────────────────────────────────
    "nuttall_pick": [
        "eric nuttall", "nuttall",
    ],
    "bnn_top_pick": [
        "bnn market call", "top pick", "market call top pick",
        "analyst top pick", "bnn top pick",
    ],

    # ── Earnings & guidance ────────────────────────────────────────────────
    "earnings": [
        "earnings", "financial results", "quarterly results", "annual results",
        "year-end results", "q1 results", "q2 results", "q3 results", "q4 results",
        "reports revenue", "reports net", "eps", "per share",
        "beats consensus", "misses consensus", "beats estimates", "misses estimates",
    ],
    "guidance": [
        "guidance", "outlook", "forecast", "raises guidance", "lowers guidance",
        "updates guidance", "revises guidance", "production guidance",
        "capital guidance", "annual guidance",
    ],

    # ── Oil & gas operational events ───────────────────────────────────────
    "oil_well_results": [
        "well results", "initial production", "ip rate", "barrels per day",
        "boe/d", "bbl/d", "mmcf/d", "mcf/d", "horizontal well",
        "spud", "rig release", "completion results", "flow test",
        "production test", "oil discovery", "natural gas discovery",
    ],
    "production_update": [
        "production update", "operations update", "field update",
        "production guidance", "exit rate", "production volumes",
        "quarterly production", "corporate update",
    ],
    "reserves_update": [
        "reserves", "reserve update", "year-end reserves",
        "independent reserves", "contingent resources",
    ],

    # ── Mining operational events ──────────────────────────────────────────
    "drill_results": [
        "drill results", "drilling results", "assay results", "drill intercept",
        "hole results", "mineralization", "g/t gold", "g/t au", "grams per tonne",
        "copper equivalent", "silver equivalent", "maiden resource",
        "resource estimate", "mineral resource", "inferred resource",
        "indicated resource", "measured resource", "ni 43-101",
    ],
}

# Words that indicate a trading halt — skip any signal if detected
HALT_KEYWORDS = [
    "trading halt", "halted", "halt in trading", "cease trading",
    "regulatory halt", "stock halted",
]


def is_trading_halt(headline: str) -> bool:
    """Return True if the headline suggests a trading halt — skip signalling."""
    t = headline.lower()
    return any(kw in t for kw in HALT_KEYWORDS)


def classify_headline(headline: str) -> str:
    t = headline.lower()
    for event, keywords in EVENT_KEYWORDS.items():
        if any(kw in t for kw in keywords):
            return event
    return "other_catalyst"


def build_signal(
    label: str,
    score: float,
    event_type: str,
    volume: float = 0,
) -> str:
    """
    Map (FinBERT sentiment, event type) → actionable trading signal.

    Signal hierarchy (from the original strategy):
      1. Trading halt    → always SKIP (checked before this function)
      2. NCIB / dividend → always LONG (unconditionally bullish corporate actions)
      3. Nuttall / BNN   → always LONG (speed beats the viewers)
      4. Earnings        → FinBERT sentiment on headline vs consensus language
      5. Guidance        → FinBERT sentiment (raised = LONG, lowered = SHORT)
      6. Drill / well    → FinBERT sentiment (positive intercepts = LONG)
      7. Production upd  → FinBERT sentiment
      8. Everything else → FinBERT sentiment
      SHORT only when daily volume >= MIN_VOLUME_FOR_SHORT (liquidity required to exit)
    """
    # Unconditionally bullish
    if event_type == "ncib":
        return "LONG  — NCIB (unconditionally bullish: company buying own shares)"
    if event_type == "dividend_increase":
        return "LONG  — dividend increase (unconditionally bullish)"

    # Speed-driven analyst picks — sentiment irrelevant, beat the crowd
    if event_type == "nuttall_pick":
        return "LONG  — Nuttall pick (buy before BNN viewers react)"
    if event_type == "bnn_top_pick":
        return "LONG  — BNN Market Call top pick (buy before viewers react)"

    # ── Drill results & resource estimates ────────────────────────────────
    # Domain rule: companies only issue standalone press releases for GOOD
    # drill results. Bad holes are quietly omitted or buried in quarterly
    # reports. If FinBERT is neutral/inconclusive, default to LONG.
    # Only go SHORT if FinBERT is explicitly negative with high confidence.
    if event_type in ("drill_results", "oil_well_results", "reserves_update"):
        if label == "negative" and score >= 0.80:
            if volume >= MIN_VOLUME_FOR_SHORT:
                return f"SHORT — {event_type.replace('_', ' ')} negative ({score:.0%})"
            return (
                f"SKIP  — negative {event_type.replace('_', ' ')} ({score:.0%}) "
                f"vol {volume:,.0f} < {MIN_VOLUME_FOR_SHORT:,} needed to short"
            )
        # Positive or neutral → assume positive (companies only PR good results)
        return (
            f"LONG  — {event_type.replace('_', ' ')} "
            f"({'FinBERT: ' + label + ' ' + str(round(score*100)) + '%' if label != 'neutral' else 'neutral headline — companies only PR good results'})"
        )

    # ── Sentiment-driven for earnings, guidance, production updates ───────
    if label == "positive" and score >= 0.70:
        return f"LONG  — {event_type.replace('_', ' ')} positive ({score:.0%})"

    if label == "negative" and score >= 0.70:
        if volume >= MIN_VOLUME_FOR_SHORT:
            return f"SHORT — {event_type.replace('_', ' ')} negative ({score:.0%})"
        return (
            f"SKIP  — negative {event_type.replace('_', ' ')} ({score:.0%}) but "
            f"vol {volume:,.0f} < {MIN_VOLUME_FOR_SHORT:,} needed to short"
        )

    return f"FLAT  — sentiment inconclusive ({label} {score:.0%})"


# ---------------------------------------------------------------------------
# 1. RELEASE MONITOR
# ---------------------------------------------------------------------------

def _print_release_signal(
    ticker: str, event_type: str, headline: str,
    label: str, score: float, signal: str,
) -> None:
    bar  = "─" * 64
    emoji = SENTIMENT_EMOJI.get(label, "⚪")
    print(f"\n{bar}")
    print(f"🚨  RELEASE   {ticker}   {now_et_str()}")
    print(f"    Event    : {event_type}")
    print(f"    Headline : {headline[:110]}")
    print(f"    FinBERT  : {emoji} {label.upper()}  ({score:.1%})")
    print(f"    Signal   : {signal}")
    print(bar)


def poll_releases(
    tickers: list[str],
    pipe,
    stop_event: threading.Event,
) -> None:
    """
    Continuously polls Yahoo Finance news for every ticker in the list.
    Fires FinBERT the instant a new article appears during market hours.

    Articles older than FRESH_ARTICLE_WINDOW_MIN minutes are silently marked
    as seen on first scan so stale news never triggers alerts.
    """
    seen: set[str] = set()
    print(f"[Release monitor]  Watching {len(tickers)} tickers  "
          f"(poll every {RELEASE_POLL_INTERVAL}s)")

    while not stop_event.is_set():
        if not is_market_hours():
            time.sleep(30)
            continue

        for ticker in tickers:
            try:
                news_items = yf.Ticker(ticker).news or []
            except Exception:
                news_items = []

            for article in news_items:
                # yfinance 1.x nests everything under "content"
                content = article.get("content") or article
                uid = content.get("id") or article.get("id") or ""
                if not uid or uid in seen:
                    continue

                # Age check: only fire on very fresh releases
                pub_str = content.get("pubDate") or content.get("displayTime") or ""
                if pub_str:
                    try:
                        from datetime import timezone
                        pub_dt  = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        age_min = (datetime.now(tz=timezone.utc) - pub_dt).total_seconds() / 60
                    except Exception:
                        age_min = 0
                else:
                    age_min = 0

                seen.add(uid)   # mark as seen regardless, so we don't re-alert

                if age_min > FRESH_ARTICLE_WINDOW_MIN:
                    continue    # stale — skip silently

                headline = str(content.get("title") or "")

                # Safety: never signal on a trading halt
                if is_trading_halt(headline):
                    print(f"\n⚠️  HALT DETECTED  {ticker}  —  {headline[:80]}")
                    print(f"    Skipping signal — stock may be halted.")
                    continue

                event_type = classify_headline(headline)
                label, score = analyze_text(headline, pipe)

                # Get volume for short sizing filter
                try:
                    volume = float(
                        yf.Ticker(ticker).fast_info.three_month_average_volume or 0
                    )
                except Exception:
                    volume = 0

                signal = build_signal(label, score, event_type, volume)
                _print_release_signal(ticker, event_type, headline, label, score, signal)

        time.sleep(RELEASE_POLL_INTERVAL)

    print("[Release monitor]  Stopped.")


# ---------------------------------------------------------------------------
# 2. INTRADAY MONITOR
# ---------------------------------------------------------------------------

def _print_intraday_signal(
    ticker: str,
    open_px: float,
    current_px: float,
    pct: float,
    headline: str,
    label: str,
    score: float,
    signal: str,
) -> None:
    bar       = "─" * 64
    direction = "▲" if pct > 0 else "▼"
    emoji     = SENTIMENT_EMOJI.get(label, "⚪")
    print(f"\n{bar}")
    print(f"📊  INTRADAY   {ticker}   {now_et_str()}")
    print(f"    Move     : ${open_px:.2f} → ${current_px:.2f}  "
          f"{direction} {abs(pct):.1f}% from open")
    if headline:
        print(f"    News     : {headline[:100]}")
        print(f"    FinBERT  : {emoji} {label.upper()}  ({score:.1%})")
    else:
        print(f"    News     : (no recent headline found)")
    print(f"    Signal   : {signal}")
    print(bar)


def poll_intraday(
    tickers: list[str],
    pipe,
    stop_event: threading.Event,
    move_threshold: float = MOVE_THRESHOLD_PCT,
) -> None:
    """
    Checks 5-min OHLCV for every ticker every INTRADAY_POLL_INTERVAL seconds.
    When a ticker moves more than move_threshold % from its opening price,
    pulls the latest headline, runs FinBERT for context, and prints a signal.

    Each ticker only alerts once per trading day to avoid spam.
    """
    alerted_today: dict[str, date] = {}
    print(f"[Intraday monitor] Watching {len(tickers)} tickers  "
          f"(threshold ±{move_threshold:.1f}%,  poll every {INTRADAY_POLL_INTERVAL}s)")

    while not stop_event.is_set():
        if not is_market_hours():
            time.sleep(30)
            continue

        today = date.today()

        for ticker in tickers:
            # Only alert once per ticker per day
            if alerted_today.get(ticker) == today:
                continue

            try:
                import contextlib, io
                with contextlib.redirect_stderr(io.StringIO()):
                    df = yf.download(
                        ticker,
                        period="1d",
                        interval="5m",
                        progress=False,
                        auto_adjust=True,
                    )
            except Exception:
                continue

            if df is None or df.empty or len(df) < 2:
                continue

            # yfinance 1.x always returns MultiIndex (Price, Ticker) — flatten it
            if isinstance(df.columns, pd.MultiIndex):
                df = df.xs(ticker, axis=1, level="Ticker") if ticker in df.columns.get_level_values("Ticker") else df
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)

            open_px    = float(df["Open"].iloc[0])
            current_px = float(df["Close"].iloc[-1])

            if open_px <= 0:
                continue

            pct = (current_px / open_px - 1) * 100

            if abs(pct) < move_threshold:
                continue

            # Significant move — fetch latest headline for FinBERT context
            headline     = ""
            label, score = "neutral", 0.0
            try:
                news = yf.Ticker(ticker).news or []
                if news:
                    content  = news[0].get("content") or news[0]
                    headline = str(content.get("title") or "")
                    label, score = analyze_text(headline, pipe)
            except Exception:
                pass

            try:
                volume = float(df["Volume"].sum())
            except Exception:
                volume = 0

            # For intraday, use the price direction as primary signal
            # if FinBERT is inconclusive (neutral / low confidence)
            if label == "neutral" or score < 0.70:
                if pct > 0:
                    label_eff, score_eff = "positive", score
                else:
                    label_eff, score_eff = "negative", score
            else:
                label_eff, score_eff = label, score

            event_type = classify_headline(headline)
            signal     = build_signal(label_eff, score_eff, event_type, volume)

            _print_intraday_signal(
                ticker, open_px, current_px, pct,
                headline, label, score, signal,
            )
            alerted_today[ticker] = today

        time.sleep(INTRADAY_POLL_INTERVAL)

    print("[Intraday monitor] Stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Real-time TSX/TSXV small-cap event monitor with FinBERT.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python3 monitor.py\n"
            "  python3 monitor.py --mode release\n"
            "  python3 monitor.py --mode intraday --move-threshold 2.0\n"
        ),
    )
    p.add_argument(
        "--mode",
        choices=["release", "intraday", "both"],
        default="both",
        help="Which monitor to run (default: both).",
    )
    p.add_argument(
        "--move-threshold",
        type=float,
        default=MOVE_THRESHOLD_PCT,
        help=f"Intraday %% move from open required to trigger (default: {MOVE_THRESHOLD_PCT}).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print("\n" + "=" * 64)
    print("  TSX/TSXV Small-Cap Event Monitor  —  FinBERT")
    print("=" * 64)
    print(f"  Tickers        : {len(TICKERS)}")
    print(f"  Mode           : {args.mode}")
    print(f"  Intraday trigger: ±{args.move_threshold:.1f}% from open")
    print(f"  Short min vol  : {MIN_VOLUME_FOR_SHORT:,}")
    print(f"  Market hours   : 09:30 – 16:00 ET (monitors sleep outside hours)")
    print("=" * 64 + "\n")
    print("Press Ctrl-C to stop.\n")

    pipe = load_finbert()
    stop = threading.Event()
    threads: list[threading.Thread] = []

    if args.mode in ("release", "both"):
        threads.append(threading.Thread(
            target=poll_releases,
            args=(TICKERS, pipe, stop),
            daemon=True,
            name="release-monitor",
        ))

    if args.mode in ("intraday", "both"):
        threads.append(threading.Thread(
            target=poll_intraday,
            args=(TICKERS, pipe, stop, args.move_threshold),
            daemon=True,
            name="intraday-monitor",
        ))

    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping monitors...")
        stop.set()
        for t in threads:
            t.join(timeout=5)
        print("Done.")


if __name__ == "__main__":
    main()
