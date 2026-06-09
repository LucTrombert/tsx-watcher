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
  • Infers trading halts from a yfinance volume proxy — skips a matched stock
    that looks halted (CIRO's halt page is Cloudflare-blocked)
  • ~80 RSS requests/day; yfinance + Claude calls only for matched candidates
  • Seen URLs persisted to disk — no duplicate alerts across restarts

Usage:
    python3 watcher.py                # run until 4:00pm ET
    python3 watcher.py --test         # fire a test notification and exit
    python3 watcher.py --url <URL>    # manually score any press release URL
    python3 watcher.py --all-hours    # skip market-hours guard (for testing)
    python3 watcher.py --verbose      # show every RSS item checked
    python3 watcher.py --discover     # run weekly ticker discovery scan and exit
    python3 watcher.py --until 12:30  # clean self-exit at HH:MM ET (CI handoff)

Install (once):
    pip3 install feedparser yfinance requests beautifulsoup4
"""
from __future__ import annotations

import argparse, json, os, re, subprocess, time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import feedparser
import requests
import yfinance as yf
from bs4 import BeautifulSoup

# ── Config ─────────────────────────────────────────────────────────────────────
EASTERN        = ZoneInfo("America/Toronto")
PREMARKET_OPEN = (7,  0)   # start scanning for pre-market PRs
MARKET_OPEN    = (9, 30)
MARKET_CLOSE   = (16, 0)
POLL_SECS      = 300       # 5 minutes — ~78 requests/day per feed

SEEN_FILE    = Path("data/signals/seen_urls.json")
LOG_FILE     = Path("data/signals/signals.log")
PR_LOG_DIR      = Path("data/signals/press_releases")  # stores raw PR text for future backtest
POSITIONS_FILE  = Path("data/signals/open_positions.json")  # tracks open positions for exit AI
TG_CONFIG    = Path("data/signals/telegram_config.json")
AUTO_TICKERS_FILE = Path("data/signals/auto_added_tickers.json")  # autonomous Saturday auto-adds — merged into the universe at load (kept as DATA so the weekly job never edits its own source)
OUTCOMES_FILE = Path("data/signals/outcomes.json")  # forward-return dataset: joins each signal to D+1/D+3 outcomes at multiple entry points (the 'y' column for validating the PR edge)
PAPER_FILE = Path("data/signals/paper_portfolio.json")  # autonomous paper-trading account (simulated, no real money)
UNIVERSE_FILE = Path("data/signals/universe.json")      # live universe snapshot w/ per-ticker lifecycle status (for the dashboard)

# ── RSS feeds ──────────────────────────────────────────────────────────────────
PRESS_RELEASE_FEEDS = [
    "https://www.globenewswire.com/RssFeed/country/Canada",
    # Accesswire RSS removed — feed went dead May 2026
]

# TMX Newsfile category pages — dominant wire for TSXV small caps.
# Scraped every 5 min (same cadence as RSS). No public RSS available.
# Rate: 3 pages × 12 polls/hr × 6.5 hrs = ~234 req/day. Well within limits.
NEWSFILE_CATEGORIES = [
    "https://www.newsfilecorp.com/news/mining-metals",
    "https://www.newsfilecorp.com/news/precious-metals",
    "https://www.newsfilecorp.com/news/oil-gas",
]
NEWSFILE_BASE = "https://www.newsfilecorp.com"

# BNN Market Call podcast — analyst top picks (Eric Nuttall energy, etc.)
BNN_MARKET_CALL_FEED = "https://omny.fm/shows/market-call/playlists/podcast.rss"

# ── Small-cap ticker universe (<$300M market cap, limited analyst coverage) ───
# Strategy only applies to small caps — large/mid caps have too much coverage.
# Add tickers here as you discover new names. All should be TSX or TSXV.
TICKERS = [
    # ── TSX Small Cap Energy (<$300M, limited analyst coverage) ──────────────
    # PRQ.TO removed — symbol is Petrus Resources, not Perpetual Energy (which merged into Rubellite/RBY.TO, Oct 2024)
    # KEI.TO removed — symbol is Kolibri Global Energy, not Kelt; real Kelt (KEL.TO) is now ~$1.9B, too big
    "BNE.TO",    # Bonterra Energy        ~$228M
    "PNE.TO",    # Pine Cliff Energy      ~$235M
    "JOY.TO",    # Journey Energy         ~$335M  (borderline — active quarterly earnings)

    # ── TSXV Energy ──────────────────────────────────────────────────────────
    "HME.V",     # Hemisphere Energy      ~$257M  quarterly earnings, active driller
    "ALV.V",     # Alvopetro Energy       ~$328M  Brazil-focused, quarterly results
    # PCQ.V removed — coal, thin liquidity, poor fit
    # TAO.V removed — renewables micro-cap, wrong catalyst type
    # PUL.V removed — $9M, never clears $50k volume filter

    # ── TSX Small Cap Mining ─────────────────────────────────────────────────
    # GMX.TO removed — symbol is Globex Mining, not Gold Mountain Mining; intended GMTN.TO appears delisted/acquired
    "ORV.TO",    # Orvana Minerals        ~$245M  gold/copper/silver — all three metals

    # ── TSXV Mining — Gold/Silver ─────────────────────────────────────────────
    "SKP.V",     # Strikepoint Gold       ~$10M   Yukon high-grade (corrected from wrong symbol SAG.V = Sterling Metals)
    "AGX.V",     # Silver X Mining        ~$210M  Peru silver producer (Nueva Recuperada)
    "AZM.V",     # Azimut Exploration     ~$71M   active explorer, frequent drill news
    "BHS.V",     # Bayhorse Silver        ~$29M
    # AHR.V removed — symbol is Amarc Resources, not American Helium (which became Auscan, now dormant on NEX)
    # GSP.V removed — symbol is Gensource Potash, not Gossan; real Gossan (GSS.V) is a $2M illiquid penny
    # GLD.V removed — worst in universe (D+1 -1.19%, 29% win); symbol is Gold Finder, intended Goldstrike→Trailbreaker (TBK.V)

    # ── TSXV Mining — Copper ─────────────────────────────────────────────────
    "KDK.V",     # Kodiak Copper          ~$85M   active drill program
    "SURG.V",    # Surge Copper           ~$215M  active drill program
    "CUU.V",     # Copper Fox Metals      ~$352M  (borderline — active Schaft Creek project)

    # ── TSXV / TSX Other Small Cap ───────────────────────────────────────────
    "USCU.V",    # US Copper              ~$25M
    # ABR.V removed — symbol is Arbor Metals, not Aberdeen Int'l; real Aberdeen (AAB.TO) is a $0.03 penny, too illiquid
    # RVX.TO removed — pharma, no drill/earnings signal logic for clinical trials
    # MCS.V  removed — $6M, dead weight, never clears volume filter
    # AFM.V  removed — $1.68B, institutional coverage, no pricing inefficiency

    # ── TSXV Mining — Gold (high-grade active drillers, zero coverage) ────────
    "ROCK.V",    # Trident Resources      ~$160M  Contact Lake SK — 15–17 g/t Au active holes
    "SMN.V",     # Sun Summit Minerals    ~$51M   Toodoggone BC — JD Project, 21/21 holes hit 2025

    # ── TSXV Mining — Copper-Gold Porphyry ───────────────────────────────────
    "SCMI.V",    # Selkirk Copper Mines   ~$244M  Minto Cu-Au Yukon — MRE + PEA mid-2026 binary
    "AE.V",      # American Eagle Gold    ~$219M  NAK BC porphyry — S32/Teck backed, 2026 holes imminent
    "KFR.V",     # Kingfisher Metals      ~$144M  Hank BC Golden Triangle — 3 rigs mid-June start

    # ── TSXV Mining — Silver / Base Metals ────────────────────────────────────
    "BBB.V",     # Brixton Metals         ~$54M   Langis ON silver + Thorn BC porphyry — serial assays
    "CGNT.V",    # Copper Giant Resources ~$150M  Mocoa Colombia Cu-Mo — PEA H2 2026  (2 analysts)
    "MGG.V",     # Minaurum Silver        ~$199M  Alamos Sonora MX — 55 Moz AgEq resource, 6 rigs

    # ── June 3 2026 — Cowork discovery batch (21 added, all yfinance longName-validated) ──
    # Energy
    "YGR.TO",    # Yangarra Resources     ~$147M  Belly River light oil — Q1 9,638 boe/d
    "LTC.V",     # Lotus Creek Exploration ~$156M Belly River oil (spun from Gear) — Q1 FFO $10.6M, 75% growth
    "PRQ.TO",    # Petrus Resources       ~$262M  Cardium oil — RE-ADDED with correct label (was mislabeled Perpetual)
    # Copper
    "MMA.V",     # Midnight Sun Mining    ~$198M  Dumbwa Cu, Zambia — >5.3km strike, drilling
    "MOG.V",     # Mogotes Metals         ~$283M  Filo Sur Cu-Au, Argentina (Vicuña) — 86m @ 0.7% Cu ⚡
    "NIM.V",     # Nicola Mining          ~$184M  New Craigmont BC + toll-mill cash flow
    # Gold
    "BOGO.V",    # Borealis Mining        ~$156M  first production + Sandman PEA, Nevada
    "MGM.V",     # Maple Gold Mines       ~$215M  Abitibi — 30,000m Phase II, maiden Joutel MRE H1 ⚡
    "CBR.V",     # Cabral Gold            ~$283M  Brazil — 6 rigs, first gold pour Q4'26
    "ONYX.V",    # Onyx Gold              ~$109M  Timmins — 110,000m, 4 rigs
    "PRG.V",     # Precipitate Gold       ~$58M   Dominican Rep. — drilling adj. Barrick Pueblo Viejo
    "GWM.V",     # Galway Metals          ~$73M   Clarence Stream NB — monthly high-grade hits, ~2.2M oz
    "KTO.V",     # K2 Gold                ~$177M  Mojave CA — permitted oxide gold (thin vol)
    "DRY.V",     # Dryden Gold            ~$80M   Hyndman ON 23.3 g/t (thin vol)
    "ECR.V",     # Cartier Resources      ~$125M  Cadillac/Val-d'Or — 7.1 g/t/8m (thin vol)
    # Silver
    "GRSL.V",    # GR Silver Mining       ~$191M  San Marcial MX — best-ever hits
    "KTN.V",     # Kootenay Silver        ~$164M  Columba MX — high-grade
    "IPT.V",     # IMPACT Silver          ~$124M  Zacualpan producer — Q1 rev tripled $31.2M
    "OCG.TO",    # Outcrop Silver & Gold  ~$166M  Santa Ana Colombia (jurisdiction flag)
    "SVE.V",     # Silver One Resources   ~$169M  Candelaria NV restart (thin vol)
    "BPAG.V",    # BP Silver              ~$59M   Cosuño Bolivia 600 g/t Ag/5m (jurisdiction flag)
]

# ── Company name → ticker (used to match press release headlines) ──────────────
COMPANY_NAMES = {
    # TSX Energy
    "BNE.TO":  "Bonterra Energy",
    "PNE.TO":  "Pine Cliff Energy",
    "JOY.TO":  "Journey Energy",
    # TSXV Energy
    "HME.V":   "Hemisphere Energy",
    "ALV.V":   "Alvopetro Energy",
    # TSX Mining
    "ORV.TO":  "Orvana Minerals",
    # TSXV Mining — Gold/Silver
    "SKP.V":   "Strikepoint Gold",
    "AGX.V":   "Silver X Mining",
    "AZM.V":   "Azimut Exploration",
    "BHS.V":   "Bayhorse Silver",
    # TSXV Mining — Copper
    "KDK.V":   "Kodiak Copper",
    "SURG.V":  "Surge Copper",
    "CUU.V":   "Copper Fox Metals",
    # TSXV Other
    "USCU.V":  "US Copper",
    # TSXV Gold
    "ROCK.V":  "Trident Resources",
    "SMN.V":   "Sun Summit Minerals",
    # TSXV Copper-Gold Porphyry
    "SCMI.V":  "Selkirk Copper Mines",
    "AE.V":    "American Eagle Gold",
    "KFR.V":   "Kingfisher Metals",
    # TSXV Silver / Base Metals
    "BBB.V":   "Brixton Metals",
    "CGNT.V":  "Copper Giant Resources",
    "MGG.V":   "Minaurum Silver",
    # June 3 2026 — Cowork discovery batch (yfinance longName-validated)
    "YGR.TO":  "Yangarra Resources",
    "LTC.V":   "Lotus Creek",
    "PRQ.TO":  "Petrus Resources",
    "MMA.V":   "Midnight Sun Mining",
    "MOG.V":   "Mogotes Metals",
    "NIM.V":   "Nicola Mining",
    "BOGO.V":  "Borealis Mining",
    "MGM.V":   "Maple Gold",
    "CBR.V":   "Cabral Gold",
    "ONYX.V":  "Onyx Gold",
    "PRG.V":   "Precipitate Gold Corp",   # qualified: bare "precipitate gold" is a metallurgical phrase (false-positive)
    "GWM.V":   "Galway Metals",
    "KTO.V":   "K2 Gold",
    "DRY.V":   "Dryden Gold",
    "ECR.V":   "Cartier Resources",
    "GRSL.V":  "GR Silver Mining",
    "KTN.V":   "Kootenay Silver",
    "IPT.V":   "IMPACT Silver Corp",   # qualified: bare "impact silver" matches "impact silver prices" (false-positive)
    "OCG.TO":  "Outcrop Silver",
    "SVE.V":   "Silver One Resources",
    "BPAG.V":  "BP Silver",
}

SECTOR_MAP = {
    "BNE.TO": "Energy",
    "PNE.TO": "Energy", "JOY.TO": "Energy", "HME.V":  "Energy",
    "ALV.V":  "Energy",
    "YGR.TO": "Energy", "LTC.V":  "Energy", "PRQ.TO": "Energy",
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
    # Genuine financial / production guidance only.
    # Bare year tokens ("2025"/"2026"), "outlook", "full year", "going forward",
    # "next quarter" were REMOVED — they match in any explorer drill PR
    # ("2026 drill program", "exploration outlook"), falsely flagging guidance
    # and over-promoting explorers to STRONG BUY. These phrases are guidance-
    # specific and still fire for real producers (HME.V, JOY.TO, etc.).
    "guidance", "production guidance", "production target",
    "annual guidance", "full-year guidance", "full year guidance",
    "fiscal guidance", "capex budget", "capital budget",
    "production outlook", "financial outlook", "annual production target",
    "raises guidance", "increases guidance", "updates guidance",
    "revised guidance", "reaffirms guidance", "reiterates guidance",
    "issues guidance",
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

# ── Commodity context for Claude analysis ──────────────────────────────────────
# Maps commodity names to yfinance futures symbols
_COMMODITY_SYMBOLS = {
    "gold":   "GC=F",
    "copper": "HG=F",
    "silver": "SI=F",
    "oil":    "CL=F",
}

# Per-ticker commodity override (default: energy→oil, mining→gold)
_TICKER_COMMODITY: dict[str, str] = {
    "KDK.V":  "copper", "SURG.V": "copper", "CUU.V":  "copper", "USCU.V": "copper",
    "BHS.V":  "silver",
    # New tickers May 2026
    "SCMI.V": "copper", "AE.V":   "copper", "KFR.V":  "copper", "CGNT.V": "copper",
    "BBB.V":  "silver", "MGG.V":  "silver", "AGX.V":  "silver",
    # June 3 2026 batch — copper + silver (gold/oil fall through to defaults)
    "MMA.V":  "copper", "MOG.V":  "copper", "NIM.V":  "copper",
    "GRSL.V": "silver", "KTN.V":  "silver", "IPT.V":  "silver",
    "OCG.TO": "silver", "SVE.V":  "silver", "BPAG.V": "silver",
}


def get_commodity(ticker: str) -> str:
    """Sub-sector bucket used for cluster detection: gold / copper / silver / oil.
    Mirrors the default used for commodity context (energy→oil, mining→gold)."""
    return _TICKER_COMMODITY.get(ticker, "oil" if get_sector(ticker) == "Energy" else "gold")


_NAME_SUFFIXES = {"corp", "corporation", "inc", "incorporated", "ltd",
                  "limited", "co", "company", "plc"}

def _match_name(longname: str) -> str:
    """
    Strip trailing corporate suffixes so headline matching works. Press releases
    usually say 'Midnight Sun Mining' or 'Midnight Sun Mining drills…', not the full
    legal 'Midnight Sun Mining Corp.' — and match_ticker() does substring matching,
    so a stored name WITH the suffix silently misses those headlines. Idempotent.
    'Midnight Sun Mining Corp.' -> 'Midnight Sun Mining'.
    """
    longname = str(longname or "")        # coerce — a non-str ledger value must not throw
    toks = longname.strip().split()
    while toks and re.sub(r"[^a-z]", "", toks[-1].lower()) in _NAME_SUFFIXES:
        toks.pop()
    return " ".join(toks) if toks else longname.strip()


def _merge_auto_added() -> None:
    """
    Merge autonomously auto-added tickers (the weekly Saturday scan) into the live
    universe at import time. Kept as DATA (auto_added_tickers.json), never written
    into this source file — so the automated job can grow the watchlist without any
    risk of corrupting watcher.py. Each entry already passed the deterministic
    screener (cap/liquidity/price/sector/longName) before being written here.
    """
    if not AUTO_TICKERS_FILE.exists():
        return
    try:
        data = json.loads(AUTO_TICKERS_FILE.read_text())
    except Exception:
        return
    if not isinstance(data, dict):
        return
    for sym, meta in data.items():
        # One malformed ledger entry must never crash module import (which would take
        # down EVERY entry point + the dashboard data jobs). Skip bad entries.
        try:
            if not sym or sym in COMPANY_NAMES or not isinstance(meta, dict):
                continue
            TICKERS.append(sym)
            COMPANY_NAMES[sym] = _match_name(meta.get("name", sym))   # suffix-stripped for matching
            if meta.get("sector") == "Energy":
                SECTOR_MAP[sym] = "Energy"
            if meta.get("commodity"):
                _TICKER_COMMODITY[sym] = meta["commodity"]
        except Exception:
            continue


def _load_auto_added_file() -> dict:
    """Raw read of the auto-add ledger (used by the discovery scan to append)."""
    if AUTO_TICKERS_FILE.exists():
        try:
            return json.loads(AUTO_TICKERS_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_auto_added_file(data: dict) -> None:
    AUTO_TICKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    AUTO_TICKERS_FILE.write_text(json.dumps(data, indent=2, sort_keys=True))


_merge_auto_added()   # extend the universe with prior auto-adds before anything runs


_commodity_cache:    dict           = {}
_commodity_cache_ts: datetime|None  = None


# ══════════════════════════════════════════════════════════════════════════════
# Claude AI press release analysis
# Optional — requires ANTHROPIC_API_KEY env var.  Falls back to keyword scoring.
# Uses claude-haiku-4-5 (cost-efficient for automated per-PR calls).
# System prompt is prompt-cached — only charged once per cache TTL (5 min).
# ══════════════════════════════════════════════════════════════════════════════

_CLAUDE_SYSTEM_PROMPT = """\
You are the signal engine for a TSX/TSXV small-cap event-driven trading strategy.
Your job: read one press release and decide whether it justifies holding a momentum trade for 1-3 days.

══════════════════════════════════════════════════════
THE MISSION
══════════════════════════════════════════════════════
Small-cap TSX/TSXV stocks (<$300M market cap) have almost no analyst coverage.
When real news drops, retail investors take 2-3 days to fully price it in.
The system enters AFTER the stock has already moved ≥15% intraday (confirming the news is real),
then rides the continuation. Backtest: 1,929 events, mining ≥15% → D+1 avg +8.47%, win rate 63%.
Your analysis determines whether to enter, hold, or skip. Every wrong SKIP is a missed trade.
Every wrong BUY risks real capital. When in doubt → CAUTION, not BUY.

The stock has ALREADY moved. You are not predicting the first move — you are deciding if the
underlying catalyst is strong enough to sustain momentum for 2-3 more days.

══════════════════════════════════════════════════════
SIGNAL DEFINITIONS
══════════════════════════════════════════════════════
STRONG BUY : Catalyst is unambiguously exceptional. Market will keep buying for days.
             Triggers: maiden NI 43-101 resource, PEA/PFS/FS release, drill hole that
             redefines the deposit, major acquisition at premium, massive earnings beat
             with guidance raise, production record + outlook upgrade.
BUY        : Clear positive catalyst — solid drill intercept, earnings beat, deal announcement,
             resource expansion, record quarter, guidance raise, dividend increase, NCIB.
CAUTION    : Mixed signals. Good news offset by dilution, unclear grade/width, or boilerplate
             language with no real numbers. The move may not sustain. Stay on sideline.
SKIP       : Negative catalyst. Net loss, revenue decline, guidance cut, production miss,
             operational problem, failed drill, resource downgrade, dilutive PP at discount,
             force majeure, trading halt context.

══════════════════════════════════════════════════════
DRILL RESULT THRESHOLDS — READ THESE CAREFULLY
══════════════════════════════════════════════════════
Gold (open-pit context, g/t Au):
  SKIP     < 0.5 g/t   (sub-economic, not worth mining)
  CAUTION  0.5–1.0 g/t (marginal, needs high volume to work)
  BUY      1.0–5.0 g/t (solid open-pit grade)
  STRONG BUY > 5 g/t   (high-grade, market will chase this)

Gold (underground context or depth > 300m):
  BUY      3–10 g/t
  STRONG BUY > 10 g/t

Copper (CuEq% or Cu%):
  SKIP     < 0.2%   (sub-economic porphyry)
  CAUTION  0.2–0.4% (marginal)
  BUY      0.4–0.8% (solid porphyry grade)
  STRONG BUY > 0.8% (high-grade porphyry — rare)

Silver (g/t Ag):
  SKIP     < 50 g/t
  CAUTION  50–100 g/t
  BUY      100–300 g/t
  STRONG BUY > 300 g/t

WIDTH MATTERS as much as grade. Grade × Width = grade-thickness (GT value):
  - 50m @ 1% CuEq = 50 CuEq-metres → BUY (bulk tonnage potential)
  - 2m @ 5% CuEq = 10 CuEq-metres → CAUTION (too narrow for open pit)
  - 100m @ 0.5% CuEq = 50 CuEq-metres → BUY (large low-grade porphyry)
  - 5m @ 0.3% CuEq = 1.5 CuEq-metres → SKIP
Multiple holes > single hole. Step-out holes (expand footprint) > infill holes.

══════════════════════════════════════════════════════
NI 43-101 RESOURCE LANGUAGE
══════════════════════════════════════════════════════
STRONG BUY catalysts:
  - Maiden Inferred/Indicated Resource (first-ever resource estimate for the project)
  - Pre-feasibility Study (PFS) or Feasibility Study (FS) — project moving to production
  - Preliminary Economic Assessment (PEA) with strong NPV/IRR (>15% IRR at spot)
BUY catalysts:
  - Resource expansion >20% in contained metal
  - Updated resource with higher grade or larger tonnage
SKIP catalysts:
  - Resource downgrade or restatement to lower tonnage/grade
  - Mine closure or suspension
  - Failed permitting

══════════════════════════════════════════════════════
PRIVATE PLACEMENT & FINANCING RULES
══════════════════════════════════════════════════════
These are critical. A good press release attached to a financing is ALWAYS less bullish:
  SKIP   : Non-brokered PP at >10% discount to market (company desperate for cash)
  SKIP   : Rights offering (forces all shareholders to dilute or lose ownership)
  CAUTION: PP at market price or small discount (<5%) — dilution offsets good news
  CAUTION: Bought deal alongside results (bank-backed but still dilutive)
  NEUTRAL: Bought deal on its own (no other news) — signals confidence but priced in fast
  IGNORE if financing is small (<5% of market cap) relative to the actual catalyst

Key tells for bad financing language:
  "concurrent private placement", "at a price of $X representing a X% discount",
  "flow-through shares", "hard dollar units", "full warrant attached"

══════════════════════════════════════════════════════
COMPANY CONTEXT — KNOW YOUR UNIVERSE
══════════════════════════════════════════════════════
COPPER EXPLORERS (any intercept news → check grade thresholds above carefully):
  KDK.V  — Kodiak Copper. Gate project, BC. Targeting MPD/Alpha porphyry zones.
            Backed by major drill program. Looking for bulk-tonnage copper-gold system.
            Good intercept = step-out hole confirming zone expansion or new zone.
  SURG.V — Surge Copper. Berg project, BC (Toodoggone district). JV with Centerra Gold.
            Large low-grade porphyry. Wide intercepts at 0.3–0.6% CuEq = BUY here
            (scale matters more than grade for this deposit type).
  CUU.V  — Copper Fox Metals. Schaft Creek project, BC. Giant low-grade Cu-Mo-Au-Ag system.
            News is usually permitting, JV updates, or PFS progress — NOT drill results.
            Signal = major deal / permitting milestone / JV with major miner.
  USCU.V — US Copper. Idaho. Early stage. High bar for BUY — needs exceptional drill grade.

GOLD/SILVER EXPLORERS:
  AZM.V  — Azimut Exploration. James Bay, Quebec. Active generative explorer.
            Releases drill results frequently. Use gold thresholds strictly.
  SKP.V  — Strikepoint Gold. Yukon. High-grade targets but small scale.
  AGX.V  — Silver X Mining. Silver PRODUCER in Peru (Nueva Recuperada). Use silver thresholds.
            Operational news (production oz, grades, expansion to ~6M oz/yr target, drilling
            at Plata/Tangana zones) = BUY if above guidance. NOT a gold explorer.
  BHS.V  — Bayhorse Silver. Producing silver mine, Oregon. Use silver thresholds.
            Operational news (production #s, shipments) = BUY if above guidance.
  ORV.TO — Orvana Minerals. Spain (Villalba) + Bolivia gold-copper-silver.
            Reports in USD. Quarterly production + earnings = use energy-style thresholds.
HIGH-GRADE GOLD DRILLERS (active programs — assay results are THE catalyst):
  ROCK.V — Trident Resources. Contact Lake, Saskatchewan.
            Hottest junior gold in 2026 — returned 15.11 g/t Au/51.83m (Apr 29) and
            17.88 g/t Au/11.25m (May 27). BK3 Zone + Contact Lake Zone drilling.
            Use gold underground thresholds (depth >300m context): BUY ≥3 g/t.
            Grade × width is the key metric — wide zones (>20m) matter more than narrow spikes.
  SMN.V  — Sun Summit Minerals. JD Project, Toodoggone district, BC.
            21/21 holes hit gold in 2025. 10,000m+ program starting June 2026.
            Creek Zone + Finn Zone. NI 43-101 filed April 2026. Use gold thresholds.
            At $51M market cap, each strong hole will move this 20%+.

COPPER-GOLD PORPHYRY (bulk tonnage — grade × width is everything, not single-hole peaks):
  SCMI.V — Selkirk Copper Mines. Minto Cu-Au-Ag mine, Yukon.
            Redeveloping historic mine. 50,000m Phase 2 (4 rigs) started May 1 2026.
            New 117 Lens discovery below old pit. MRE + PEA targeted mid-2026.
            Binary catalyst: MRE/PEA release = STRONG BUY if economics are solid.
            Drill results: use copper thresholds, wide intervals (>50m) at ≥0.4% CuEq = BUY.
  AE.V   — American Eagle Gold. NAK porphyry, Babine district, BC.
            Backed by South32, Teck, Eric Sprott. 50,000m+ 2026 campaign (32 holes, Phase 1).
            2025 breakthrough: 901m @ 0.43% CuEq and 618m @ 0.77% CuEq from surface.
            First 2026 holes imminent. Wide low-grade porphyry — 100m+ intercepts are the norm.
            BUY threshold: >0.4% CuEq over >100m. STRONG BUY: >0.6% CuEq over >200m.
  KFR.V  — Kingfisher Metals. Hank porphyry Cu-Au, Golden Triangle, BC.
            Discovery hole: 425m @ 0.40% CuEq. 3 rigs, 15,000m, mid-June start.
            $30M bought-deal funded. Geophysics expanding targets pre-drill.
            First hole results expected July–August 2026. Watch for step-out holes
            that confirm zone expansion beyond the 425m discovery intercept.

SILVER / BASE METALS (high-grade silver — grade thresholds are very different from gold):
  BBB.V  — Brixton Metals. Langis silver mine (Ontario) + Thorn Cu-Au porphyry (BC).
            Serial high-grade silver assays: 18.2m @ 3,638 g/t Ag; 13.0m @ 594 g/t Ag.
            60,000m program ongoing. 8 PRs in 60 days — this fires frequently.
            Use silver thresholds strictly: BUY ≥100 g/t, STRONG BUY ≥300 g/t.
            Thorn copper results arriving mid-2026 — use copper porphyry thresholds there.
  CGNT.V — Copper Giant Resources. Mocoa Cu-Mo deposit, Colombia.
            12.7-billion-pound Cu-Mo resource. PEA completion H2 2026.
            Drill results: 258m @ 0.70% CuEq (May 20) confirmed resource model.
            Colombia jurisdiction: factor in 5–10% discount vs Canadian projects.
            PEA release = binary catalyst. 3 rigs active, directional drilling.
  MGG.V  — Minaurum Silver. Alamos Silver project, Sonora, Mexico.
            55 Moz AgEq initial resource at 320 g/t AgEq (Jan 28 2026).
            6 rigs, Phase 2 50,000m expansion. May 27: 3.20m @ 882 g/t AgEq.
            Mexico jurisdiction: legal mining titles in place. Silver bull market tailwind.
            Resource expansion news = BUY if AgEq grade maintained or improved vs initial.

ENERGY (quarterly earnings + operational news):
  HME.V  — Hemisphere Energy. Atlee Buffalo polymer flood, SE Alberta.
            Extremely consistent — growing production each quarter.
            Watch for: production record (boe/d), funds flow beat, dividend increase.
            Any guidance raise = STRONG BUY.
  ALV.V  — Alvopetro Energy. Brazil natural gas. Quarterly results + production updates.
            Revenue in USD. Watch: Caburé field production, gas sales volumes, dividends.
  BNE.TO — Bonterra Energy. Pembina Cardium light oil, Alberta.
  PNE.TO — Pine Cliff Energy. Natural gas-weighted, Alberta.
  JOY.TO — Journey Energy. Conventional oil, central Alberta.

══════════════════════════════════════════════════════
JUNE 2026 ADDITIONS (Cowork discovery — all yfinance longName-validated)
══════════════════════════════════════════════════════
COPPER:
  MMA.V  — Midnight Sun Mining. Dumbwa Cu, Zambia. >5.3km strike, active drilling. Copper thresholds.
  MOG.V  — Mogotes Metals. Filo Sur Cu-Au, Argentina (Vicuña belt, along strike from BHP/Lundin Filo del Sol).
            86m @ 0.7% Cu discovery — binary porphyry hunt. Argentina jurisdiction discount.
  NIM.V  — Nicola Mining. New Craigmont brownfield Cu, BC + toll-mill cash flow. Catalyst is exploration, not production.
GOLD:
  BOGO.V — Borealis Mining. Borealis mine (Nevada) first production + Sandman PEA. Near-production: operational + dev catalysts.
  MGM.V  — Maple Gold Mines. Abitibi, Quebec. 30,000m Phase II; maiden Joutel MRE H1 2026 = binary. Gold thresholds.
  CBR.V  — Cabral Gold. Cuiú Cuiú, Brazil. 6 rigs; MG starter-pit first gold pour Q4'26. Brazil jurisdiction.
  ONYX.V — Onyx Gold. Timmins, Ontario. 110,000m, 4 rigs — very active driller. Gold thresholds.
  PRG.V  — Precipitate Gold. Pueblo Grande Norte, Dominican Rep. (adjacent Barrick Pueblo Viejo). Active drilling.
  GWM.V  — Galway Metals. Clarence Stream, New Brunswick. Relentless monthly high-grade hits (e.g. 20.7 g/t/11m), ~2.2M oz.
  KTO.V  — K2 Gold. Mojave, California. Permitted oxide gold, multi-target. Thin liquidity — needs a strong move to clear filters.
  DRY.V  — Dryden Gold. Hyndman, Ontario. High-grade (23.3 g/t/2.8m), funded 2026 drill. Thin liquidity.
  ECR.V  — Cartier Resources. Cadillac/Val-d'Or, Abitibi. 7.1 g/t/8m new shallow zone, 2 rigs. Thin liquidity.
SILVER (use silver thresholds: BUY ≥100 g/t, STRONG BUY ≥300 g/t):
  GRSL.V — GR Silver Mining. San Marcial, Mexico. Active high-grade silver assays.
  KTN.V  — Kootenay Silver. Columba, Mexico. Consistent high-grade silver drilling.
  IPT.V  — IMPACT Silver. Zacualpan PRODUCER, Mexico. Q1'26 rev tripled to $31.2M, record net income — earnings surprises matter.
  OCG.TO — Outcrop Silver & Gold. Santa Ana, Colombia. High-grade silver. Colombia jurisdiction discount.
  SVE.V  — Silver One Resources. Candelaria past-producer restart, Nevada. Thin liquidity.
  BPAG.V — BP Silver. Cosuño, Bolivia. 600 g/t Ag/5m. Bolivia jurisdiction — high discount.
ENERGY:
  YGR.TO — Yangarra Resources. Belly River light oil, Alberta. Quarterly producer (Q1 9,638 boe/d). Earnings/production beats = BUY.
  LTC.V  — Lotus Creek Exploration. Belly River light oil (spun from Gear). Fast-growing — Q1 FFO $10.6M, 75% growth guided.
  PRQ.TO — Petrus Resources. Cardium light oil, Alberta. Quarterly producer (Q1 +13% to 10,054 boe/d, Harmattan acq.).
            NOTE: symbol is Petrus Resources, NOT Perpetual Energy (re-added June 2026 with the correct label).

══════════════════════════════════════════════════════
COMMON TRAPS — DO NOT GET FOOLED BY THESE
══════════════════════════════════════════════════════
1. "Record revenue" alongside widening net loss → SKIP, not BUY
2. Good drill result + concurrent PP at discount → CAUTION, not BUY
3. Drill intercept with no grade or width numbers → CAUTION (they're hiding bad results)
4. "Mineralization encountered" with no assay numbers → CAUTION (pending = unknown)
5. "We are pleased to announce" with no financial/operational substance → CAUTION
6. Single narrow high-grade interval (e.g. 1m @ 50 g/t) in otherwise weak hole → CAUTION
7. Resource "restatement" or "technical report update" with same or smaller numbers → SKIP
8. "Strategic review" with no acquirer named → CAUTION (may go nowhere)
9. Earnings release showing Q4 beat but full-year miss → CAUTION
10. Production update that hits guidance exactly (not beats) → CAUTION (already priced in)

══════════════════════════════════════════════════════
OUTPUT FORMAT — return ONLY valid JSON, no markdown, no extra text
══════════════════════════════════════════════════════
{
  "signal": "BUY" | "STRONG BUY" | "CAUTION" | "SKIP",
  "confidence": 0.0-1.0,
  "reasoning": "One sentence: state the specific catalyst and why it is/isn't strong enough.",
  "key_numbers": {"grade": "X g/t", "width": "Xm", "depth": "Xm", ...},
  "release_type": "earnings" | "drill" | "guidance" | "deal" | "resource" | "operational" | "other"
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

    # Append commodity context to user content
    commodity_ctx = get_commodity_context(ticker, sector)
    if commodity_ctx:
        content += f"\n\n{commodity_ctx}"

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


def get_commodity_context(ticker: str, sector: str) -> str:
    """
    Returns a one-line commodity price context string for Claude prompts.
    E.g. "Commodity context: Gold ↓1.2% today"
    Cached for 1 hour to avoid hammering yfinance.
    """
    global _commodity_cache, _commodity_cache_ts
    now = datetime.now(EASTERN)
    if _commodity_cache_ts is None or (now - _commodity_cache_ts).seconds > 3600:
        _commodity_cache = {}
        for name, sym in _COMMODITY_SYMBOLS.items():
            try:
                hist = yf.Ticker(sym).history(period="2d", interval="1d", auto_adjust=True)
                if len(hist) >= 2:
                    prev = float(hist["Close"].iloc[-2])
                    curr = float(hist["Close"].iloc[-1])
                    _commodity_cache[name] = round((curr - prev) / prev * 100, 2)
            except Exception:
                pass
        _commodity_cache_ts = now

    commodity = _TICKER_COMMODITY.get(ticker, "oil" if sector == "Energy" else "gold")
    pct = _commodity_cache.get(commodity)
    if pct is None:
        return ""
    arrow = "↑" if pct > 0 else "↓"
    sentiment = " (headwind)" if (pct < -1.5) else (" (tailwind)" if pct > 1.5 else "")
    return f"Commodity context: {commodity.title()} {arrow}{abs(pct):.1f}% today{sentiment}"


# ══════════════════════════════════════════════════════════════════════════════
# Exit intelligence — position tracking + follow-up PR analysis
# ══════════════════════════════════════════════════════════════════════════════

def load_positions() -> list[dict]:
    if POSITIONS_FILE.exists():
        try:
            with open(POSITIONS_FILE) as f:
                data = json.load(f)
            return data if isinstance(data, list) else []
        except Exception:
            return []   # corrupt positions file must not crash startup
    return []


def save_positions(positions: list[dict]) -> None:
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(POSITIONS_FILE, "w") as f:
        json.dump(positions, f, indent=2)


def add_position(r: dict) -> None:
    """Record a BUY/STRONG BUY signal as an open position for exit monitoring."""
    if r.get("signal") not in ("BUY", "STRONG BUY"):
        return
    if r.get("cluster_capped"):
        return  # correlated same-commodity cluster — don't stack a new position
    positions = load_positions()
    if any(p["ticker"] == r["ticker"] for p in positions):
        return  # already tracking this ticker
    positions.append({
        "ticker":         r["ticker"],
        "company":        r["company"],
        "sector":         r["sector"],
        "entry_date":     datetime.now(EASTERN).strftime("%Y-%m-%d"),
        "entry_price":    r.get("price"),
        "signal":         r["signal"],
        "original_title": r["title"],
        "original_url":   r["url"],
        "ai_reasoning":   r.get("ai_reasoning", ""),
    })
    save_positions(positions)


def expire_positions() -> None:
    """Remove positions older than D+3 (auto-expire after hold window)."""
    positions = load_positions()
    now = datetime.now(EASTERN)
    fresh = []
    for p in positions:
        try:
            entry_dt = datetime.strptime(p["entry_date"], "%Y-%m-%d").replace(tzinfo=EASTERN)
            if (now - entry_dt).days <= 3:
                fresh.append(p)
        except Exception:
            fresh.append(p)
    save_positions(fresh)


def analyze_exit_with_claude(
    position: dict,
    new_title: str,
    new_body: str | None,
    current_price: float | None,
) -> dict | None:
    """
    Given an open position and a follow-up press release, advise HOLD or CUT.
    Returns dict with action, confidence, reasoning — or None if unavailable.
    """
    client = _get_anthropic_client()
    if client is None:
        return None

    price_line = ""
    if position.get("entry_price") and current_price:
        pct = (current_price - position["entry_price"]) / position["entry_price"] * 100
        price_line = f"Current price: ${current_price} ({pct:+.1f}% vs entry ${position['entry_price']})\n"

    content = (
        f"You are managing an open position. A new press release from the same company just dropped.\n\n"
        f"OPEN POSITION\n"
        f"Ticker:       {position['ticker']} ({position['sector']})\n"
        f"Entry date:   {position['entry_date']}\n"
        f"Entry signal: {position['signal']}\n"
        f"Entry reason: {position.get('ai_reasoning') or 'N/A'}\n"
        f"Original PR:  {position['original_title']}\n"
        f"{price_line}\n"
        f"NEW PRESS RELEASE\n"
        f"Headline: {new_title}\n"
        + (f"Body:\n{new_body[:2000]}" if new_body else "(headline only)")
        + "\n\nShould we HOLD to D+3 or CUT now?\n"
        "Return ONLY valid JSON:\n"
        '{"action": "STRONG HOLD" | "HOLD" | "CUT" | "STRONG CUT", '
        '"confidence": 0.0-1.0, "reasoning": "One sentence."}'
    )

    try:
        response = _get_anthropic_client().messages.create(
            model="claude-haiku-4-5",
            max_tokens=200,
            messages=[{"role": "user", "content": content}],
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```(?:json)?\n?", "", text).strip("`").strip()
        result = json.loads(text)
        assert result.get("action") in ("STRONG HOLD", "HOLD", "CUT", "STRONG CUT")
        result["confidence"] = float(result.get("confidence", 0.5))
        return result
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# Utilities
# ══════════════════════════════════════════════════════════════════════════════

def is_market_hours(dt: datetime | None = None) -> bool:
    now = dt or datetime.now(EASTERN)
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return MARKET_OPEN <= t < MARKET_CLOSE


def is_premarket(dt: datetime | None = None) -> bool:
    now = dt or datetime.now(EASTERN)
    if now.weekday() >= 5:
        return False
    t = (now.hour, now.minute)
    return PREMARKET_OPEN <= t < MARKET_OPEN


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

    if r.get("cluster_note"):
        lines.append(_tg_escape(r["cluster_note"]))

    lines += [
        f"*Exit:* Green D+1 hold D+3 | Red D+1 cut",
        f"",
        safe_title,
        r.get("url", ""),
    ]
    send_telegram("\n".join(l for l in lines if l))


def tg_exit_advisory(position: dict, exit_result: dict, new_title: str, url: str) -> None:
    """Send a hold/cut advisory for an open position."""
    action  = exit_result["action"]
    emoji   = {"STRONG HOLD": "🟢🟢", "HOLD": "🟢", "CUT": "🔴", "STRONG CUT": "🔴🔴"}.get(action, "")
    conf    = exit_result.get("confidence", 0)
    safe_co = _tg_escape(position["company"])
    safe_r  = _tg_escape(exit_result.get("reasoning", "")[:120])
    safe_t  = _tg_escape(new_title[:100])
    lines   = [
        f"{emoji} *EXIT ADVISORY: {action}* — {position['ticker']}",
        f"{safe_co}  |  held since {position['entry_date']}",
        f"",
        f"*AI ({conf:.0%}):* {safe_r}",
        f"*New PR:* {safe_t}",
        url,
    ]
    send_telegram("\n".join(l for l in lines if l))


def tg_premarket_open(n_tickers: int) -> None:
    send_telegram(
        f"🌅 *TSX Watcher — Pre-Market Scan Active*\n"
        f"Watching {n_tickers} tickers for early PRs (7:00–9:30 ET)\n"
        f"Signals fire without move gate — watch at open for confirmation"
    )


def tg_premarket_signal(r: dict) -> None:
    emoji  = "📋"
    signal = r.get("signal", "")
    conf   = r.get("ai_confidence")
    conf_str = f" ({conf:.0%})" if conf else ""
    reasoning = r.get("ai_reasoning", "")
    msg = (
        f"{emoji} *PRE-MARKET WATCH — {r['ticker']}*\n"
        f"{r['company']}  |  {r['timestamp']}\n\n"
        f"*{signal}*{conf_str}"
        + (f"\nAI: {reasoning[:120]}" if reasoning else "")
        + (f"\n{r['cluster_note']}" if r.get("cluster_note") else "") +
        f"\n\n_{r['title'][:100]}_\n"
        f"⚠️ No price yet — watch open move ≥{'10' if r['sector']=='Energy' else '15'}%\n"
        f"{r['url']}"
    )
    send_telegram(msg)


def tg_market_open(n_tickers: int) -> None:
    send_telegram(
        f"📈 *TSX Watcher — Market Open*\n"
        f"Watching {n_tickers} small-cap tickers\n"
        f"Polling GlobeNewswire + BNN Market Call every 5 min\n"
        f"Market hours: 9:30-16:00 ET"
    )


def count_signals_today() -> int:
    """Count today's logged entry signals from signals.log.

    signals_today is per-process and resets when Run A hands off to Run B, so the
    end-of-day close summary (sent by Run B) must read the log for the true total.
    Pre-market watch alerts aren't logged, so this counts actionable entries only.
    """
    if not LOG_FILE.exists():
        return 0
    today = datetime.now(EASTERN).strftime("%Y-%m-%d")
    n = 0
    try:
        with open(LOG_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                if str(rec.get("timestamp", "")).startswith(today):
                    n += 1
    except Exception:
        return 0
    return n


def tg_market_close(n_signals: int) -> None:
    # Prefer the log-derived count (survives the Run A → Run B handoff); fall back
    # to the in-process counter if the log can't be read.
    total = max(count_signals_today(), n_signals)
    if total == 0:
        send_telegram("📉 *TSX Watcher — Market Closed*\nNo signals today.")
    else:
        send_telegram(
            f"📉 *TSX Watcher — Market Closed*\n"
            f"{total} signal(s) fired today — check signals.log for details."
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
        try:
            with open(SEEN_FILE) as f:
                data = json.load(f)
            return set(data) if isinstance(data, (list, set)) else set()
        except Exception as e:
            # A corrupt seen cache must NOT crash startup — rebuild empty (worst case:
            # a few already-seen URLs re-fire once, then re-cache). Never a hard stop.
            print(f"  ⚠ seen_urls.json unreadable ({e}) — starting with empty cache")
    return set()


def save_seen(seen: set) -> None:
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        json.dump(list(seen), f)


def log_signal(record: dict) -> None:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


# ══════════════════════════════════════════════════════════════════════════════
# Forward-outcome logger — the dataset's 'y' column
# Joins each signal to its D+1/D+3 outcome at MULTIPLE entry points (D0 open, D0
# close, intraday signal price). This is what lets us later test whether PR content
# predicts returns *beyond the raw intraday move* — and at an entry we could actually
# hit. Runs daily; idempotent; backfills D+1 then D+3 as trading days roll forward.
# ══════════════════════════════════════════════════════════════════════════════

def _ret(entry, exit_) -> float | None:
    if entry and exit_ and entry > 0:
        return round((exit_ - entry) / entry * 100, 2)
    return None


def log_outcomes(verbose: bool = False) -> None:
    """For every logged signal, fill in D0 open/close + D+1/D+3 closes and the forward
    return from each entry candidate. Cheap: one daily-bar fetch per still-incomplete
    signal, skipped once D+3 is recorded."""
    if not LOG_FILE.exists():
        print("  No signals.log yet — nothing to score.")
        return
    try:
        outcomes = json.loads(OUTCOMES_FILE.read_text()) if OUTCOMES_FILE.exists() else {}
    except Exception:
        outcomes = {}

    today = datetime.now(EASTERN).date()
    seen_ids: set[str] = set()
    updated = 0

    for line in LOG_FILE.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        tkr = r.get("ticker")
        if not tkr or tkr == "SECTOR":          # skip BNN sector picks (no single ticker)
            continue
        ts = (r.get("timestamp") or "").strip()
        sid = f"{tkr}|{ts}"
        if sid in seen_ids:
            continue
        seen_ids.add(sid)

        rec = outcomes.get(sid, {})
        if rec.get("d3_close") is not None or rec.get("stale"):   # resolved, or gave up
            continue
        try:
            sig_date = datetime.strptime(ts.replace(" ET", "").strip(), "%Y-%m-%d %H:%M").date()
        except Exception:
            continue
        days_since = (today - sig_date).days
        if days_since < 1:                       # need at least D+1 to exist
            continue
        if rec.get("d1_close") is not None and days_since < 3:
            continue                              # have D+1, D+3 not available yet

        try:
            hist = yf.Ticker(tkr).history(
                start=sig_date.isoformat(),
                end=(sig_date + timedelta(days=12)).isoformat(),
                interval="1d", auto_adjust=True,
            )
        except Exception:
            hist = None
        if hist is None or hist.empty:
            # No data after trying. Give up ONLY if it's old enough that data is never
            # coming (delisted/halted) — stamp stale so we stop re-fetching forever.
            # (Must be AFTER the fetch: old-but-valid signals still need backfilling.)
            if days_since > 20:
                rec["stale"] = True
                outcomes[sid] = rec
                updated += 1
            continue
        opens  = [float(x) for x in hist["Open"].tolist()]
        closes = [float(x) for x in hist["Close"].tolist()]
        if not opens:
            continue

        d0_open  = opens[0]
        d0_close = closes[0]
        d1_close = closes[1] if len(closes) > 1 else None
        d3_close = closes[3] if len(closes) > 3 else None   # D0=idx0 … D+3=idx3 (trading days)
        sig_px   = r.get("price")                            # intraday entry (None pre-market)

        rec.update({
            "ticker":        tkr,
            "signal_ts":     ts,
            "signal":        r.get("signal"),
            "score":         r.get("score"),
            "ai_confidence": r.get("ai_confidence"),
            "premarket":     r.get("premarket"),
            "intraday_pct":  r.get("intraday_pct"),   # the MOVE to control for
            "spread_pct":    r.get("spread_pct"),     # fill-quality proxy
            "sig_px":        sig_px,
            "d0_open":       round(d0_open, 4),
            "d0_close":      round(d0_close, 4),
            "d1_close":      round(d1_close, 4) if d1_close else None,
            "d3_close":      round(d3_close, 4) if d3_close else None,
            # forward return from each ENTRY candidate (the experiment's y, by entry)
            "ret_open_d1":     _ret(d0_open,  d1_close),
            "ret_open_d3":     _ret(d0_open,  d3_close),
            "ret_close_d1":    _ret(d0_close, d1_close),
            "ret_close_d3":    _ret(d0_close, d3_close),
            "ret_sigpx_d1":    _ret(sig_px,   d1_close),
            "ret_sigpx_d3":    _ret(sig_px,   d3_close),
        })
        outcomes[sid] = rec
        updated += 1

    OUTCOMES_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTCOMES_FILE.write_text(json.dumps(outcomes, indent=2, sort_keys=True))
    resolved = sum(1 for v in outcomes.values() if v.get("d3_close") is not None)
    print(f"  Outcomes: {updated} updated, {len(outcomes)} tracked, {resolved} fully resolved (D+3).")


# ══════════════════════════════════════════════════════════════════════════════
# Autonomous PAPER trader (simulated — no real money, no broker)
# A deterministic forward-walking simulation over the live signals. Parameters are
# tuned to the stress test: enter only confirmed +15–40% opening-gap movers (the
# validated band), fill at D0 open with modeled spread+commission, hold to D+3 close
# (best total P&L / Sharpe — beats Rule-3 exit), size ~half-Kelly with a cluster cap.
# Reproducible (a pure function of signals.log + market data) and idempotent.
# ══════════════════════════════════════════════════════════════════════════════

PAPER_START_EQUITY = 10_000.0   # simulated starting account
PAPER_RISK_FRAC    = 0.08       # 8% of equity per trade (~half-Kelly, trimmed for thin realistic edge)
PAPER_MAX_DEPLOYED = 0.60       # cap total at-risk capital
PAPER_GAP_MIN      = 15.0       # only enter confirmed +15% opening-gap movers (edge dies below)
PAPER_GAP_MAX      = 40.0       # 40%+ reverses (-9.5% close-entry) — skip
PAPER_HOLD_TDAYS   = 3          # exit at D+3 close (stress test: best total P&L + Sharpe)
PAPER_MAX_HOLD_DAYS = 10        # force-settle if D+3 never resolves (delisted/halted) — no perpetual open positions
PAPER_SLIPPAGE     = 0.0075     # one-way haircut when spread unknown (nano-cap reality)
IBKR_PER_SHARE     = 0.0035     # IBKR Canada commission
IBKR_MIN_COMM      = 1.0


def _paper_commission(shares: float) -> float:
    return max(IBKR_MIN_COMM, shares * IBKR_PER_SHARE)


def _paper_window(ticker: str, sig_date) -> dict | None:
    """Daily bars around a signal: prev_close (gap denom), D0 open/close, D+3 close."""
    try:
        hist = yf.Ticker(ticker).history(
            start=(sig_date - timedelta(days=6)).isoformat(),
            end=(sig_date + timedelta(days=12)).isoformat(),
            interval="1d", auto_adjust=True,
        )
    except Exception:
        return None
    if hist is None or hist.empty:
        return None
    idx = [d.date() for d in hist.index]
    opens  = [float(x) for x in hist["Open"].tolist()]
    closes = [float(x) for x in hist["Close"].tolist()]
    # D0 = first trading day on/after the signal date
    d0 = next((i for i, d in enumerate(idx) if d >= sig_date), None)
    if d0 is None or d0 == 0:                    # need a prior close for the gap
        return None
    return {
        "prev_close": closes[d0 - 1],
        "d0_open":    opens[d0],
        "d0_date":    idx[d0].isoformat(),
        "d3_close":   closes[d0 + PAPER_HOLD_TDAYS] if len(closes) > d0 + PAPER_HOLD_TDAYS else None,
        "d3_date":    idx[d0 + PAPER_HOLD_TDAYS].isoformat() if len(idx) > d0 + PAPER_HOLD_TDAYS else None,
    }


def _paper_load() -> dict:
    if PAPER_FILE.exists():
        try:
            return json.loads(PAPER_FILE.read_text())
        except Exception:
            pass
    return {"cash": PAPER_START_EQUITY, "start_equity": PAPER_START_EQUITY,
            "positions": {}, "closed": [], "processed": [], "equity_curve": []}


def _paper_save(p: dict) -> None:
    PAPER_FILE.parent.mkdir(parents=True, exist_ok=True)
    PAPER_FILE.write_text(json.dumps(p, indent=2, sort_keys=True))


def run_paper_trader(verbose: bool = False) -> None:
    """
    Walk signals.log forward: open paper positions on confirmed +15–40% gap movers at
    the D0 open, hold to D+3 close, mark equity. Deterministic + idempotent — replays
    only unprocessed signals and settles positions whose D+3 has arrived.
    """
    if not LOG_FILE.exists():
        print("  No signals.log yet — paper trader idle.")
        return
    p = _paper_load()
    processed = set(p["processed"])
    today = datetime.now(EASTERN).date()

    # Gather candidate signals (non-SKIP), in chronological order
    rows = []
    for line in LOG_FILE.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except Exception:
            continue
        tkr = r.get("ticker")
        if not tkr or tkr == "SECTOR" or r.get("signal") == "SKIP":
            continue
        ts = (r.get("timestamp") or "").strip()
        try:
            sig_dt = datetime.strptime(ts.replace(" ET", "").strip(), "%Y-%m-%d %H:%M")
        except Exception:
            continue
        rows.append((sig_dt, f"{tkr}|{ts}", r))
    rows.sort(key=lambda x: x[0])

    entries, exits = [], []

    # ── 1. Settle exits first (frees cash for same-run entries) ───────────────
    for sid, pos in list(p["positions"].items()):
        try:
            entry_d = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
        except Exception:
            entry_d = today
        w   = _paper_window(pos["ticker"], entry_d)
        d3  = w["d3_close"] if w else None
        age = (today - entry_d).days
        exit_date = None
        if d3 is not None:
            exit_px, exit_date = d3 * (1 - PAPER_SLIPPAGE), (w["d3_date"] if w else today.isoformat())
            forced = False
        elif age > PAPER_MAX_HOLD_DAYS:
            # D+3 never resolved (delisted/halted/illiquid). Force-settle so the
            # position can't stay open forever and tie up simulated capital. Use the
            # last known price, else cost basis (breakeven) — never fabricate a result.
            last = get_current_price(pos["ticker"])
            exit_px = (last * (1 - PAPER_SLIPPAGE)) if (last and last > 0) else (pos["cost_basis"] / max(pos["shares"], 1))
            exit_date, forced = today.isoformat(), True
        else:
            continue   # still within the resolution window — wait
        comm     = _paper_commission(pos["shares"])
        proceeds = pos["shares"] * exit_px - comm
        pnl      = proceeds - pos["cost_basis"]
        p["cash"] += proceeds
        rec = {**pos, "exit_date": exit_date, "exit_px": round(exit_px, 4),
               "pnl": round(pnl, 2), "ret_pct": round(pnl / pos["cost_basis"] * 100, 2)}
        if forced:
            rec["forced"] = True
        p["closed"].append(rec)
        del p["positions"][sid]
        exits.append((pos["ticker"], round(pnl, 2), round(pnl / pos["cost_basis"] * 100, 2)))

    # ── 2. Process new signals → maybe open positions ─────────────────────────
    day_commodity: set[tuple] = set()   # cluster cap within this batch (commodity, date)
    for c in p["positions"].values():   # seed with already-open same-day commodities
        day_commodity.add((c.get("commodity"), c.get("entry_date")))

    for sig_dt, sid, r in rows:
        if sid in processed:
            continue
        sig_date = sig_dt.date()
        if (today - sig_date).days < 1:          # D0 not resolved yet — revisit next run
            continue
        w = _paper_window(r["ticker"], sig_date)
        if not w:
            processed.add(sid); continue
        gap = (w["d0_open"] - w["prev_close"]) / w["prev_close"] * 100 if w["prev_close"] else 0.0
        commodity = r.get("commodity") or get_commodity(r["ticker"])
        d0d = w["d0_date"]

        # entry gate: confirmed +15–40% gap, cluster not capped, cash/deploy room
        equity_now = p["cash"] + sum(c["cost_basis"] for c in p["positions"].values())
        deployed   = sum(c["cost_basis"] for c in p["positions"].values())
        reason = None
        if not (PAPER_GAP_MIN <= gap < PAPER_GAP_MAX):
            reason = f"gap {gap:+.1f}% outside +{PAPER_GAP_MIN:.0f}–{PAPER_GAP_MAX:.0f}%"
        elif (commodity, d0d) in day_commodity:
            reason = f"cluster-capped ({commodity} already traded {d0d})"
        elif deployed >= PAPER_MAX_DEPLOYED * equity_now:
            reason = "max deployed"
        if reason:
            if verbose: print(f"  ↷ paper skip {r['ticker']} ({d0d}): {reason}")
            processed.add(sid); continue

        fill_px = w["d0_open"] * (1 + PAPER_SLIPPAGE)
        budget  = min(PAPER_RISK_FRAC * equity_now, p["cash"] - 1.0)
        shares  = int(budget / fill_px) if fill_px > 0 else 0
        if shares < 1:
            processed.add(sid); continue
        comm       = _paper_commission(shares)
        cost_basis = shares * fill_px + comm
        if cost_basis > p["cash"]:
            processed.add(sid); continue
        p["cash"] -= cost_basis
        p["positions"][sid] = {
            "ticker": r["ticker"], "commodity": commodity, "signal": r.get("signal"),
            "entry_date": d0d, "entry_px": round(fill_px, 4), "shares": shares,
            "cost_basis": round(cost_basis, 2), "gap_pct": round(gap, 1),
        }
        day_commodity.add((commodity, d0d))
        processed.add(sid)
        entries.append((r["ticker"], r.get("signal"), round(gap, 1), shares, round(fill_px, 4)))

    # ── 3. Mark equity, persist, report ───────────────────────────────────────
    mkt_value = 0.0
    for pos in p["positions"].values():
        last = get_current_price(pos["ticker"]) or (pos["cost_basis"] / pos["shares"])
        mkt_value += pos["shares"] * last
    equity = round(p["cash"] + mkt_value, 2)
    p["processed"] = sorted(processed)
    # One equity point per DAY (Run A + Run B + restarts all hit this) — replace today's.
    p["equity_curve"] = [pt for pt in p.get("equity_curve", []) if pt.get("date") != today.isoformat()]
    p["equity_curve"].append({"date": today.isoformat(), "equity": equity})
    _paper_save(p)

    wins = [c for c in p["closed"] if c["pnl"] > 0]
    winr = (len(wins) / len(p["closed"]) * 100) if p["closed"] else 0.0
    total_pl = equity - p["start_equity"]
    print(f"  Paper: equity ${equity:,.0f} ({total_pl:+,.0f} / {total_pl/p['start_equity']*100:+.1f}%), "
          f"{len(p['positions'])} open, {len(p['closed'])} closed, win {winr:.0f}%")

    # Telegram on activity
    if entries or exits:
        lines = ["📊 *Paper Trader*"]
        for t, sig, gap, sh, px in entries:
            lines.append(f"🟢 BUY {_tg_escape(t)} ({sig}, gap +{gap}%) — {sh} sh @ \\${px}")
        for t, pnl, ret in exits:
            emo = "🟩" if pnl > 0 else "🟥"
            lines.append(f"{emo} EXIT {_tg_escape(t)} D+3 — {pnl:+.0f} ({ret:+.1f}%)")
        lines.append(f"\n_Equity \\${equity:,.0f} ({total_pl:+,.0f}), {len(p['positions'])} open · win {winr:.0f}% over {len(p['closed'])}_")
        send_telegram("\n".join(lines))


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
# Trading halt check (yfinance volume proxy — CIRO's page is Cloudflare-blocked)
# Strategy: "it really works as long as there is no trading halt"
# ══════════════════════════════════════════════════════════════════════════════

def is_halted(ticker: str) -> bool:
    """
    Lazy per-candidate trading-halt check via yfinance volume proxy.

    CIRO's halt page (ciro.ca) is behind Cloudflare and blocks automated requests,
    so we infer halts from price data instead. During market hours, if a ticker's
    last two consecutive 1-min bars both have 0 volume, the stock is likely halted.
    A single zero-volume bar can occur at open or in thin trading — require 2.

    This is only called for tickers that already matched a press release (a small
    handful per poll), NOT all 28 every cycle. The yfinance fetch is shared with
    get_price_data via its TTL cache, so the halt check costs no extra request.

    Limitation: pre-market halts aren't detectable (no 1-min data). Pre-market
    signals already bypass this check since there's no move gate anyway.
    """
    now = datetime.now(EASTERN)
    if not is_market_hours(now):
        return False
    d = get_price_data(ticker)
    return bool(d and d.get("halted"))


# ══════════════════════════════════════════════════════════════════════════════
# Press release fetching
# ══════════════════════════════════════════════════════════════════════════════

FETCH_HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0", "Accept": "text/html,*/*"}
WIRE_DOMAINS  = ("globenewswire", "prnewswire", "businesswire", "accesswire", "newswire", "newsfilecorp")


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

    raw = title + " " + summary

    # 2. Dotted ticker symbol in text: (PRQ.TO), PRQ.V, etc.
    for pat in [r'\(([A-Z]{2,7})\.(?:TO|V)\)', r'\b([A-Z]{2,7})\.(?:TO|V)\b']:
        for m in re.finditer(pat, raw):
            sym_to = m.group(1) + ".TO"
            if sym_to in TICKERS: return sym_to
            sym_v  = m.group(1) + ".V"
            if sym_v  in TICKERS: return sym_v

    # 3. Exchange-prefixed ticker — the form press releases actually use:
    #    "(TSXV: PRG)", "(TSX-V:PRG)", "(TSX: BNE)". Check TSXV/.V before TSX/.TO
    #    (TSXV contains "TSX", so the .V variants must win first).
    for m in re.finditer(r'\bTSX[\s-]?V\s*:\s*([A-Z]{1,7})\b', raw, re.I):
        sym = m.group(1).upper() + ".V"
        if sym in TICKERS: return sym
    for m in re.finditer(r'\bTSX\s*:\s*([A-Z]{1,7})\b', raw, re.I):
        sym = m.group(1).upper() + ".TO"
        if sym in TICKERS: return sym

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
    # STRONG BUY (keyword path) requires a genuine guidance/earnings release —
    # NOT a drill PR. Drill-grade STRONG BUYs are judged on g/t thresholds in
    # the Claude path (analyze_with_claude); the keyword fallback must not
    # promote explorers to STRONG BUY off a stray guidance-word match.
    if score >= 2 and has_guidance and release_type in ("earnings", "guidance"):
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

        # Skip old episodes — BNN RSS contains months of back-catalogue.
        # Only fire on episodes published within the last 3 days.
        published = entry.get("published_parsed") or entry.get("updated_parsed")
        if published:
            import calendar
            pub_dt = datetime.fromtimestamp(calendar.timegm(published), tz=EASTERN)
            age_days = (datetime.now(EASTERN) - pub_dt).days
            if age_days > 3:
                if verbose:
                    print(f"  BNN skip (old episode, {age_days}d): {title[:55]}")
                continue

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

# ── Autonomous discovery / auto-add thresholds ──────────────────────────────────
# Deliberately STRICTER than the runtime watch floor: an autonomous add has no human
# veto, so it must clear a clean liquidity + cap + price bar before joining the code.
MAX_DISCOVERY_CAP       = 300_000_000   # small-cap ceiling
AUTO_ADD_MIN_DOLLAR_VOL = 100_000       # ≥$100k/day MEDIAN (vs the $50k runtime floor)
AUTO_ADD_MIN_PRICE      = 0.25          # fee-drag floor (IBKR ~1.75%/side eats sub-$0.25)
AUTO_ADD_MIN_Q25_VOL    = 30_000        # spike-guard: 25th-pct day must still trade ≥$30k
MAX_DISCOVERY_EVAL      = 40            # cap resolve+screen lookups per scan (Yahoo politeness)
# No per-week add cap: the quality gate (numeric + AI veto) is the sole throttle; a
# week with zero qualifiers adds nothing, a strong week may add several.

# AI model for the discovery veto + removal gate. Defaults to the same proven model
# the watcher uses (guaranteed compatible — a bad model string would fail-closed the
# front-door veto and silently stop ALL adds). Bump to a stronger model here if wanted.
DISCOVERY_AI_MODEL  = "claude-haiku-4-5"

# Recycle / removal lifecycle (auto-added names only; hand-curated core is exempt).
# "Trigger" = a BUY/STRONG BUY/CAUTION signal in signals.log (SKIP/silence don't count).
RECYCLE_QUIET_DAYS = 30   # no trigger for a month → Recycled (still watched, on the clock)
REMOVE_QUIET_DAYS  = 60   # a 2nd quiet month → AI-gated removal

# ── PR freshness gate ───────────────────────────────────────────────────────────
# RSS feeds carry a backlog; re-posted promos (e.g. "Named to the TSX Venture 50")
# and stale items can leak through and fire a signal with no fresh catalyst.
# Mirror the BNN feed's age filter on the press-release path. Fail-open: entries
# with no parseable date are NOT dropped (avoids losing real PRs that lack a date).
MAX_PR_AGE_DAYS = 3


def _entry_age_days(entry) -> float | None:
    """Age in days from a feed entry's published/updated time. None if no date."""
    published = entry.get("published_parsed") or entry.get("updated_parsed")
    if not published:
        return None
    import calendar
    pub_dt = datetime.fromtimestamp(calendar.timegm(published), tz=EASTERN)
    return (datetime.now(EASTERN) - pub_dt).total_seconds() / 86400.0


# ── Sector-cluster guard ─────────────────────────────────────────────────────────
# When the system fires several same-commodity names on one day (e.g. 4 gold
# juniors), they're almost certainly riding a sector-wide move, not independent
# alpha — and stacking them as separate entries badly understates correlation
# risk. Cap actionable (BUY/STRONG BUY) entries per commodity per day; beyond the
# cap, still alert but flag as a correlated cluster and DON'T auto-track a new
# position. Count is persisted so it survives the Run A→B handoff and restarts.
MAX_SIGNALS_PER_COMMODITY_PER_DAY = 2
CLUSTER_FILE = Path("data/signals/cluster_counts.json")


def bump_cluster_count(commodity: str) -> int:
    """Increment and return today's running count of actionable signals for a
    commodity. Resets automatically on a new day."""
    today = datetime.now(EASTERN).strftime("%Y-%m-%d")
    data  = {"date": today, "counts": {}}
    if CLUSTER_FILE.exists():
        try:
            loaded = json.load(open(CLUSTER_FILE))
            if loaded.get("date") == today:
                data = loaded
        except Exception:
            pass
    n = data["counts"].get(commodity, 0) + 1
    data["counts"][commodity] = n
    CLUSTER_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CLUSTER_FILE, "w") as f:
        json.dump(data, f)
    return n

_price_cache: dict = {}   # {ticker: (fetched_at_epoch, data_or_None)}
_PRICE_TTL = 60           # seconds — share one fetch between halt check + filters
_PRICE_RETRIES = 3        # yfinance attempts before giving up (Yahoo 429s/blips)


def _fetch_history(ticker: str):
    """yfinance fetch with retry + exponential backoff.

    Yahoo Finance occasionally returns 429s or empty results under transient
    throttling. Without a retry, a hiccup at the moment a signal fires would skip
    the move-gate/dollar-volume filters and cost the signal. Retries on exception
    OR empty result; backoff 0.6s → 1.8s. Returns the DataFrame, or None if every
    attempt failed (caller caches None and the next poll, 5 min later, retries).
    """
    delay = 0.6
    for attempt in range(_PRICE_RETRIES):
        try:
            hist = yf.Ticker(ticker).history(period="1d", interval="1m", auto_adjust=True)
            if not hist.empty:
                return hist
        except Exception:
            pass
        if attempt < _PRICE_RETRIES - 1:
            time.sleep(delay)
            delay *= 3
    return None


def _yf_info_retry(ticker: str) -> dict | None:
    """
    Fetch yfinance .info with retry + backoff. Returns the dict on success, or
    None if EVERY attempt failed/returned empty — so callers can tell a genuine
    "no data" (None) apart from real data. Critical for validate_universe: under
    Yahoo 429 throttling .info returns an empty/partial dict instead of raising,
    which would otherwise read as "delisted" and fire a false drift alert. None
    here means "couldn't verify" (treat as transient), NOT "delisted".
    """
    delay = 0.8
    for attempt in range(_PRICE_RETRIES):
        try:
            info = yf.Ticker(ticker).info
            if info and (info.get("longName") or info.get("shortName")):
                return info
        except Exception:
            pass
        if attempt < _PRICE_RETRIES - 1:
            time.sleep(delay)
            delay *= 3
    return None


def get_price_data(ticker: str) -> dict | None:
    """Returns current price, today's open, intraday move %, estimated dollar
    volume, and a `halted` flag.

    Cached for _PRICE_TTL seconds so the halt check (is_halted) and the
    price/liquidity/move filters reuse a single yfinance fetch per candidate
    instead of hitting the API twice. Poll cadence (5 min) is well above the TTL,
    so each poll cycle still gets fresh data.
    """
    now_ts = time.time()
    cached = _price_cache.get(ticker)
    if cached and now_ts - cached[0] < _PRICE_TTL:
        return cached[1]
    try:
        hist = _fetch_history(ticker)   # retries on transient Yahoo throttling
        if hist is None or hist.empty:
            _price_cache[ticker] = (now_ts, None)
            return None
        current  = round(float(hist["Close"].iloc[-1]), 2)
        open_px  = round(float(hist["Open"].iloc[0]), 2)
        intraday = (current - open_px) / open_px if open_px > 0 else 0.0
        # Estimate dollar volume: sum of (close * volume) across 1-min bars today
        dvol = float((hist["Close"] * hist["Volume"]).sum())
        # Halt proxy: 2 consecutive zero-volume 1-min bars (see is_halted)
        halted = False
        if len(hist) >= 2:
            last_two_vols = hist["Volume"].iloc[-2:].tolist()
            halted = all(v == 0 for v in last_two_vols)
        data = {
            "price":        current,
            "open":         open_px,
            "intraday_pct": round(intraday * 100, 2),
            "intraday_abs": round(abs(intraday) * 100, 2),
            "dollar_vol":   round(dvol, 0),
            "halted":       halted,
        }
        _price_cache[ticker] = (now_ts, data)
        return data
    except Exception:
        _price_cache[ticker] = (now_ts, None)
        return None


def get_current_price(ticker: str) -> float | None:
    d = get_price_data(ticker)
    return d["price"] if d else None


def _microstructure(ticker: str) -> tuple:
    """
    Best-effort LIVE bid/ask/spread at signal time — the one input that cannot be
    reconstructed later (historical intraday spread isn't in any free dataset).
    Returns (bid, ask, spread_pct) or (None, None, None).

    CAVEAT: yfinance bid/ask is unreliable for TSXV nano-caps (often stale/zero).
    This is a placeholder proxy; trustworthy fill-quality data needs the IBKR Level-1
    feed, which is the right home for this once execution is wired. Captured now so
    the dataset at least has *something* in the spread column to refine later.
    """
    try:
        info = yf.Ticker(ticker).info or {}
        bid, ask = info.get("bid"), info.get("ask")
        if bid and ask and bid > 0 and ask >= bid:
            mid = (ask + bid) / 2
            return round(float(bid), 4), round(float(ask), 4), round((ask - bid) / mid * 100, 2)
    except Exception:
        pass
    return None, None, None


# ══════════════════════════════════════════════════════════════════════════════
# TMX Newsfile scraper
# Newsfile is the dominant wire service for TSXV small caps. No public RSS,
# so we scrape category pages. Release IDs are sequential integers — we track
# seen URLs just like GlobeNewswire entries.
# ══════════════════════════════════════════════════════════════════════════════

def scrape_newsfile_categories(seen: set, verbose: bool = False) -> list[dict]:
    """Scrape TMX Newsfile category pages and return new entries matching our tickers.

    Each entry is a dict with 'url', 'title', 'summary' — same shape as RSS entries
    fed into poll_press_releases, so they go through the same signal pipeline.
    """
    entries = []
    seen_release_ids: set[str] = set()   # dedup within this poll cycle (same release in 2 categories)

    for cat_url in NEWSFILE_CATEGORIES:
        try:
            r = requests.get(cat_url, headers=FETCH_HEADERS, timeout=12)
            if r.status_code != 200:
                if verbose:
                    print(f"  Newsfile {cat_url.split('/')[-1]}: HTTP {r.status_code}")
                continue

            # Extract release slugs: /release/{id}/{slug}
            # Use set to dedup duplicates within same page (social share links repeat hrefs)
            matches = re.findall(r'/release/(\d+)/([^"\s\'<>&?]+)', r.text)
            unique = list(dict.fromkeys(matches))   # preserve order, drop duplicates

            for release_id, slug in unique:
                if release_id in seen_release_ids:
                    continue
                seen_release_ids.add(release_id)

                url = f"{NEWSFILE_BASE}/release/{release_id}/{slug}"
                if url in seen:
                    continue

                # Title: un-slug (hyphens → spaces, strip trailing junk)
                title = re.sub(r'-+', ' ', slug).strip()

                if verbose:
                    print(f"    Newsfile checking: {title[:65]}")

                ticker = match_ticker(title, "")
                if ticker is None:
                    continue

                # Found a match — mark seen and return the entry
                seen.add(url)
                entries.append({"url": url, "title": title, "summary": ""})
                if verbose:
                    print(f"  ✅ Newsfile match: {ticker} — {title[:55]}")

            # Polite 0.5s gap between category page requests
            time.sleep(0.5)

        except Exception as e:
            if verbose:
                print(f"  Newsfile error ({cat_url.split('/')[-1]}): {e}")

    return entries


# ══════════════════════════════════════════════════════════════════════════════
# Press release RSS polling
# ══════════════════════════════════════════════════════════════════════════════

def poll_press_releases(seen: set, verbose: bool = False, premarket: bool = False) -> tuple[list[dict], list[dict]]:
    signals         = []
    exit_advisories = []
    open_positions  = load_positions()
    fired_tickers:  set[str] = set()   # dedup within poll cycle — same PR on 2 wires = 1 signal

    # ── Collect candidates from all sources ──────────────────────────────────
    # Each candidate: (url, title, summary, ticker)
    # seen is updated here for RSS; scrape_newsfile_categories updates it internally.
    candidates: list[tuple[str, str, str, str]] = []

    # Source 1: GlobeNewswire RSS
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
            # Freshness gate — drop stale backlog / re-posted promos
            age = _entry_age_days(entry)
            if age is not None and age > MAX_PR_AGE_DAYS:
                if verbose:
                    print(f"    GNW skip (stale {age:.0f}d): {title[:55]}")
                continue
            if verbose:
                print(f"    GNW checking: {title[:65]}")
            ticker = match_ticker(title, summary)
            if ticker:
                candidates.append((url, title, summary, ticker))

    # Source 2: TMX Newsfile category pages (adds matching URLs to seen internally)
    for nf_entry in scrape_newsfile_categories(seen, verbose=verbose):
        ticker = match_ticker(nf_entry["title"], nf_entry["summary"])
        if ticker:
            candidates.append((nf_entry["url"], nf_entry["title"], nf_entry["summary"], ticker))

    # ── Process all candidates through the signal pipeline ───────────────────
    for url, title, summary, ticker in candidates:

        # Skip if this ticker already fired this poll cycle (same PR on two wires)
        if ticker in fired_tickers:
            if verbose:
                print(f"  ↷ {ticker} deduped — already fired this cycle from another source")
            continue
        fired_tickers.add(ticker)

        # Skip if stock is currently halted
        if is_halted(ticker):
            if verbose:
                print(f"  ⚠ {ticker} is halted — skipping")
            continue

        # Fetch full article body from wire services (includes newsfilecorp)
        body = None
        if any(d in url for d in WIRE_DOMAINS):
            body = fetch_body(url)

        # ── Exit intelligence: check if this is a follow-up PR for a held position ──
        held = next((p for p in open_positions if p["ticker"] == ticker
                     and url != p.get("original_url")), None)
        if held:
            current_price = get_current_price(ticker)
            exit_result   = analyze_exit_with_claude(held, title, body, current_price)
            if exit_result:
                exit_advisories.append({
                    "position":    held,
                    "exit_result": exit_result,
                    "new_title":   title,
                    "url":         url,
                })
                if verbose:
                    print(f"  📊 Exit advisory for {ticker}: {exit_result['action']} ({exit_result['confidence']:.0%})")
            continue  # don't also fire a new entry signal for a held position

        # ── Signal analysis: Claude AI first, keyword scoring as fallback ────
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

        # ── Intraday move gate + liquidity filters ────────────────────────────
        # Pre-market: market is closed, no price data available.
        # Skip all price-based filters — alert as WATCH AT OPEN instead.
        px           = get_price_data(ticker) if not premarket else None
        price        = px["price"]        if px else None
        intraday_pct = px["intraday_pct"] if px else None
        intraday_abs = px["intraday_abs"] if px else 0.0
        dollar_vol   = px["dollar_vol"]   if px else 0.0

        if not premarket:
            # Dollar volume filter: skip illiquid names (<$50k today)
            if px and dollar_vol < MIN_DOLLAR_VOL:
                if verbose:
                    print(f"  ↷ {ticker} filtered — dollar vol ${dollar_vol:,.0f} < ${MIN_DOLLAR_VOL:,}")
                continue

            # Move gate — lower bound: confirm something real is happening
            threshold = MIN_INTRADAY_ENERGY if sector == "Energy" else MIN_INTRADAY_MOVE
            if px and intraday_abs < threshold * 100:
                if verbose:
                    print(f"  ↷ {ticker} filtered — intraday {intraday_abs:.1f}% < {threshold*100:.0f}% gate")
                continue

            # Move cap — upper bound: 40%+ intraday movers reverse sharply
            if px and intraday_abs >= MAX_INTRADAY_MOVE * 100:
                if verbose:
                    print(f"  ↷ {ticker} filtered — intraday {intraday_abs:.1f}% ≥ {MAX_INTRADAY_MOVE*100:.0f}% reversal zone")
                continue

        sig       = analysis["signal"]
        exit_rule = "Hold to D+3 if green at D+1 close. Cut at D+1 close if red."

        # ── Sector-cluster guard ──────────────────────────────────────────────
        # Same-commodity names firing together = one correlated bet, not N.
        commodity      = get_commodity(ticker)
        cluster_capped = False
        cluster_note   = ""
        if sig in ("BUY", "STRONG BUY"):
            n = bump_cluster_count(commodity)
            if n > MAX_SIGNALS_PER_COMMODITY_PER_DAY:
                cluster_capped = True
                cluster_note = (
                    f"⚠️ {commodity.upper()} signal #{n} today — over the "
                    f"{MAX_SIGNALS_PER_COMMODITY_PER_DAY}/day cap. Likely a sector-wide "
                    f"{commodity} move, not independent alpha. NOT auto-tracked — size the "
                    f"whole {commodity} basket as ONE correlated position."
                )
                if verbose:
                    print(f"  ⚠ {ticker} cluster-capped — {commodity} signal #{n} today")
            elif n > 1:
                cluster_note = (
                    f"⚠️ {commodity.upper()} signal #{n} today — correlated with earlier "
                    f"{commodity} alert(s); size the basket as one position, not {n}."
                )

        source = "newsfile" if "newsfilecorp" in url else "press_release"
        # Microstructure is LIVE-only (can't be reconstructed) → capture for intraday
        # signals. Pre-market has no live book; its realistic entry is the D0 open,
        # which the outcome logger reconstructs later.
        bid, ask, spread_pct = _microstructure(ticker) if not premarket else (None, None, None)
        signals.append({
            "timestamp":      datetime.now(EASTERN).strftime("%Y-%m-%d %H:%M ET"),
            "ticker":         ticker,
            "company":        COMPANY_NAMES.get(ticker, ticker),
            "sector":         sector,
            "signal":         sig,
            "score":          analysis["score"],
            "release_type":   analysis["release_type"],
            "has_guidance":   analysis["has_guidance"],
            "pos_hits":       analysis["pos_hits"],
            "neg_hits":       analysis["neg_hits"],
            "ai_used":        ai_used,
            "ai_reasoning":   analysis.get("ai_reasoning", ""),
            "ai_confidence":  analysis.get("ai_confidence", None),
            "ai_key_numbers": analysis.get("ai_key_numbers", {}),
            "price":          price,            # intraday entry candidate (None pre-market)
            "open_d0":        px["open"] if px else None,   # D0 open entry candidate
            "bid":            bid,
            "ask":            ask,
            "spread_pct":     spread_pct,       # fill-quality proxy (live-only)
            "intraday_pct":   intraday_pct,     # the MOVE — the confound to control for
            "dollar_vol":     dollar_vol,
            "title":          title,
            "url":            url,
            "hold_days":      "1–3",
            "exit_rule":      exit_rule,
            "source":         source,
            "commodity":      commodity,
            "cluster_capped": cluster_capped,
            "cluster_note":   cluster_note,
        })

    return signals, exit_advisories


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
  Exit     : {exit_rule}{(chr(10) + '  Cluster  : ' + r['cluster_note']) if r.get('cluster_note') else ''}
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
# Universe label validation (symbol-recycling guard)
# match_ticker() pairs press releases to symbols BY COMPANY NAME. TSXV recycles
# delisted symbols onto new companies, so a label can silently drift off its
# symbol (e.g. AGX.V was "Argo Gold" but the symbol now belongs to Silver X
# Mining). A wrong label means the watcher listens for the wrong company → never
# matches the real one, and can stamp an unrelated PR onto the wrong symbol.
# This guard cross-checks every label against yfinance longName every week.
# ══════════════════════════════════════════════════════════════════════════════

_CORP_SUFFIXES = {
    "corp", "corporation", "inc", "incorporated", "ltd", "limited", "co",
    "company", "plc", "sa", "nl", "the",
}

def _normalize_co_name(name: str) -> set[str]:
    """Lowercase, strip punctuation + corporate suffixes → significant token set."""
    toks = re.sub(r"[^a-z0-9 ]", " ", name.lower()).split()
    return {t for t in toks if t and t not in _CORP_SUFFIXES}


def _labels_match(label: str, yf_name: str) -> bool:
    """True if our label plausibly refers to the same company as yfinance longName."""
    a, b = _normalize_co_name(label), _normalize_co_name(yf_name)
    if not a or not b:
        return False
    if a <= b or b <= a:           # one token set contained in the other
        return True
    overlap = len(a & b) / min(len(a), len(b))
    return overlap >= 0.5


def validate_universe(verbose: bool = False) -> list[str]:
    """
    Cross-check every TICKERS label against its live yfinance longName.
    Returns a list of human-readable mismatch warnings (empty = all clean).
    Alerts via Telegram if any drift is detected. Run weekly with discovery.
    """
    print("🔎 Validating universe labels against yfinance longName...")
    mismatches:  list[str] = []   # REAL drift — got a name, it doesn't match
    unverified:  list[str] = []   # no data after retries — almost always throttling, NOT delisting

    for sym, label in COMPANY_NAMES.items():
        info = _yf_info_retry(sym)           # retries + backoff; None = couldn't verify
        if info is None:
            unverified.append(sym)
            if verbose:
                print(f"  {sym}: unverified (no data after retries — likely throttling)")
            time.sleep(0.4)
            continue

        yf_name = info.get("longName") or info.get("shortName")
        if not _labels_match(label, yf_name):
            mismatches.append(f"❌ {sym}: labeled '{label}' but yfinance says '{yf_name}'")
        elif verbose:
            print(f"  {sym}: OK ('{label}' ≈ '{yf_name}')")
        time.sleep(0.4)                       # space the calls so we don't trip 429 mid-loop

    # Only REAL name mismatches fire the 🚨 alert. "Unverified" is treated as a
    # transient (throttle) condition and NEVER alarmed — a genuinely delisted symbol
    # also surfaces via get_price_data returning None during trading, so we don't
    # risk crying wolf and prompting a bad manual ticker removal.
    if mismatches:
        body = "\n".join(_tg_escape(m) for m in mismatches)
        msg = (
            "🚨 *Universe label drift detected* — symbols may have recycled.\n"
            f"{body}\n\n"
            "_Fix COMPANY\\_NAMES/TICKERS in watcher.py before trusting these signals._"
        )
        send_telegram(msg)
        print("\n".join(mismatches))
    else:
        print("  All labels verified clean.")

    if unverified:
        # Soft, non-alarming note — surfaced only if a LARGE share is unverified
        # (which would mean the validation pass itself was throttled and unreliable).
        note = f"  {len(unverified)}/{len(COMPANY_NAMES)} unverified this run (likely throttling): {', '.join(unverified)}"
        print(note)
        if len(unverified) > len(COMPANY_NAMES) // 2:
            send_telegram(
                f"ℹ️ Universe check: {len(unverified)}/{len(COMPANY_NAMES)} tickers "
                f"couldn't be verified this run (Yahoo throttling) — guard ran but was "
                f"incomplete. No action needed; will re-check next Saturday."
            )

    return mismatches


# ══════════════════════════════════════════════════════════════════════════════
# Weekly ticker discovery
# Scans GlobeNewswire for active TSX/TSXV companies not yet in our universe.
# Run with: python watcher.py --discover
# Or via the Saturday GitHub Actions cron.
# ══════════════════════════════════════════════════════════════════════════════

# ── Autonomous screener helpers ─────────────────────────────────────────────────
_SEARCH_HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0", "Accept": "application/json"}


def _resolve_symbol(name: str) -> dict | None:
    """
    Reverse-resolve a company name → TSX/TSXV symbol via Yahoo's search endpoint.
    Returns {symbol, longname} for the best TSX(.TO)/TSXV(.V) match whose name
    actually agrees with the query (reuses _labels_match — the recycling guard),
    else None. Retries with backoff on Yahoo's 429 throttling.
    """
    quotes = None
    for attempt in range(3):
        try:
            r = requests.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": name, "quotesCount": 10, "newsCount": 0},
                headers=_SEARCH_HEADERS, timeout=10,
            )
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            quotes = r.json().get("quotes", [])
            break
        except Exception:
            time.sleep(2 ** attempt)
    if quotes is None:
        return None
    for q in quotes:
        sym  = q.get("symbol", "")
        exch = q.get("exchange", "")
        ln   = q.get("longname") or q.get("shortname") or ""
        if (exch in ("TOR", "VAN") or sym.endswith((".TO", ".V"))) and _labels_match(name, ln):
            return {"symbol": sym, "longname": ln}
    return None


def _screen_symbol(symbol: str) -> dict | None:
    """
    Pull yfinance fundamentals and apply the deterministic AUTO-ADD gate.
    Returns metrics + per-criterion verdict + overall `pass`, or None if unfetchable.
    """
    info = _yf_info_retry(symbol)   # retry + backoff; None = couldn't verify
    if not info:
        return None

    longname = info.get("longName") or info.get("shortName") or ""
    cap      = info.get("marketCap")
    price    = info.get("currentPrice") or info.get("regularMarketPrice")
    sect     = ((info.get("sector") or "") + " " + (info.get("industry") or "")).lower()

    # Median + 25th-pct daily $-vol over ~1mo, with retry/backoff so a transient 429
    # doesn't zero out liquidity and wrongly reject (or — worse — pass) a candidate.
    dollar_vol = 0.0
    q25_vol    = 0.0
    delay = 0.8
    for attempt in range(_PRICE_RETRIES):
        try:
            hist = yf.Ticker(symbol).history(period="1mo", interval="1d", auto_adjust=True)
            if hist is not None and not hist.empty:
                daily = (hist["Close"] * hist["Volume"]).dropna()
                if len(daily):
                    dollar_vol = float(daily.median())
                    q25_vol    = float(daily.quantile(0.25))
                break
        except Exception:
            pass
        if attempt < _PRICE_RETRIES - 1:
            time.sleep(delay)
            delay *= 3
    # Liquidity unknown after retries → fail safe: cannot confirm ≥$100k, so reject
    # (never auto-add a name whose liquidity we couldn't actually measure).
    if dollar_vol == 0.0:
        return None

    is_energy = any(k in sect for k in ("oil", "gas", "energy"))
    is_mining = any(k in sect for k in ("mining", "metal", "gold", "copper", "silver", "coal"))
    if   any(k in sect for k in ("oil", "gas", "energy")): commodity = "oil"
    elif "copper" in sect:                                 commodity = "copper"
    elif "silver" in sect:                                 commodity = "silver"
    else:                                                  commodity = "gold"

    checks = {
        "cap<300M":   bool(cap)   and cap < MAX_DISCOVERY_CAP,
        "$vol>=100k": dollar_vol >= AUTO_ADD_MIN_DOLLAR_VOL,
        "consistent": q25_vol    >= AUTO_ADD_MIN_Q25_VOL,   # spike-guard: not a 1-week financing pop
        "px>=0.25":   bool(price) and price >= AUTO_ADD_MIN_PRICE,
        "min/energy": is_energy or is_mining,
        "name_ok":    bool(longname),
    }
    return {
        "symbol":     symbol,
        "longname":   longname,
        "market_cap": cap,
        "price":      price,
        "dollar_vol": round(dollar_vol, 0),
        "q25_vol":    round(q25_vol, 0),
        "sector":     "Energy" if is_energy else "Mining",
        "commodity":  commodity,
        "summary":    (info.get("longBusinessSummary") or "")[:1500],
        "industry":   info.get("industry") or "",
        "checks":     checks,
        "pass":       all(checks.values()),
    }


def _harvest_candidate_titles(verbose: bool = False) -> list[tuple[str, str, str]]:
    """
    Collect (title, summary, url) from the SAME news universe the live watcher matches
    against — all GlobeNewswire feeds + Newsfile category pages (raw titles, not the
    ticker-filtered scrape) — so discovery can see new untracked names, not just the
    one feed it used before.
    """
    out: list[tuple[str, str, str]] = []
    for feed_url in PRESS_RELEASE_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for e in feed.entries:
                out.append((e.get("title", ""), e.get("summary", "") or "", e.get("link", "")))
        except Exception as ex:
            if verbose: print(f"  harvest feed error: {ex}")
    # Newsfile category pages — raw release titles (un-slugged), NO ticker filter
    for cat_url in NEWSFILE_CATEGORIES:
        try:
            r = requests.get(cat_url, headers=FETCH_HEADERS, timeout=12)
            if r.status_code == 200:
                for rid, slug in dict.fromkeys(re.findall(r'/release/(\d+)/([^"\s\'<>&?]+)', r.text)):
                    title = re.sub(r'-+', ' ', slug).strip()
                    out.append((title, "", f"{NEWSFILE_BASE}/release/{rid}/{slug}"))
            time.sleep(0.5)
        except Exception as ex:
            if verbose: print(f"  harvest newsfile error: {ex}")
    return out


def _ai_operator_veto(scr: dict) -> tuple[bool, str]:
    """
    Front-door AI veto: is this a GENUINE event-driven explorer/producer, or
    royalty / streaming / holding / investment / services / shell slop?
    Returns (approve, reason). FAIL-CLOSED — any error/uncertainty → (False, ...)
    so an unvetted name is never auto-added. Reject-on-doubt by instruction.
    """
    client = _get_anthropic_client()
    if client is None:
        return (False, "AI veto unavailable (no API key) — fail-closed")
    prompt = (
        "You are the final gate before a ticker is auto-added to a TSX/TSXV small-cap "
        "EVENT-DRIVEN trading watchlist. The watchlist only works on companies whose own "
        "press releases move the stock: active drill/assay programs, mine production, or "
        "quarterly oil & gas results.\n\n"
        "APPROVE only if you are confident this is a genuine mineral EXPLORER/PRODUCER or "
        "oil & gas E&P company with its own operational catalysts.\n"
        "REJECT if it is a royalty/streaming company, a holding/investment company, an ETF/fund, "
        "an equipment/oilfield-SERVICES company, a generalist, or a dormant/shell entity — or if "
        "you are unsure.\n\n"
        f"Symbol: {scr['symbol']}\nName: {scr['longname']}\nIndustry: {scr.get('industry','?')}\n"
        f"Sector (screened): {scr['sector']} / {scr['commodity']}\n"
        f"Business summary: {scr.get('summary','(none)')}\n\n"
        'Return ONLY JSON: {"approve": true/false, "reason": "one short sentence"}'
    )
    try:
        resp = client.messages.create(
            model=DISCOVERY_AI_MODEL, max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = re.sub(r"```(?:json)?\n?", "", resp.content[0].text.strip()).strip("`").strip()
        v = json.loads(text)
        return (bool(v.get("approve")), str(v.get("reason", ""))[:120])
    except Exception as e:
        return (False, f"AI veto error ({type(e).__name__}) — fail-closed")


def _last_trigger_date(ticker: str) -> datetime | None:
    """Most recent date this ticker fired a BUY/STRONG BUY/CAUTION signal (a 'trigger').
    SKIP and silence do NOT count. Parsed from signals.log. None if never."""
    if not LOG_FILE.exists():
        return None
    latest = None
    keep = {"BUY", "STRONG BUY", "CAUTION"}
    try:
        for line in LOG_FILE.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            if r.get("ticker") != ticker or r.get("signal") not in keep:
                continue
            ts = (r.get("timestamp") or "").replace(" ET", "").strip()
            try:
                dt = datetime.strptime(ts, "%Y-%m-%d %H:%M").replace(tzinfo=EASTERN)
            except Exception:
                continue
            if latest is None or dt > latest:
                latest = dt
    except Exception:
        return None
    return latest


def _recent_pr_for(ticker: str) -> str:
    """Most recent logged press-release headline for a ticker (for the removal gate)."""
    if not PR_LOG_DIR.exists():
        return ""
    tag = ticker.replace(".", "")
    files = sorted([p for p in PR_LOG_DIR.glob(f"*_{tag}_*.json")], reverse=True)
    for p in files[:1]:
        try:
            return json.loads(p.read_text()).get("title", "")[:160]
        except Exception:
            pass
    return ""


def _ai_removal_gate(symbol: str, name: str, days_quiet: int) -> tuple[bool, str]:
    """
    Decide whether a 2-month-quiet auto-added ticker should be permanently removed.
    Returns (remove, reason). FAIL-SAFE — any error/uncertainty → (False, ...) i.e.
    KEEP, because removal is the destructive action and a blip shouldn't delete a name.
    """
    client = _get_anthropic_client()
    last_pr = _recent_pr_for(symbol)
    info    = _yf_info_retry(symbol) or {}
    summary = (info.get("longBusinessSummary") or "")[:1200]
    if client is None:
        return (False, "AI gate unavailable — keep (fail-safe)")
    prompt = (
        f"An auto-added TSX/TSXV ticker has produced NO tradeable signal for {days_quiet} days. "
        "Decide whether to permanently remove it from the watchlist, or keep it because a "
        "catalyst looks imminent.\n\n"
        "REMOVE if it looks genuinely dormant/dead, acquired, or perpetually inactive.\n"
        "KEEP if its profile or last news suggests a near-term binary catalyst (pending drill "
        "results, maiden resource/PEA, permit decision, restart) — or if you are unsure.\n\n"
        f"Symbol: {symbol}\nName: {name}\nDays quiet: {days_quiet}\n"
        f"Last press release: {last_pr or '(none on record)'}\n"
        f"Business summary: {summary or '(none)'}\n\n"
        'Return ONLY JSON: {"remove": true/false, "reason": "one short sentence"}'
    )
    try:
        resp = client.messages.create(
            model=DISCOVERY_AI_MODEL, max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = re.sub(r"```(?:json)?\n?", "", resp.content[0].text.strip()).strip("`").strip()
        v = json.loads(text)
        return (bool(v.get("remove")), str(v.get("reason", ""))[:120])
    except Exception as e:
        return (False, f"AI gate error ({type(e).__name__}) — keep (fail-safe)")


def run_ticker_lifecycle(verbose: bool = False) -> None:
    """
    Monthly recycle/removal pass over AUTO-ADDED tickers only (the hand-curated core
    is never touched). Status is DERIVED from each ticker's last trigger date:
      • <30d quiet  → Active
      • 30–60d quiet → Recycled (still watched, on the deletion clock)
      • ≥60d quiet  → AI-gated removal (delete from ledger unless a catalyst looks imminent)
    A new trigger automatically resets the clock (re-promotion is implicit — no flag to flip).
    """
    print("♻️  Running monthly ticker lifecycle (auto-added only)...")
    ledger = _load_auto_added_file()
    if not ledger:
        print("  No auto-added tickers — nothing to recycle.")
        return

    now = datetime.now(EASTERN)
    recycled, removed, spared = [], [], []
    changed = False

    for sym, meta in list(ledger.items()):
        last = _last_trigger_date(sym)
        if last is None:
            try:
                last = datetime.strptime(meta.get("added_on", ""), "%Y-%m-%d").replace(tzinfo=EASTERN)
            except Exception:
                last = now
        days_quiet = (now - last).days

        if days_quiet >= REMOVE_QUIET_DAYS:
            remove, reason = _ai_removal_gate(sym, meta.get("name", sym), days_quiet)
            if remove:
                del ledger[sym]
                changed = True
                removed.append((sym, meta.get("name", sym), reason))
                print(f"  🗑️  REMOVE {sym} ({days_quiet}d quiet): {reason}")
            else:
                spared.append((sym, meta.get("name", sym), reason))
                print(f"  ⏸️  KEEP {sym} ({days_quiet}d quiet) — AI spared: {reason}")
        elif days_quiet >= RECYCLE_QUIET_DAYS:
            recycled.append((sym, meta.get("name", sym), days_quiet))
            print(f"  ♻️  RECYCLED {sym} ({days_quiet}d quiet) — on the clock")
        elif verbose:
            print(f"  ✓ {sym} active ({days_quiet}d since last trigger)")

    if changed:
        _save_auto_added_file(ledger)

    if not (recycled or removed or spared):
        print("  All auto-added tickers active — none recycled.")
        return

    lines = ["♻️ *Monthly Ticker Lifecycle*"]
    if removed:
        lines.append(f"\n🗑️ *Removed ({len(removed)})* — 2 months no tradeable signal, AI confirmed dormant:")
        for s, n, r in removed:
            lines.append(f"  • {_tg_escape(s)} {_tg_escape(n)} — {_tg_escape(r)}")
    if spared:
        lines.append(f"\n⏸️ *Kept ({len(spared)})* — quiet but AI sees an imminent catalyst:")
        for s, n, r in spared:
            lines.append(f"  • {_tg_escape(s)} {_tg_escape(n)} — {_tg_escape(r)}")
    if recycled:
        lines.append(f"\n♻️ *Recycled ({len(recycled)})* — 1 quiet month, still watched; removed next month if no trigger:")
        for s, n, d in recycled:
            lines.append(f"  • {_tg_escape(s)} {_tg_escape(n)} ({d}d)")
    send_telegram("\n".join(lines))


def export_universe(verbose: bool = False) -> None:
    """
    Dump the full live universe (core + auto-added) with each ticker's lifecycle
    status to universe.json, so the dashboard is fully data-driven — manual edits,
    auto-adds, AND recycle/on-the-clock status all reflect with no hardcoding.

    Status (lifecycle applies to AUTO-ADDED only; the hand-curated core is exempt):
      active · recycled (30d+ quiet, on the deletion clock) · pending-removal (60d+).
    """
    # One pass over signals.log → last non-SKIP trigger date per ticker.
    last_trig: dict[str, datetime] = {}
    keep = {"BUY", "STRONG BUY", "CAUTION"}
    if LOG_FILE.exists():
        for line in LOG_FILE.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                r = json.loads(line)
            except Exception:
                continue
            t = r.get("ticker")
            if not t or t == "SECTOR" or r.get("signal") not in keep:
                continue
            try:
                dt = datetime.strptime((r.get("timestamp") or "").replace(" ET", "").strip(),
                                       "%Y-%m-%d %H:%M").replace(tzinfo=EASTERN)
            except Exception:
                continue
            if t not in last_trig or dt > last_trig[t]:
                last_trig[t] = dt

    auto = _load_auto_added_file()
    now  = datetime.now(EASTERN)
    out  = []
    for sym in TICKERS:
        is_auto = sym in auto
        lt = last_trig.get(sym)
        if lt is not None:
            dq = (now - lt).days
        elif is_auto:
            try:
                dq = (now - datetime.strptime(auto[sym].get("added_on", ""), "%Y-%m-%d").replace(tzinfo=EASTERN)).days
            except Exception:
                dq = None
        else:
            dq = None   # core, never triggered → unknown, treat as active
        # status — only auto-added names are subject to recycle/removal
        if not is_auto:
            status = "active"
        elif dq is None or dq < RECYCLE_QUIET_DAYS:
            status = "active"
        elif dq >= REMOVE_QUIET_DAYS:
            status = "pending-removal"
        else:
            status = "recycled"
        out.append({
            "symbol":     sym,
            "sector":     get_sector(sym),
            "commodity":  get_commodity(sym),
            "source":     "auto" if is_auto else "core",
            "status":     status,
            "days_quiet": dq,
            "name":       COMPANY_NAMES.get(sym, sym),
            "added_on":   auto.get(sym, {}).get("added_on") if is_auto else None,
        })

    UNIVERSE_FILE.parent.mkdir(parents=True, exist_ok=True)
    UNIVERSE_FILE.write_text(json.dumps({"generated": now.strftime("%Y-%m-%d %H:%M ET"),
                                         "count": len(out), "tickers": out}, indent=2))
    if verbose:
        rec = sum(1 for t in out if t["status"] != "active")
        print(f"  Universe exported: {len(out)} tickers ({rec} recycled/pending).")


def discover_new_tickers(verbose: bool = False) -> None:
    """
    Autonomous weekly discovery + AUTO-ADD. Harvests active TSX/TSXV mining/energy
    names from all news feeds + Newsfile, resolves each to a real symbol, screens it
    against the deterministic gate (cap <$300M, ≥$100k/day median, 25th-pct ≥$30k,
    ≥$0.25, mining/energy, verified longName) PLUS an AI operator-veto, and AUTO-ADDS
    every qualifier to auto_added_tickers.json (merged into the universe at next load).
    On the first Saturday of each month it also runs the recycle/removal lifecycle.
    Reports exactly what was added — with per-criterion confirmation — to Telegram.
    """
    print("🔍 Running weekly ticker discovery scan...")

    # Symbol-recycling guard: verify existing labels before scanning for new ones.
    # Catches the AGX.V-class problem (label drifted off a recycled symbol) weekly.
    try:
        validate_universe(verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"  Universe validation error: {e}")

    # First Saturday of the month → run the recycle/removal lifecycle on auto-adds.
    if datetime.now(EASTERN).day <= 7:
        try:
            run_ticker_lifecycle(verbose=verbose)
        except Exception as e:
            if verbose:
                print(f"  Lifecycle error: {e}")

    known_names_lower = {name.lower() for name in COMPANY_NAMES.values()}
    candidates: dict[str, dict] = {}   # company_key → {count, titles, url}

    for title, summary, url in _harvest_candidate_titles(verbose=verbose):
        text = (title + " " + summary).lower()

        # Must be TSX/TSXV
        if not any(kw in text for kw in ["tsx", "tsxv", "tsx venture", "tsx:", "tsxv:"]):
            continue
        # Must be mining or energy
        if not any(kw in text for kw in [
            "mining", "gold", "copper", "silver", "zinc", "nickel", "lithium",
            "oil", "gas", "energy", "drill", "exploration", "resource", "mineral",
        ]):
            continue
        # Skip if already tracked
        if match_ticker(title, summary):
            continue

        # Extract company name: everything before action verbs
        company_key = re.split(
            r'\b(?:reports|announces|completes|updates|releases|files|closes|enters|signs|grants|declares)\b',
            title, flags=re.I
        )[0].strip()[:50]

        if not company_key or company_key.lower() in known_names_lower:
            continue

        if company_key not in candidates:
            candidates[company_key] = {"count": 0, "titles": [], "url": url}
        candidates[company_key]["count"] += 1
        if len(candidates[company_key]["titles"]) < 2:
            candidates[company_key]["titles"].append(title[:60])

    if not candidates:
        msg = "🔍 *Weekly Discovery*: No new candidates found this scan."
        print(msg.replace("*", ""))
        send_telegram(msg)
        return

    if verbose:
        print(f"  {len(candidates)} raw candidates found")

    # ── Resolve → screen → auto-add ──────────────────────────────────────────
    auto_added = _load_auto_added_file()
    known      = {s.upper() for s in TICKERS}        # includes prior auto-adds (merged at load)
    today_str  = datetime.now(EASTERN).strftime("%Y-%m-%d")
    by_activity = sorted(candidates.items(), key=lambda kv: kv[1]["count"], reverse=True)

    added:    list[dict] = []
    rejected: int = 0

    vetoed: int = 0
    for name, data in by_activity[:MAX_DISCOVERY_EVAL]:
        time.sleep(0.5)                              # Yahoo politeness
        res = _resolve_symbol(name)
        if not res:
            if verbose: print(f"  ↷ {name}: no TSX/TSXV symbol resolved")
            continue
        sym = res["symbol"].upper()
        if sym in known or sym in auto_added:
            continue
        scr = _screen_symbol(sym)
        if not scr:
            if verbose: print(f"  ↷ {name} ({sym}): screen fetch failed")
            continue
        if not scr["pass"]:
            rejected += 1
            if verbose:
                fails = [k for k, v in scr["checks"].items() if not v]
                print(f"  ↷ {sym} {scr['longname']}: failed {fails}")
            continue

        # Numeric gate passed → AI operator-veto (fail-closed: error/doubt = reject)
        approve, reason = _ai_operator_veto(scr)
        if not approve:
            vetoed += 1
            print(f"  ⃠ {sym} {scr['longname']}: AI veto — {reason}")
            continue

        # Passed every criterion AND the AI veto → auto-add (data, merged next load)
        auto_added[sym] = {
            "name":       _match_name(scr["longname"]),   # suffix-stripped → headline matching works
            "longname":   scr["longname"],                # full legal name, for display/reference
            "sector":     scr["sector"],
            "commodity":  scr["commodity"],
            "market_cap": scr["market_cap"],
            "dollar_vol": scr["dollar_vol"],
            "price":      scr["price"],
            "added_on":   today_str,
            "source":     "auto-discovery",
            "ai_reason":  reason,
        }
        scr["ai_reason"] = reason
        added.append(scr)
        print(f"  ✅ AUTO-ADD {sym} {scr['longname']} — cap ${scr['market_cap']:,} "
              f"$vol ${scr['dollar_vol']:,.0f} px ${scr['price']} | AI: {reason}")

    if added:
        _save_auto_added_file(auto_added)

    # ── Telegram report — exactly what was added + criteria confirmation ──────
    total = len(TICKERS) + len(added)   # TICKERS already holds prior auto-adds
    if not added:
        msg = (f"🤖 *Weekly Auto-Add* — 0 new tickers.\n"
               f"Scanned {len(candidates)} active names; {rejected} failed the numeric "
               f"screen, {vetoed} failed the AI operator-veto.\n"
               f"Universe unchanged at *{len(TICKERS)}* tickers.")
        send_telegram(msg)
        print(msg.replace("*", "").replace("\\", ""))
        return

    lines = [f"🤖 *Weekly Auto-Add* — {len(added)} ticker(s) added ✅"]
    for p in added:
        cap_m = f"${p['market_cap']/1e6:.0f}M" if p["market_cap"] else "?"
        lines.append(
            f"\n*{_tg_escape(p['symbol'])}* — {_tg_escape(p['longname'])}\n"
            f"  ✓ cap {cap_m} < \\$300M\n"
            f"  ✓ \\${p['dollar_vol']/1e3:.0f}k/day median ≥ \\$100k\n"
            f"  ✓ \\${p['q25_vol']/1e3:.0f}k/day 25th-pct ≥ \\$30k (not a spike)\n"
            f"  ✓ px \\${p['price']} ≥ \\$0.25\n"
            f"  ✓ {p['sector']} ({p['commodity']})\n"
            f"  ✓ name verified vs yfinance\n"
            f"  ✓ AI operator-veto: {_tg_escape(p.get('ai_reason',''))}"
        )
    lines.append(f"\n_All criteria met. Universe now *{total}* tickers — live next run._")
    tail = []
    if rejected: tail.append(f"{rejected} failed numeric screen")
    if vetoed:   tail.append(f"{vetoed} AI-vetoed")
    if tail:
        lines.append(f"_({', '.join(tail)}.)_")

    msg = "\n".join(lines)
    send_telegram(msg)
    print(msg.replace("*", "").replace("_", "").replace("\\", ""))


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
    p.add_argument("--discover",  action="store_true", help="Run weekly ticker discovery scan and exit")
    p.add_argument("--validate",  action="store_true", help="Cross-check universe labels vs yfinance longName and exit")
    p.add_argument("--lifecycle", action="store_true", help="Run the recycle/removal lifecycle on auto-added tickers and exit")
    p.add_argument("--outcomes",  action="store_true", help="Backfill D+1/D+3 forward outcomes for logged signals and exit")
    p.add_argument("--paper",     action="store_true", help="Run the autonomous paper trader (simulated) and exit")
    p.add_argument("--universe",  action="store_true", help="Export the live universe + lifecycle status to universe.json and exit")
    p.add_argument("--until",     type=str,            help="Clean self-exit at HH:MM ET (two-run handoff, no close summary)")
    args = p.parse_args()

    # ── Two-run split handoff time (HH:MM ET) ────────────────────────────────
    # On GitHub Actions the day is split into two runs to stay under GitHub's hard
    # 6h job cap. Two Make.com scenarios drive this:
    #   • Run A (~6:55 AM ET) passes --until 12:30 → self-exits at 12:30 (handoff)
    #   • Run B (~12:30 PM ET) passes no --until   → runs through to the 4 PM close
    # The elif below is a CI-only safety net: if Run A's dispatch ever drops the
    # --until input, a weekday morning start still hands off at 12:30 so it can't
    # die at the 6h cap. On a long-running host (VM/VPS) GITHUB_ACTIONS is unset,
    # so neither path triggers and the watcher runs the full session in one go.
    until_t = None
    if args.until:
        try:
            hh, mm = args.until.split(":")
            until_t = (int(hh), int(mm))
        except Exception:
            print(f"Invalid --until '{args.until}', expected HH:MM. Ignoring.")
            until_t = None
    elif not args.all_hours and os.environ.get("GITHUB_ACTIONS") == "true":
        # Safety net — GitHub Actions ONLY: if the morning CI run ever starts
        # without --until (e.g. Make drops the input), still hand off at 12:30 so
        # it can't die at GitHub's 6h job cap. Gated on $GITHUB_ACTIONS so a
        # long-running host (Oracle VM, VPS) runs the full session in one process
        # and never self-exits mid-day. Threshold 11:00 < the 12:30 afternoon
        # start, so the afternoon CI run is never misclassified.
        _now_et = datetime.now(EASTERN)
        if _now_et.weekday() < 5 and _now_et.hour < 11:
            until_t = (12, 30)   # morning CI start → hand off to the afternoon run

    if args.url:
        run_manual_url(args.url)
        return

    if args.validate:
        validate_universe(verbose=True)
        return

    if args.lifecycle:
        run_ticker_lifecycle(verbose=True)
        return

    if args.outcomes:
        log_outcomes(verbose=True)
        return

    if args.paper:
        run_paper_trader(verbose=True)
        return

    if args.universe:
        export_universe(verbose=True)
        return

    if args.discover:
        discover_new_tickers(verbose=args.verbose)
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
    try:
        expire_positions()   # clean up positions older than D+3
    except Exception as e:
        print(f"  Position expiry skipped: {e}")
    try:
        log_outcomes()   # backfill D+1/D+3 forward returns for past signals (idempotent, daily)
    except Exception as e:
        print(f"  Outcome backfill skipped: {e}")
    try:
        run_paper_trader()   # autonomous simulated trading on the signals (idempotent, daily)
    except Exception as e:
        print(f"  Paper trader skipped: {e}")
    try:
        export_universe()    # refresh universe.json (lifecycle status) for the dashboard
    except Exception as e:
        print(f"  Universe export skipped: {e}")

    print(f"TSX/TSXV Small-Cap Watcher  —  {datetime.now(EASTERN).strftime('%Y-%m-%d %H:%M ET')}")
    print(f"Watching {len(TICKERS)} small-cap tickers (<$300M market cap)")
    print(f"Feeds    : {len(PRESS_RELEASE_FEEDS)} press release + BNN Market Call")
    print(f"Interval : every {POLL_SECS // 60} min during market hours")
    print(f"Hours    : 7:00–9:30 ET pre-market scan + 9:30–16:00 ET live signals, Mon–Fri")
    print(f"Log      : {LOG_FILE}")
    print(f"Telegram : {'configured ✓' if _load_tg_config() else 'not configured (run setup_telegram.py)'}")
    print(f"AI       : {'Claude claude-haiku-4-5 ✓ (prompt-cached)' if _get_anthropic_client() else 'not configured — using keyword scoring (set ANTHROPIC_API_KEY)'}")
    print(f"Hold     : 1–3 days (NOT intraday)\n")
    print(f"Strategy : Small caps, during-market releases, speed wins.\n")

    signals_today       = 0
    # If we start mid-session (already past 9:30, not pre-market), this is a
    # Run B handoff continuation — the morning run (Run A) already sent the
    # "market open" alert. Suppress the duplicate so the afternoon start is quiet.
    _start_now = datetime.now(EASTERN)
    market_open_notified    = is_market_hours(_start_now) and not is_premarket(_start_now)
    premarket_open_notified = False

    if until_t:
        print(f"Handoff mode: clean self-exit at {until_t[0]:02d}:{until_t[1]:02d} ET (two-run split)\n")

    try:
        while True:
            now = datetime.now(EASTERN)

            # ── Weekend defensive exit ────────────────────────────────────────
            # Make/cron should never fire a non-discovery run on a weekend, but if
            # one does, don't idle-sleep until the 6h cap — exit immediately.
            if not args.all_hours and now.weekday() >= 5:
                print(f"\nWeekend ({now.strftime('%a %H:%M ET')}). Nothing to watch. Exiting.")
                break

            # ── Two-run handoff: clean exit at --until, no close summary ───────
            # Run A exits here (~12:30) so it stays under GitHub's 6h job cap;
            # Run B takes over and sends the single end-of-day close summary.
            if until_t and (now.hour, now.minute) >= until_t and now.hour < 16:
                print(f"\nHandoff time {until_t[0]:02d}:{until_t[1]:02d} ET reached. Exiting for Run B takeover.")
                save_seen(seen)
                break

            # ── After market close → exit ─────────────────────────────────────
            if not args.all_hours and now.weekday() < 5 and now.hour >= 16:
                print(f"\nMarket closed ({now.strftime('%H:%M ET')}). Watcher exiting.")
                tg_market_close(signals_today)
                break

            # ── Before pre-market window → sleep ─────────────────────────────
            # Weekends already exited above, so this only fires on a weekday
            # before the 7:00 AM pre-market window opens.
            if not args.all_hours and not is_premarket(now) and not is_market_hours(now):
                print(f"  [{now.strftime('%H:%M')}] Outside active hours (before 7:00 AM). Sleeping...", end="\r")
                time.sleep(POLL_SECS)
                continue

            # ── Pre-market window (7:00–9:30 ET) ─────────────────────────────
            in_premarket = is_premarket(now)
            if in_premarket:
                if not premarket_open_notified:
                    tg_premarket_open(len(TICKERS))
                    premarket_open_notified = True
                print(f"  [{now.strftime('%H:%M')}] Pre-market scan...", end=" ", flush=True)

            # ── Market hours (9:30–16:00 ET) ──────────────────────────────────
            else:
                if not market_open_notified:
                    tg_market_open(len(TICKERS))
                    market_open_notified = True
                print(f"  [{now.strftime('%H:%M')}] Polling...", end=" ", flush=True)

            # Halt detection is now lazy (per-matched-candidate, inside the signal
            # pipeline via is_halted/get_price_data) — no bulk pre-poll refresh.

            pr_signals, exit_advisories = poll_press_releases(
                seen, verbose=args.verbose, premarket=in_premarket
            )
            # BNN only during market hours — no pre-market podcast signals
            bnn_signals = [] if in_premarket else check_bnn_feed(seen, verbose=args.verbose)
            signals     = pr_signals + bnn_signals
            save_seen(seen)

            # Handle exit advisories first (market hours only — need live price)
            if not in_premarket:
                for adv in exit_advisories:
                    tg_exit_advisory(adv["position"], adv["exit_result"], adv["new_title"], adv["url"])
                    action = adv["exit_result"]["action"]
                    conf   = adv["exit_result"]["confidence"]
                    print(f"  📊 EXIT ADVISORY — {adv['position']['ticker']}: {action} ({conf:.0%}) — {adv['exit_result'].get('reasoning','')[:60]}")

            if signals:
                print(f"{len(signals)} signal(s)!")
                for r in signals:
                    print_signal(r)
                    fire_notification(r)
                    if in_premarket:
                        tg_premarket_signal(r)   # different Telegram format, no position tracking
                        r["premarket"] = True
                        log_signal(r)            # log for the backtest dataset too (no price/position yet)
                    else:
                        tg_signal(r)
                        r["premarket"] = False
                        log_signal(r)
                        add_position(r)          # track for exit intelligence
                    signals_today += 1
            else:
                print("no new signals.")

            time.sleep(POLL_SECS)

    except KeyboardInterrupt:
        print("\nWatcher stopped.")
        save_seen(seen)


if __name__ == "__main__":
    main()
