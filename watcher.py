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
from datetime import datetime
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
    "PRQ.TO",    # Perpetual Energy       ~$283M
    "BNE.TO",    # Bonterra Energy        ~$228M
    "KEI.TO",    # Kelt Exploration       ~$247M
    "PNE.TO",    # Pine Cliff Energy      ~$235M
    "JOY.TO",    # Journey Energy         ~$335M  (borderline — active quarterly earnings)

    # ── TSXV Energy ──────────────────────────────────────────────────────────
    "HME.V",     # Hemisphere Energy      ~$257M  quarterly earnings, active driller
    "ALV.V",     # Alvopetro Energy       ~$328M  Brazil-focused, quarterly results
    # PCQ.V removed — coal, thin liquidity, poor fit
    # TAO.V removed — renewables micro-cap, wrong catalyst type
    # PUL.V removed — $9M, never clears $50k volume filter

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
    "USCU.V",    # US Copper              ~$25M
    "ABR.V",     # Aberdeen International ~$17M
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
    # TSX Mining
    "GMX.TO":  "Gold Mountain Mining",
    "ORV.TO":  "Orvana Minerals",
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
}

SECTOR_MAP = {
    "PRQ.TO": "Energy", "BNE.TO": "Energy", "KEI.TO": "Energy",
    "PNE.TO": "Energy", "JOY.TO": "Energy", "HME.V":  "Energy",
    "ALV.V":  "Energy",
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
    "BBB.V":  "silver", "MGG.V":  "silver",
}

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
  SAG.V  — Strikepoint Gold. Yukon. High-grade targets but small scale.
  AGX.V  — Argo Gold. Ontario. Small explorer.
  BHS.V  — Bayhorse Silver. Producing silver mine, Oregon. Use silver thresholds.
            Operational news (production #s, shipments) = BUY if above guidance.
  GMX.TO — Gold Mountain Mining. BC. Advanced-stage gold project.
  ORV.TO — Orvana Minerals. Spain (Villalba) + Bolivia gold-copper-silver.
            Reports in USD. Quarterly production + earnings = use energy-style thresholds.
  AHR.V  — American Helium. Helium exploration, Saskatchewan.
            Signal = flow test results (Mcf/d), new well spud, helium % concentration.
            Use energy-style thresholds. High helium % (>1%) = BUY.
  GSP.V  — Gossan Resources. Manitoba. VMS (zinc-copper-gold) deposits.
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
  PRQ.TO — Perpetual Energy. Heavy oil + gas storage, Alberta.
  BNE.TO — Bonterra Energy. Pembina Cardium light oil, Alberta.
  KEI.TO — Kelt Exploration. BC/Alberta conventional oil and gas.
  PNE.TO — Pine Cliff Energy. Natural gas-weighted, Alberta.
  JOY.TO — Journey Energy. Conventional oil, central Alberta.
  ABR.V  — Aberdeen International. Investment company. News rarely actionable.

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
        with open(POSITIONS_FILE) as f:
            return json.load(f)
    return []


def save_positions(positions: list[dict]) -> None:
    POSITIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(POSITIONS_FILE, "w") as f:
        json.dump(positions, f, indent=2)


def add_position(r: dict) -> None:
    """Record a BUY/STRONG BUY signal as an open position for exit monitoring."""
    if r.get("signal") not in ("BUY", "STRONG BUY"):
        return
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
        + (f"\nAI: {reasoning[:120]}" if reasoning else "") +
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

        source = "newsfile" if "newsfilecorp" in url else "press_release"
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
            "price":          price,
            "intraday_pct":   intraday_pct,
            "dollar_vol":     dollar_vol,
            "title":          title,
            "url":            url,
            "hold_days":      "1–3",
            "exit_rule":      exit_rule,
            "source":         source,
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
# Weekly ticker discovery
# Scans GlobeNewswire for active TSX/TSXV companies not yet in our universe.
# Run with: python watcher.py --discover
# Or via the Saturday GitHub Actions cron.
# ══════════════════════════════════════════════════════════════════════════════

def discover_new_tickers(verbose: bool = False) -> None:
    """
    Scan the last batch of GlobeNewswire Canada entries for companies NOT in our
    watchlist that show active news flow (mining/energy, TSX/TSXV, small cap hints).
    Uses Claude to evaluate each candidate and sends a Telegram discovery report.
    """
    print("🔍 Running weekly ticker discovery scan...")

    known_names_lower = {name.lower() for name in COMPANY_NAMES.values()}
    candidates: dict[str, dict] = {}   # company_key → {count, titles, url}

    try:
        feed = feedparser.parse(PRESS_RELEASE_FEEDS[0])
    except Exception as e:
        print(f"  Discovery feed error: {e}")
        return

    for entry in feed.entries:
        title   = entry.get("title", "")
        summary = entry.get("summary", "") or ""
        url     = entry.get("link", "")
        text    = (title + " " + summary).lower()

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

    # Use Claude to evaluate candidates (up to 15)
    client = _get_anthropic_client()
    top = list(candidates.items())[:15]

    if not client:
        # No AI — just report raw candidates
        lines = [f"🔍 *Weekly Discovery* — {len(top)} candidate(s) found (no AI eval)"]
        for name, data in top[:6]:
            lines.append(f"• {_tg_escape(name)}: {data['count']} PR(s)")
        send_telegram("\n".join(lines))
        return

    candidates_text = "\n".join(
        f"- {name}: {'; '.join(data['titles'])}" for name, data in top
    )

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=800,
            messages=[{"role": "user", "content": (
                "You are evaluating TSX/TSXV companies for a small-cap event-driven trading watchlist.\n\n"
                "Strategy criteria: market cap <$300M, active news flow (drills/earnings/deals), "
                "TSX or TSXV listed, mining or energy sector, limited analyst coverage.\n\n"
                "Evaluate these candidates and return ONLY valid JSON:\n"
                '{"recommendations": [{"company": "name", "add": true/false, '
                '"reason": "one sentence", "sector": "Mining|Energy|Other", '
                '"estimated_cap": "micro|small|mid|unknown"}]}\n\n'
                f"Candidates:\n{candidates_text}"
            )}]
        )
        text = response.content[0].text.strip()
        text = re.sub(r"```(?:json)?\n?", "", text).strip("`").strip()
        result = json.loads(text)
        recs = result.get("recommendations", [])

        add_list  = [r for r in recs if r.get("add") and r.get("sector") != "Other"]
        skip_list = [r for r in recs if not r.get("add")]

        lines = [f"🔍 *Weekly Discovery* — {len(add_list)} candidate(s) worth reviewing"]
        for r in add_list[:6]:
            cap = r.get("estimated_cap", "unknown")
            lines.append(
                f"✅ *{_tg_escape(r['company'])}* ({r.get('sector','?')}, {cap} cap): "
                f"{_tg_escape(r.get('reason','')[:80])}"
            )
        if skip_list:
            lines.append(f"\n_{len(skip_list)} evaluated — don't fit criteria_")
        lines.append("\n_Review and add promising names to TICKERS in watcher.py_")

        msg = "\n".join(l for l in lines if l)
        send_telegram(msg)
        print(msg.replace("*", "").replace("_", ""))

    except Exception as e:
        if verbose:
            print(f"  Discovery AI error: {e}")
        lines = [f"🔍 *Weekly Discovery* — {len(top)} candidate(s) found"]
        for name, data in top[:6]:
            lines.append(f"• {_tg_escape(name)}: {data['count']} PR(s)")
        send_telegram("\n".join(lines))


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
    expire_positions()   # clean up positions older than D+3

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
                    else:
                        tg_signal(r)
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
