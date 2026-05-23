"""
backtest_v3.py — Keyword scoring validation (press release backtest)

The 1929-event backtest_v2 measured pure price/volume momentum because
yfinance news doesn't go back 5 years — so 0/1929 events had a press
release fetched. The keyword scoring (BUY/CAUTION/SKIP) was never tested.

This script validates the keyword engine two ways:

  MODE 1 — RECENT (default):
    Pull the last N months of catalyst days from yfinance (news works for ~3m).
    Fetch actual press releases. Score them. Measure BUY vs CAUTION vs SKIP.

  MODE 2 — LIVE LOG:
    Read press releases saved by watcher.py → data/signals/press_releases/
    Match to price data. Report keyword score vs actual D+1/D+3 returns.
    Use this mode after running watcher.py for a few weeks.

Usage:
    python3 backtest_v3.py                        # recent events (default 90d)
    python3 backtest_v3.py --days 180             # last 6 months
    python3 backtest_v3.py --mode live            # use watcher.py's logged PRs
    python3 backtest_v3.py --ticker KEI.TO        # single ticker
    python3 backtest_v3.py --verbose
"""
from __future__ import annotations

import argparse, json, re, time, warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

OUT_DIR = Path("data/backtest")
PR_LOG_DIR = Path("data/signals/press_releases")

# ── Ticker universe (matches watcher.py — GLD.V and GSP.V removed) ──────────
TICKERS = [
    "PRQ.TO","BNE.TO","KEI.TO","PNE.TO","JOY.TO",
    "HME.V","ALV.V","PCQ.V","TAO.V","PUL.V",
    "GMX.TO","ORV.TO","RVX.TO",
    "SAG.V","AHR.V","AGX.V","AZM.V","BHS.V",
    "KDK.V","SURG.V","CUU.V",
    "USCU.V","ABR.V","MCS.V","AFM.V",
]

COMPANY_NAMES = {
    "PRQ.TO":"Perpetual Energy","BNE.TO":"Bonterra Energy","KEI.TO":"Kelt Exploration",
    "PNE.TO":"Pine Cliff Energy","JOY.TO":"Journey Energy","HME.V":"Hemisphere Energy",
    "ALV.V":"Alvopetro Energy","PCQ.V":"Pacific Coal Resources","TAO.V":"Tidewater Renewables",
    "PUL.V":"Pulse Oil","GMX.TO":"Gold Mountain Mining","ORV.TO":"Orvana Minerals",
    "RVX.TO":"Resverlogix","SAG.V":"Strikepoint Gold","AHR.V":"American Helium",
    "AGX.V":"Argo Gold","AZM.V":"Azimut Exploration","BHS.V":"Bayhorse Silver",
    "KDK.V":"Kodiak Copper","SURG.V":"Surge Copper","CUU.V":"Copper Fox Metals",
    "USCU.V":"US Copper","ABR.V":"Aberdeen International","MCS.V":"Miners Capital",
    "AFM.V":"Alphamin Resources",
}

SECTOR_MAP = {t: "Energy" for t in [
    "PRQ.TO","BNE.TO","KEI.TO","PNE.TO","JOY.TO",
    "HME.V","ALV.V","PCQ.V","TAO.V","PUL.V",
]}

# ── Detection thresholds (aligned with updated watcher.py) ──────────────────
MIN_MOVE        = 0.04    # 4% total move to qualify as catalyst
MIN_VOL_RATIO   = 1.5
MIN_INTRADAY    = 0.10    # ≥10% intraday (lower gate for PR validation — want more events)
MAX_INTRADAY    = 0.40    # cap: 40%+ tend to reverse
MIN_DOLLAR_VOL  = 50_000
MIN_PRICE       = 0.05

# ── Keyword engine (identical to watcher.py) ────────────────────────────────
EARNINGS_KW = [
    "quarterly results","annual results","financial results",
    "q1 ","q2 ","q3 ","q4 ","fourth quarter","third quarter","second quarter","first quarter",
    "earnings per share","net income","revenue","operating cash flow",
    "funds from operations","cash flow from operations","net earnings","adjusted earnings",
]
DRILL_KW = [
    "drill results","assay results","drill program","intercepts","mineralization",
    "resource estimate","reserve estimate","grams per tonne","g/t","metres of","meters of",
    "hole ","intersection","drill hole","well results","production test",
    "flow rate","barrels per day","boe/d","completion results",
]
GUIDANCE_KW = [
    "guidance","outlook","forecast","production target","full year",
    "next quarter","2024","2025","2026","going forward","raises guidance","updates guidance",
]
POSITIVE_KW = [
    "beats consensus","beat consensus","exceeds expectations","surpasses consensus",
    "above consensus","above expectations","record quarter","record revenue",
    "record production","record cash flow","record earnings","record high",
    "stronger than expected","beats","exceeded","outperforms",
    "raises guidance","increases guidance","raises production","increases dividend",
    "declares dividend","normal course issuer bid","ncib","significant intercept",
    "high grade","broad zone","bought deal","acquisition",
]
NEGATIVE_KW = [
    "misses consensus","miss consensus","below consensus","below expectations",
    "shortfall","disappoints","reduces guidance","lowers guidance","cuts guidance",
    "suspends dividend","eliminates dividend","impairment","write-down","write-off",
    "restructuring","production shortfall","cost overrun","delays","force majeure",
]

FETCH_HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/120.0.0.0", "Accept": "text/html,*/*"}
WIRE_DOMAINS  = ("globenewswire","prnewswire","businesswire","accesswire","newswire")


# ══════════════════════════════════════════════════════════════════════════════
# Keyword scorer
# ══════════════════════════════════════════════════════════════════════════════

def score(title: str | None, body: str | None) -> dict:
    text = ((title or "") + " " + (body or "")).lower()
    if any(k in text for k in EARNINGS_KW):   rtype = "earnings"
    elif any(k in text for k in DRILL_KW):    rtype = "drill"
    elif any(k in text for k in GUIDANCE_KW): rtype = "guidance"
    else:                                      rtype = "unknown"

    pos = [k for k in POSITIVE_KW if k in text]
    neg = [k for k in NEGATIVE_KW if k in text]
    s   = len(pos) - len(neg)
    has_guidance = any(k in text for k in GUIDANCE_KW)

    if s >= 2 and has_guidance: signal = "STRONG BUY"
    elif s > 0:                 signal = "BUY"
    elif s < 0:                 signal = "SKIP"
    else:                       signal = "CAUTION"

    return {
        "signal": signal, "score": s, "release_type": rtype,
        "has_guidance": has_guidance, "pr_found": bool(title),
        "pos_hits": pos[:4], "neg_hits": neg[:4],
    }


# ══════════════════════════════════════════════════════════════════════════════
# Press release fetch
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_body(url: str) -> str | None:
    try:
        r = requests.get(url, headers=FETCH_HEADERS, timeout=10, allow_redirects=True)
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


HEADLINE_KW = [
    "quarter","result","earning","financial","revenue","income","production",
    "record","annual","fiscal","guidance","outlook","fourth","third","second",
    "first","q4","q3","q2","q1","drill","assay","intercept","resource",
]

_news_cache: dict[str, list] = {}

def _get_news(ticker: str) -> list:
    if ticker in _news_cache:
        return _news_cache[ticker]
    try:
        import io, contextlib
        with contextlib.redirect_stderr(io.StringIO()):
            news = yf.Ticker(ticker).news or []
    except Exception:
        news = []
    _news_cache[ticker] = news
    return news


def fetch_press_release(ticker: str, ev_date: date) -> tuple[str | None, str | None]:
    """Returns (title, body). Tries yfinance news first (works for ~3 months back)."""
    news = _get_news(ticker)
    if not news:
        return None, None

    ws = ev_date - timedelta(days=3)
    we = ev_date + timedelta(days=2)

    cands = []
    for n in news:
        cn    = n.get("content", {}) or {}
        title = (cn.get("title") or n.get("title") or "").strip()
        pts   = cn.get("pubDate") or cn.get("displayTime") or n.get("providerPublishTime") or 0
        link  = (cn.get("canonicalUrl", {}).get("url") or
                 cn.get("clickThroughUrl", {}).get("url") or n.get("link") or "")
        try:
            if isinstance(pts, (int, float)) and pts > 0:
                pd_ = datetime.utcfromtimestamp(float(pts)).date()
            elif isinstance(pts, str) and pts:
                pd_ = datetime.fromisoformat(pts[:10]).date()
            else:
                pd_ = ev_date
        except Exception:
            pd_ = ev_date

        if not (ws <= pd_ <= we):
            continue
        s = sum(1 for kw in HEADLINE_KW if kw in title.lower())
        cands.append({"title": title, "link": link, "score": s, "pub_date": pd_})

    if not cands:
        return None, None

    cands.sort(key=lambda x: (x["score"], -(abs((ev_date - x["pub_date"]).days))), reverse=True)
    best = cands[0]
    body = None
    if best["link"] and any(d in best["link"] for d in WIRE_DOMAINS):
        body = _fetch_body(best["link"])
    if not body:
        body = " ".join(c["title"] for c in cands[:3] if c["title"]) or None

    return best["title"], body


# ══════════════════════════════════════════════════════════════════════════════
# Forward returns
# ══════════════════════════════════════════════════════════════════════════════

def get_returns(hist: pd.DataFrame, ev_date: date) -> dict | None:
    dates  = sorted(hist.index)
    d0     = next((d for d in dates if d >= ev_date), None)
    if d0 is None:
        return None
    future = [d for d in dates if d > d0]
    if len(future) < 3:
        return None
    op   = float(hist.loc[d0, "Open"])
    cl0  = float(hist.loc[d0, "Close"])
    if op <= 0:
        return None
    return {
        "open_d0":  round(op, 4),
        "close_d0": round(cl0, 4),
        "ret_d1":   round((float(hist.loc[future[0], "Close"]) - op) / op * 100, 2),
        "ret_d2":   round((float(hist.loc[future[1], "Close"]) - op) / op * 100, 2),
        "ret_d3":   round((float(hist.loc[future[2], "Close"]) - op) / op * 100, 2),
        "ret_d1_close": round((float(hist.loc[future[0], "Close"]) - cl0) / cl0 * 100, 2),
        "ret_d3_close": round((float(hist.loc[future[2], "Close"]) - cl0) / cl0 * 100, 2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# MODE 1 — Recent events (yfinance news works ~3 months back)
# ══════════════════════════════════════════════════════════════════════════════

def run_recent(tickers: list[str], days: int = 90, verbose: bool = False) -> pd.DataFrame:
    end   = date.today()
    start = end - timedelta(days=days)

    print(f"MODE 1 — Recent events with press release fetch")
    print(f"Period  : {start} → {end}  ({days} days)")
    print(f"Tickers : {len(tickers)}\n")

    rows  = []
    stats = dict(catalyst_days=0, pr_found=0, pr_missing=0,
                 sig_strong_buy=0, sig_buy=0, sig_caution=0, sig_skip=0)

    for i, ticker in enumerate(tickers, 1):
        sector = SECTOR_MAP.get(ticker, "Mining")
        buf_start = (start - timedelta(days=30)).isoformat()

        try:
            raw = yf.Ticker(ticker).history(
                start=buf_start, end=end.isoformat(),
                interval="1d", auto_adjust=True
            )
        except Exception:
            continue

        if raw.empty:
            continue

        raw.index = pd.to_datetime(raw.index).tz_localize(None).normalize()
        raw.index = raw.index.date

        # Find catalyst days in the window
        if len(raw) < 22:
            continue

        raw2 = raw.copy()
        raw2["prev_close"] = raw2["Close"].shift(1)
        raw2["total_move"] = (raw2["Close"] - raw2["prev_close"]).abs() / raw2["prev_close"]
        raw2["vol_20d"]    = raw2["Volume"].rolling(20, min_periods=10).mean()
        raw2["vol_ratio"]  = raw2["Volume"] / raw2["vol_20d"]
        raw2["gap"]        = (raw2["Open"] - raw2["prev_close"]) / raw2["prev_close"]
        raw2["intraday"]   = (raw2["Close"] - raw2["Open"]) / raw2["Open"]
        raw2["dvol"]       = raw2["Open"] * raw2["Volume"]

        mask = (
            (raw2.index >= start) & (raw2.index <= end)
            & (raw2["total_move"]  >= MIN_MOVE)
            & (raw2["vol_ratio"]   >= MIN_VOL_RATIO)
            & (raw2["prev_close"]  >= MIN_PRICE)
            & (raw2["dvol"]        >= MIN_DOLLAR_VOL)
            & (raw2["intraday"].abs() >= MIN_INTRADAY)
            & (raw2["intraday"].abs() <  MAX_INTRADAY)
        )
        catalysts = raw2[mask]
        stats["catalyst_days"] += len(catalysts)

        if verbose and len(catalysts) > 0:
            print(f"  [{i:2d}] {ticker:<10} {len(catalysts)} catalyst day(s)")

        seen: set[date] = set()
        for ev_date, row in catalysts.iterrows():
            if any(abs((ev_date - d).days) <= 5 for d in seen):
                continue
            seen.add(ev_date)

            title, body = fetch_press_release(ticker, ev_date)
            if title:
                stats["pr_found"] += 1
            else:
                stats["pr_missing"] += 1

            analysis = score(title, body)
            sig = analysis["signal"]
            stats[f"sig_{sig.lower().replace(' ', '_')}"] += 1

            fwd = get_returns(raw, ev_date)
            if fwd is None:
                continue

            during_mkt = abs(row["intraday"]) > abs(row["gap"])
            rows.append({
                "ticker":       ticker,
                "sector":       sector,
                "event_date":   ev_date.isoformat(),
                "signal":       sig,
                "release_type": analysis["release_type"],
                "pr_found":     analysis["pr_found"],
                "score":        analysis["score"],
                "pos_hits":     ", ".join(analysis["pos_hits"]),
                "neg_hits":     ", ".join(analysis["neg_hits"]),
                "intraday_pct": round(row["intraday"] * 100, 2),
                "during_market":during_mkt,
                "headline":     (title or "")[:100],
                **fwd,
            })

        time.sleep(0.25)

    print(f"\nPipeline stats:")
    for k, v in stats.items():
        print(f"  {k:<20}: {v}")

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# MODE 2 — Live log (reads PRs saved by watcher.py)
# ══════════════════════════════════════════════════════════════════════════════

def run_live_log(verbose: bool = False) -> pd.DataFrame:
    pr_files = sorted(PR_LOG_DIR.glob("*.json"))
    print(f"MODE 2 — Live log  ({len(pr_files)} saved press releases in {PR_LOG_DIR})")

    if not pr_files:
        print(f"\n  No press releases logged yet.")
        print(f"  Run watcher.py during market hours and signals will be saved here.")
        print(f"  Re-run this script after a few weeks of live operation.")
        return pd.DataFrame()

    rows = []
    for fp in pr_files:
        try:
            with open(fp) as f:
                data = json.load(f)
        except Exception:
            continue

        ticker    = data.get("ticker", "")
        ev_date   = datetime.fromisoformat(data["event_date"]).date()
        title     = data.get("title", "")
        body      = data.get("body", "")

        if ticker not in TICKERS:
            continue

        analysis = score(title, body)

        # Fetch forward returns for this date
        try:
            buf = (ev_date - timedelta(days=5)).isoformat()
            end = (ev_date + timedelta(days=10)).isoformat()
            raw = yf.Ticker(ticker).history(start=buf, end=end, interval="1d", auto_adjust=True)
            raw.index = pd.to_datetime(raw.index).tz_localize(None).normalize()
            raw.index = raw.index.date
        except Exception:
            continue

        fwd = get_returns(raw, ev_date)
        if fwd is None:
            continue

        rows.append({
            "ticker":       ticker,
            "sector":       SECTOR_MAP.get(ticker, "Mining"),
            "event_date":   ev_date.isoformat(),
            "signal":       analysis["signal"],
            "release_type": analysis["release_type"],
            "pr_found":     True,
            "score":        analysis["score"],
            "pos_hits":     ", ".join(analysis["pos_hits"]),
            "neg_hits":     ", ".join(analysis["neg_hits"]),
            "headline":     title[:100],
            **fwd,
        })

        if verbose:
            print(f"  {ev_date}  {ticker:<10}  {analysis['signal']:<12}  D+1={fwd['ret_d1']:+.1f}%  {title[:50]}")

        time.sleep(0.1)

    print(f"\n  Loaded {len(rows)} events from live press release log")
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════

def report(df: pd.DataFrame, note: str = "") -> None:
    if df.empty:
        print("\nNo events to report.")
        return

    print(f"\n{'='*65}")
    print(f"KEYWORD SCORING VALIDATION — {len(df)} events, {df['ticker'].nunique()} tickers")
    if note:
        print(f"Note: {note}")
    print(f"{'='*65}\n")

    def grp_stats(sub, label):
        if sub.empty: return
        r1 = sub["ret_d1"]; r3 = sub["ret_d3"]
        r1c = sub["ret_d1_close"]; r3c = sub["ret_d3_close"]
        print(f"  {label} (n={len(sub)}):")
        print(f"    Open-entry  D+1 avg={r1.mean():+.2f}%  win={(r1>0).mean()*100:.0f}%  "
              f"D+3 avg={r3.mean():+.2f}%  win={(r3>0).mean()*100:.0f}%")
        print(f"    Close-entry D+1 avg={r1c.mean():+.2f}%  win={(r1c>0).mean()*100:.0f}%  "
              f"D+3 avg={r3c.mean():+.2f}%  win={(r3c>0).mean()*100:.0f}%")

    # By keyword signal
    print("── By keyword signal ─────────────────────────────────────────")
    for sig in ["STRONG BUY", "BUY", "CAUTION", "SKIP"]:
        grp_stats(df[df["signal"] == sig], sig)

    # PR found vs not
    print("\n── PR found vs not ───────────────────────────────────────────")
    grp_stats(df[df["pr_found"] == True],  "PR text fetched")
    grp_stats(df[df["pr_found"] == False], "Headline only / no PR")

    # By release type
    print("\n── By release type ───────────────────────────────────────────")
    for rt in sorted(df["release_type"].unique()):
        grp_stats(df[df["release_type"] == rt], rt)

    # THE KEY QUESTION: does BUY > CAUTION > SKIP?
    print("\n── THE KEY QUESTION: keyword edge ────────────────────────────")
    pos = df[df["signal"].isin(["BUY", "STRONG BUY"])]
    neu = df[df["signal"] == "CAUTION"]
    neg = df[df["signal"] == "SKIP"]
    if len(pos) > 0 and len(neu) > 0:
        d1_diff = pos["ret_d1"].mean() - neu["ret_d1"].mean()
        print(f"  BUY vs CAUTION D+1 gap: {d1_diff:+.2f}% — "
              f"{'keyword scoring ADDS value' if d1_diff > 0.5 else 'keyword scoring adds little edge' if d1_diff > 0 else '⚠ BUY underperforms CAUTION'}")
    if len(neg) > 0 and len(neu) > 0:
        d1_skip_diff = neu["ret_d1"].mean() - neg["ret_d1"].mean()
        print(f"  CAUTION vs SKIP D+1 gap: {d1_skip_diff:+.2f}% — "
              f"{'SKIP filtering works' if d1_skip_diff > 0.5 else '⚠ SKIP does not meaningfully differ'}")

    grp_stats(pos, "BUY / STRONG BUY signals")
    grp_stats(neu, "CAUTION signals")
    grp_stats(neg, "SKIP signals")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--mode",    choices=["recent", "live"], default="recent")
    p.add_argument("--days",    type=int,  default=90,    help="Lookback days (recent mode)")
    p.add_argument("--ticker",  type=str,  default=None,  help="Single ticker debug")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    tickers = [args.ticker] if args.ticker else TICKERS

    if args.mode == "live":
        df = run_live_log(verbose=args.verbose)
        note = "From watcher.py live press release log"
    else:
        df = run_recent(tickers, days=args.days, verbose=args.verbose)
        note = f"Recent {args.days}-day window — yfinance news (~3 months)"

    if df.empty:
        return

    # Save
    out = OUT_DIR / f"backtest_v3_{args.mode}.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved {len(df)} events → {out}")

    report(df, note=note)

    # Warn if sample is too small to draw conclusions
    if len(df) < 30:
        print(f"\n⚠  Only {len(df)} events — results are directional only, not statistically reliable.")
        print(f"   Need ≥100 events with PR text fetched for meaningful validation.")
        print(f"   Run watcher.py for 4-8 weeks in live mode to build the dataset.")


if __name__ == "__main__":
    main()
