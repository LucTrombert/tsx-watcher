"""
backtest_v4.py — Intraday mid-day entry backtest (watcher.py simulation)

Simulates exactly what watcher.py does live:
  1. A press release hits during market hours (9:30–16:00 ET)
  2. The stock is already moving ≥10% (energy) / ≥15% (mining) from the day's open
  3. Entry is taken at the current price when the threshold is first crossed
  4. Exit rule: hold to D+3 close if green at D+1 close, cut at D+1 close if red

Key difference from backtest_v2/v3:
  - Entry price = mid-day bar when threshold is crossed (NOT next-day open)
  - Uses 5-min intraday data (yfinance: ~85 days back)
  - Applies all watcher.py filters: move gate, 40% cap, $50k dollar vol, $0.05 min price
  - Volume proxy for "press release during market hours" = vol ratio ≥ 2x 20-day avg
    on that day AND volume is concentrated in the first half of the session

Usage:
    python3 backtest_v4.py                  # all tickers, default filters
    python3 backtest_v4.py --verbose        # show each event
    python3 backtest_v4.py --ticker KEI.TO  # single ticker debug
    python3 backtest_v4.py --min-move 10    # override mining threshold to 10%
"""
from __future__ import annotations

import argparse, contextlib, io, time, warnings
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")

ET      = ZoneInfo("America/New_York")
OUT_DIR = Path("data/backtest")

# ── Ticker universe ──────────────────────────────────────────────────────────
TICKERS = [
    "PRQ.TO","BNE.TO","KEI.TO","PNE.TO","JOY.TO",
    "HME.V","ALV.V","PCQ.V","TAO.V","PUL.V",
    "GMX.TO","ORV.TO","RVX.TO",
    "SAG.V","AHR.V","AGX.V","AZM.V","BHS.V",
    "KDK.V","SURG.V","CUU.V",
    "USCU.V","ABR.V","MCS.V","AFM.V",
]

ENERGY_TICKERS = {
    "PRQ.TO","BNE.TO","KEI.TO","PNE.TO","JOY.TO",
    "HME.V","ALV.V","PCQ.V","TAO.V","PUL.V",
}

# ── Filters (aligned with watcher.py) ───────────────────────────────────────
MIN_MOVE_MINING  = 0.15   # ≥15% from day open
MIN_MOVE_ENERGY  = 0.10   # ≥10% from day open
MAX_MOVE         = 0.40   # cap: 40%+ tend to reverse
MIN_DOLLAR_VOL   = 50_000 # dollar volume up to signal bar
MIN_PRICE        = 0.05
VOL_RATIO_MIN    = 1.8    # volume ratio to 20-day avg (proxy for PR day)

MARKET_OPEN_H,  MARKET_OPEN_M  = 9, 30
MARKET_CLOSE_H, MARKET_CLOSE_M = 16, 0


def is_market_bar(ts: pd.Timestamp) -> bool:
    """True if bar falls within 9:30–16:00 ET."""
    loc = ts.tz_convert(ET)
    t = loc.hour * 60 + loc.minute
    return (MARKET_OPEN_H * 60 + MARKET_OPEN_M) <= t < (MARKET_CLOSE_H * 60 + MARKET_CLOSE_M)


def get_day_open(day_bars: pd.DataFrame) -> float | None:
    """First bar open price of the trading session."""
    mkt = day_bars[day_bars.index.map(is_market_bar)]
    if mkt.empty:
        return None
    return float(mkt["Open"].iloc[0])


# ── Daily data helpers ────────────────────────────────────────────────────────

def get_daily(ticker: str) -> pd.DataFrame | None:
    """Download 1-year daily OHLCV for volume ratio calculation."""
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            df = yf.download(ticker, period="1y", interval="1d",
                             progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(ticker, axis=1, level=1) if ticker in df.columns.get_level_values(1) else df
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
        df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
        df["vol_20d"] = df["Volume"].rolling(20, min_periods=10).mean()
        df["vol_ratio"] = df["Volume"] / df["vol_20d"]
        return df
    except Exception:
        return None


def get_intraday(ticker: str) -> pd.DataFrame | None:
    """Download 5-min intraday for last ~85 days."""
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            df = yf.download(ticker, period="60d", interval="5m",
                             progress=False, auto_adjust=True)
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df = df.xs(ticker, axis=1, level=1) if ticker in df.columns.get_level_values(1) else df
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
        return df
    except Exception:
        return None


def get_forward_returns(daily: pd.DataFrame, entry_date: date, entry_price: float) -> dict | None:
    """
    From entry_price on entry_date, measure:
      ret_d1_close  : D+1 close vs entry price
      ret_d3_close  : D+3 close vs entry price
      exit_return   : applies watcher.py exit rule:
                      green at D+1 → hold to D+3; red at D+1 → cut at D+1
    """
    dates = sorted(daily.index)
    future = [d for d in dates if d.date() > entry_date]
    if len(future) < 3:
        return None

    d1_close = float(daily.loc[future[0], "Close"])
    d3_close = float(daily.loc[future[2], "Close"])

    ret_d1 = (d1_close - entry_price) / entry_price * 100
    ret_d3 = (d3_close - entry_price) / entry_price * 100

    # Exit rule: green at D+1 → hold to D+3, red → cut at D+1
    exit_ret = ret_d3 if ret_d1 > 0 else ret_d1

    return {
        "ret_d1":    round(ret_d1, 2),
        "ret_d3":    round(ret_d3, 2),
        "exit_ret":  round(exit_ret, 2),
        "d1_close":  round(d1_close, 4),
        "d3_close":  round(d3_close, 4),
    }


# ── Core: find mid-day signal bars ───────────────────────────────────────────

def find_signal_events(
    ticker: str,
    intraday: pd.DataFrame,
    daily: pd.DataFrame,
    min_move: float,
    verbose: bool = False,
) -> list[dict]:
    """
    For each trading day in intraday data:
      1. Calculate cumulative move from day's first bar open
      2. Find the FIRST bar where |move| >= min_move AND move <= MAX_MOVE
      3. Check dollar volume up to that bar >= MIN_DOLLAR_VOL
      4. Check daily vol_ratio >= VOL_RATIO_MIN (proxy for press release day)
      5. Record entry price, time, and forward returns
    """
    events = []

    # Group 5-min bars by trading date (ET)
    intraday_et = intraday.copy()
    intraday_et.index = pd.to_datetime(intraday_et.index)
    if intraday_et.index.tz is None:
        intraday_et.index = intraday_et.index.tz_localize("UTC")

    # Only keep market-hours bars
    mkt_bars = intraday_et[intraday_et.index.map(is_market_bar)]
    if mkt_bars.empty:
        return []

    mkt_bars = mkt_bars.copy()
    mkt_bars["_date"] = mkt_bars.index.map(lambda t: t.tz_convert(ET).date())

    for day, day_bars in mkt_bars.groupby("_date"):
        if len(day_bars) < 6:   # need at least 30 min of data
            continue

        open_px = float(day_bars["Open"].iloc[0])
        if open_px < MIN_PRICE:
            continue

        # ── Check daily vol_ratio for this date ──────────────────────────
        daily_idx = daily.index
        if not hasattr(daily_idx[0], 'date'):
            day_matches = daily[daily.index == pd.Timestamp(day)]
        else:
            day_matches = daily[daily.index == pd.Timestamp(day)]

        if day_matches.empty:
            continue
        vol_ratio = float(day_matches["vol_ratio"].iloc[0])
        if vol_ratio < VOL_RATIO_MIN:
            continue  # not a PR-type day (low unusual volume)

        # ── Scan bars for first threshold crossing ───────────────────────
        cum_dvol = 0.0
        triggered = False

        for ts, bar in day_bars.iterrows():
            close  = float(bar["Close"])
            volume = float(bar["Volume"])
            cum_dvol += close * volume

            move = (close - open_px) / open_px

            if abs(move) < min_move:
                continue
            if abs(move) >= MAX_MOVE:
                if verbose:
                    print(f"  ↷ {ticker} {day} — move {abs(move)*100:.1f}% ≥ 40% cap, skipping")
                break  # don't look further — already in reversal zone

            if cum_dvol < MIN_DOLLAR_VOL:
                if verbose:
                    print(f"  ↷ {ticker} {day} — dvol ${cum_dvol:,.0f} < ${MIN_DOLLAR_VOL:,} at threshold cross")
                continue

            # Signal bar found — entry at this bar's close price
            entry_price = close
            entry_time  = ts.tz_convert(ET).strftime("%H:%M")
            direction   = "UP" if move > 0 else "DOWN"

            fwd = get_forward_returns(daily, day, entry_price)
            if fwd is None:
                break

            events.append({
                "ticker":       ticker,
                "sector":       "Energy" if ticker in ENERGY_TICKERS else "Mining",
                "date":         day.isoformat(),
                "entry_time":   entry_time,
                "direction":    direction,
                "open_px":      round(open_px, 4),
                "entry_price":  round(entry_price, 4),
                "intraday_pct": round(move * 100, 2),
                "dvol_at_signal": round(cum_dvol, 0),
                "vol_ratio":    round(vol_ratio, 2),
                **fwd,
            })

            if verbose:
                print(f"  ✓ {ticker} {day} {entry_time}  "
                      f"move={move*100:+.1f}%  entry=${entry_price:.3f}  "
                      f"D+1={fwd['ret_d1']:+.1f}%  D+3={fwd['ret_d3']:+.1f}%  "
                      f"exit={fwd['exit_ret']:+.1f}%")

            triggered = True
            break  # one signal per day per ticker

        if not triggered and verbose:
            pass  # quiet for non-events

    return events


# ── Reporting ─────────────────────────────────────────────────────────────────

def report(df: pd.DataFrame) -> None:
    n = len(df)
    tickers_hit = df["ticker"].nunique()

    print(f"\n{'='*65}")
    print(f"INTRADAY MID-DAY ENTRY BACKTEST — {n} events, {tickers_hit} tickers")
    print(f"Entry: price when move threshold first crossed during market hours")
    print(f"Exit rule: green D+1 → hold D+3 | red D+1 → cut D+1")
    print(f"{'='*65}\n")

    def grp(sub: pd.DataFrame, label: str):
        if sub.empty:
            return
        r1  = sub["ret_d1"]
        r3  = sub["ret_d3"]
        ex  = sub["exit_ret"]
        print(f"  {label} (n={len(sub)}):")
        print(f"    D+1       : avg={r1.mean():+.2f}%  win={(r1>0).mean()*100:.0f}%  "
              f"median={r1.median():+.2f}%")
        print(f"    D+3       : avg={r3.mean():+.2f}%  win={(r3>0).mean()*100:.0f}%  "
              f"median={r3.median():+.2f}%")
        print(f"    Exit rule : avg={ex.mean():+.2f}%  win={(ex>0).mean()*100:.0f}%  "
              f"median={ex.median():+.2f}%")

    print("── Overall ───────────────────────────────────────────────────")
    grp(df, "All signals")

    print("\n── By sector ─────────────────────────────────────────────────")
    for sec in ["Energy", "Mining"]:
        grp(df[df["sector"] == sec], sec)

    print("\n── By direction (UP move vs DOWN move) ──────────────────────")
    grp(df[df["direction"] == "UP"],   "Upside moves (long candidates)")
    grp(df[df["direction"] == "DOWN"], "Downside moves (short/avoid)")

    print("\n── Entry timing ──────────────────────────────────────────────")
    df["hour"] = df["entry_time"].str[:2].astype(int)
    for h_label, h_range in [("Morning 9:30–11:30", (9, 11)), ("Midday 11:30–14:00", (11, 13)), ("Afternoon 14:00–16:00", (14, 15))]:
        sub = df[df["hour"].between(h_range[0], h_range[1])]
        grp(sub, h_label)

    print("\n── Asymmetry: D+1 green vs red ──────────────────────────────")
    d1_green = df[df["ret_d1"] > 0]
    d1_red   = df[df["ret_d1"] <= 0]
    if not d1_green.empty and not d1_red.empty:
        print(f"  D+1 green → D+3 avg: {d1_green['ret_d3'].mean():+.2f}%  "
              f"win={(d1_green['ret_d3']>0).mean()*100:.0f}%  (n={len(d1_green)})")
        print(f"  D+1 red   → D+3 avg: {d1_red['ret_d3'].mean():+.2f}%  "
              f"win={(d1_red['ret_d3']>0).mean()*100:.0f}%  (n={len(d1_red)})")

    print("\n── Top events ────────────────────────────────────────────────")
    top = df.sort_values("exit_ret", ascending=False).head(8)
    for _, r in top.iterrows():
        print(f"  {r['date']}  {r['ticker']:<10}  {r['entry_time']}  "
              f"move={r['intraday_pct']:+.1f}%  "
              f"D+1={r['ret_d1']:+.1f}%  D+3={r['ret_d3']:+.1f}%  "
              f"exit={r['exit_ret']:+.1f}%")

    print("\n── Worst events ──────────────────────────────────────────────")
    bot = df.sort_values("exit_ret").head(5)
    for _, r in bot.iterrows():
        print(f"  {r['date']}  {r['ticker']:<10}  {r['entry_time']}  "
              f"move={r['intraday_pct']:+.1f}%  "
              f"D+1={r['ret_d1']:+.1f}%  D+3={r['ret_d3']:+.1f}%  "
              f"exit={r['exit_ret']:+.1f}%")

    if n < 30:
        print(f"\n⚠  {n} events — directional only, not statistically reliable (need ≥100).")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Intraday mid-day entry backtest for watcher.py strategy")
    p.add_argument("--ticker",   type=str,   default=None, help="Single ticker debug")
    p.add_argument("--min-move", type=float, default=None, help="Override mining min move %% (e.g. 10)")
    p.add_argument("--verbose",  action="store_true")
    args = p.parse_args()

    tickers = [args.ticker] if args.ticker else TICKERS
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"INTRADAY BACKTEST — watcher.py signal simulation")
    print(f"Period  : last ~60 days of 5-min data")
    print(f"Filters : move ≥{MIN_MOVE_ENERGY*100:.0f}% (energy) / "
          f"≥{MIN_MOVE_MINING*100:.0f}% (mining)  |  cap {MAX_MOVE*100:.0f}%  |  "
          f"dvol ≥${MIN_DOLLAR_VOL:,}  |  vol_ratio ≥{VOL_RATIO_MIN}x")
    print(f"Tickers : {len(tickers)}\n")

    all_events: list[dict] = []

    for i, ticker in enumerate(tickers, 1):
        sector   = "Energy" if ticker in ENERGY_TICKERS else "Mining"
        min_move = (args.min_move / 100 if args.min_move else
                    MIN_MOVE_ENERGY if sector == "Energy" else MIN_MOVE_MINING)

        print(f"  [{i:2d}/{len(tickers)}] {ticker:<10} fetching intraday...", end=" ", flush=True)

        daily    = get_daily(ticker)
        intraday = get_intraday(ticker)

        if daily is None or intraday is None:
            print("no data")
            continue

        events = find_signal_events(ticker, intraday, daily, min_move, verbose=args.verbose)
        print(f"{len(events)} signal(s)")
        all_events.extend(events)

        time.sleep(0.3)

    if not all_events:
        print("\nNo events found.")
        return

    df = pd.DataFrame(all_events)
    out = OUT_DIR / "backtest_v4_intraday.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved {len(df)} events → {out}")

    report(df)


if __name__ == "__main__":
    main()
