#!/usr/bin/env python3
"""Builds Matt's Trading Desk data without Claude.

Runs on GitHub Actions every 15 minutes:
  1. Reads this week's setups from the CONFIG message in Matt's Discord (read only, posts nothing).
  2. Pulls live OANDA prices and candles from TradingView, news from Forex Factory, currency strength.
  3. Writes site/snapshot.json + site/index.html, which GitHub Pages serves.
Off-list ideas (the heavy scan of 28 pairs + gold) refresh once an hour; other runs reuse the last ones.
"""
import json, os, shutil, subprocess, sys, urllib.request
from datetime import datetime, timezone

try:
    import websocket  # noqa: F401  (TradingView chart feed)
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "websocket-client"])

import engine

SITE = "site"
LIVE_URL = os.environ.get("SITE_URL", "").rstrip("/")


def previous_snapshot():
    if not LIVE_URL:
        return {}
    try:
        req = urllib.request.Request(LIVE_URL + "/snapshot.json", headers={"User-Agent": "matt-trading-desk"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.load(r)
    except Exception:
        return {}


def watch_first(pairs):
    rank = {"AT ZONE": 0, "NEAR": 1, "FAR": 2, "INVALID": 3}
    live = sorted((p for p in pairs if p["status"] != "INVALID"), key=lambda p: (rank[p["status"]], p["pips_away"]))[:2]
    out = []
    for p in live:
        side = "buy" if p["side"] == "BUY" else "sell"
        ctt = "CTT" in str(p["grade"])
        where = "is at the zone" if p["status"] == "AT ZONE" else f"is {p['pips_away']} pips from the zone"
        need = "a 4-hour confirmation candle and half risk (counter-trend, high risk)" if ctt else f"a {'bullish' if side == 'buy' else 'bearish'} 1-hour entry pattern"
        out.append(f"{p['pair']} {side} {where} ({p['zone'][0]} to {p['zone'][1]}) — waiting for {need}.")
    return " ".join(out) or "Nothing close to an entry right now."


def main():
    if not engine.TOKEN:
        sys.exit("DISCORD_TOKEN secret is missing")
    snap = engine.snapshot()
    prev = previous_snapshot()
    if datetime.now(timezone.utc).minute < 15 or not prev.get("ideas"):
        _, cfg = engine.find_config()
        snap["ideas"] = engine.scout({s[0] for s in cfg["setups"]})
    else:
        snap["ideas"] = prev.get("ideas", [])
    snap["watch_first"] = watch_first(snap["pairs"])
    charts = {}
    for p in snap["pairs"]:  # last ~3 weeks of 4-hour candles for the plan chart on each card
        if p["pair"] not in charts:
            dp = 2 if p["pair"] == "XAUUSD" else 3 if "JPY" in p["pair"] else 5
            charts[p["pair"]] = [[int(b["t"]), round(b["o"], dp), round(b["h"], dp), round(b["l"], dp), round(b["c"], dp)] for b in engine.candles(p["pair"], 4, 110)]
    snap["chart4h"] = charts
    q = engine.live([p["pair"] for p in snap["pairs"]])
    suggested = {}
    for p in snap["pairs"]:  # where price has turned 3+ times on the 4-hour: candidate areas of interest
        if p["pair"] in suggested: continue
        price, atr = q[p["pair"]]["price"], q[p["pair"]]["atr4"]
        mine = [x["zone"] for x in snap["pairs"] if x["pair"] == p["pair"]]
        zs = [z for z in engine.find_zones(engine.candles(p["pair"], 4, 360), atr)
              if not any(z[0] <= m[1] and z[1] >= m[0] for m in mine)]
        zs = sorted(zs, key=lambda z: abs((z[0] + z[1]) / 2 - price))[:4]
        dp = 2 if p["pair"] == "XAUUSD" else 3 if "JPY" in p["pair"] else 5
        suggested[p["pair"]] = [[round(z[0], dp), round(z[1], dp), z[2]] for z in zs]
    snap["zones4h"] = suggested
    snap["open_trades"] = ["Open trades are tracked in Discord #trade-log."]
    r = snap.get("risk") or {}
    snap["risk"] = {"pct_a": r.get("pct_a"), "pct_ctt": r.get("pct_ctt")}  # balance stays private: the page is public
    snap["source"] = "github"
    snap["candle_source"] = dict(engine.CANDLE_SOURCE)
    os.makedirs(SITE, exist_ok=True)
    json.dump(snap, open(f"{SITE}/snapshot.json", "w"))
    shutil.copy("web/index.html" if os.path.exists("web/index.html") else "index.html", f"{SITE}/index.html")
    print("wrote", len(snap["pairs"]), "pairs,", len(snap["ideas"]), "ideas, updated", snap["updated_cot"])
    print("candles from:", snap["candle_source"])


if __name__ == "__main__":
    main()
