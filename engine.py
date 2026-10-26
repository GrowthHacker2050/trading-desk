#!/usr/bin/env python3
"""Matt Marcus FX agent tools. All state lives in Matt's Discord server.
Commands: scan | inbox | log | config | config-set FILE | post alerts|log|ask FILE | candles PAIR TF_HOURS(1,2,4,24,168) N | live PAIR.. | news-day | image URL FILE | snapshot FILE
          | strength | touches PAIR LOW HIGH | scout | review
"""
import json, os, sys, urllib.request, urllib.error, time
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

TOKEN = os.environ.get("DISCORD_TOKEN", "")
MATT = "752289412867555462"
BOT = "1561867110387294258"
CH = {"alerts": "1561861721184927905", "log": "1561876469242920970", "ask": "1561876600352546926", "config": "1561861396625498225"}
COT, NY, LDN = ZoneInfo("America/Bogota"), ZoneInfo("America/New_York"), ZoneInfo("Europe/London")
RANK = {"FAR": 0, "NEAR": 1, "AT ZONE": 2, "INVALID": 3}


def http(url, method="GET", body=None, headers=None, tries=3):
    for i in range(tries):
        req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                     headers={"User-Agent": "Mozilla/5.0 (matt-fx-agent)", "Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and i < tries - 1:
                time.sleep(2 + 3 * i); continue
            raise


def dc(path, method="GET", body=None):
    return http("https://discord.com/api/v10" + path, method, body, {"Authorization": "Bot " + TOKEN, "User-Agent": "DiscordBot (matt-fx-agent, 1.0)"})


def post(channel, text):
    ids = []
    while text:
        chunk, text = text[:1900], text[1900:]
        ids.append(dc(f"/channels/{CH.get(channel, channel)}/messages", "POST", {"content": chunk})["id"])
    return ids


# ---------------- config (a bot message in #general starting with CONFIG) ----------------
def find_config():
    before = ""
    for _ in range(10):
        msgs = dc(f"/channels/{CH['config']}/messages?limit=100" + (f"&before={before}" if before else ""))
        for m in msgs:
            if m["author"]["id"] == BOT and m["content"].startswith("CONFIG"):
                return m["id"], json.loads(m["content"].split("```json", 1)[1].rsplit("```", 1)[0])
        if len(msgs) < 100: break
        before = msgs[-1]["id"]
    return None, None


def save_config(cfg):
    mid, _ = find_config()
    text = "CONFIG - Matt's AOI agent settings (do not delete)\n```json\n" + json.dumps(cfg, separators=(",", ":")) + "\n```"
    if len(text) > 2000: raise SystemExit("config too long for one Discord message")
    if mid: dc(f"/channels/{CH['config']}/messages/{mid}", "PATCH", {"content": text}); return mid
    return dc(f"/channels/{CH['config']}/messages", "POST", {"content": text})["id"]


# ---------------- market data ----------------
def live(pairs):
    cols = ["close", "close[1]|60", "high|60", "low|60", "ATR|240"]
    fx = [p for p in pairs if p != "XAUUSD"]
    tick = [f"OANDA:{p}" for p in fx] + ["OANDA:USDJPY", "OANDA:USDCAD", "OANDA:USDCHF", "OANDA:GBPUSD", "OANDA:EURUSD", "OANDA:AUDUSD", "OANDA:NZDUSD"]
    d = http("https://scanner.tradingview.com/forex/scan", "POST", {"symbols": {"tickers": list(dict.fromkeys(tick))}, "columns": cols})
    out = {r["s"].split(":")[1]: dict(zip(["price", "prev", "hhi", "hlo", "atr4"], r["d"])) for r in d["data"]}
    if "XAUUSD" in pairs:
        g = http("https://scanner.tradingview.com/cfd/scan", "POST", {"symbols": {"tickers": ["OANDA:XAUUSD"]}, "columns": cols})
        out.update({r["s"].split(":")[1]: dict(zip(["price", "prev", "hhi", "hlo", "atr4"], r["d"])) for r in g["data"]})
    return out


# ---------------- TradingView candles (same OANDA data as Matt's TradingView charts) ----------------
_TV_CACHE = {}
_TV_RES = {0.5: "30", 1: "60", 2: "120", 4: "240", 24: "1D", 168: "1W"}
_TV_FORMING = {}  # (pair, tf) -> the candle still open (for "forming" patterns)


def _tv_frame(obj):
    s = obj if isinstance(obj, str) else json.dumps(obj, separators=(",", ":"))
    return f"~m~{len(s)}~m~{s}"


def tv_candles(pair, tf_hours, n=500):
    """Closed OANDA candles straight from TradingView's chart feed. Raises on any failure (caller falls back)."""
    key = (pair, tf_hours)
    if key in _TV_CACHE and len(_TV_CACHE[key]) >= min(n, 1):
        return _TV_CACHE[key][-n:]
    import re, random, string, websocket
    ws = websocket.create_connection("wss://data.tradingview.com/socket.io/websocket",
                                     header=["Origin: https://www.tradingview.com"], timeout=20)
    try:
        cs = "cs_" + "".join(random.choices(string.ascii_lowercase, k=12))
        send = lambda m, p: ws.send(_tv_frame({"m": m, "p": p}))
        send("set_auth_token", ["unauthorized_user_token"])
        send("chart_create_session", [cs, ""])
        send("resolve_symbol", [cs, "sym_1", "=" + json.dumps({"symbol": f"OANDA:{pair}", "adjustment": "splits"})])
        send("create_series", [cs, "s1", "s1", "sym_1", _TV_RES[tf_hours], max(n, 500) + 2, ""])
        bars, deadline = [], time.time() + 25
        while time.time() < deadline:
            raw = ws.recv()
            for part in re.split(r"~m~\d+~m~", raw):
                if not part: continue
                if part.startswith("~h~"):
                    ws.send(_tv_frame(part)); continue
                try: msg = json.loads(part)
                except ValueError: continue
                m = msg.get("m")
                if m in ("timescale_update", "du"):
                    ser = (msg.get("p") or [None, {}])[1].get("s1", {}).get("s", [])
                    if m == "timescale_update": bars = [b["v"] for b in ser]
                elif m in ("symbol_error", "series_error", "critical_error"):
                    raise RuntimeError(f"TradingView {m}: {msg.get('p')}")
                elif m == "series_completed":
                    deadline = 0
        if not bars: raise RuntimeError("TradingView returned no candles")
    finally:
        try: ws.close()
        except Exception: pass
    now = datetime.now(timezone.utc).timestamp()
    out = []
    for v in bars:
        t, o, h, l, c = v[:5]
        span = tf_hours * 3600 if tf_hours < 168 else 7 * 86400
        if tf_hours == 24: span = 86400
        if t + span <= now + 60:  # closed bars only
            out.append({"t": int(t), "o": o, "h": h, "l": l, "c": c})
        else:
            _TV_FORMING[key] = {"t": int(t), "o": o, "h": h, "l": l, "c": c}
    if len(out) < 10: raise RuntimeError("TradingView returned too few closed candles")
    _TV_CACHE[key] = out
    return out[-n:]


CANDLE_SOURCE = {}


def candles(pair, tf_hours, n=80):
    """Closed candles from TradingView (OANDA); falls back to the backup feed if TradingView fails."""
    try:
        out = tv_candles(pair, tf_hours, n)
        CANDLE_SOURCE[pair] = "tradingview"
        return out
    except Exception as e:
        CANDLE_SOURCE[pair] = f"backup ({type(e).__name__})"
        return backup_candles(pair, tf_hours, n)


def backup_candles(pair, tf_hours, n=80):
    """Closed candles. tf_hours 1/2/4 built from Yahoo 1H data on the New York 17:00 boundary; 24 = daily, 168 = weekly."""
    sym = "GC=F" if pair == "XAUUSD" else f"{pair}=X"
    if tf_hours < 1: return []  # no 30-minute backup feed
    if tf_hours >= 168:  # weekly: completed weeks only (the feed's current and last partial bars are unreliable)
        d = http(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1wk&range=2y")["chart"]["result"][0]
        q = d["indicators"]["quote"][0]
        rows = [{"t": t, "o": o, "h": h, "l": l, "c": c} for t, o, h, l, c in zip(d["timestamp"], q["open"], q["high"], q["low"], q["close"]) if None not in (o, h, l, c)]
        wk_start = (datetime.now(timezone.utc).astimezone(NY) - timedelta(hours=17)).date()
        wk_start = datetime.combine(wk_start - timedelta(days=(wk_start.weekday() + 1) % 7), datetime.min.time(), NY).timestamp()
        return _gold_shift(pair, [r for r in rows if r["t"] < wk_start - 86400][-n:])
    for host in ("query1", "query2"):
        try:
            d = http(f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym}?interval=60m&range=60d")["chart"]["result"][0]
            break
        except Exception:
            d = None
    if not d: return []
    q = d["indicators"]["quote"][0]; ts = d["timestamp"]
    rows = [(t, o, h, l, c) for t, o, h, l, c in zip(ts, q["open"], q["high"], q["low"], q["close"]) if None not in (o, h, l, c)]
    out, cur = [], None
    for t, o, h, l, c in rows:
        ny = datetime.fromtimestamp(t, timezone.utc).astimezone(NY)
        k = (ny - timedelta(hours=17)).replace(minute=0, second=0, microsecond=0)
        key = (k.date(), k.hour // tf_hours)
        if cur and cur["k"] == key:
            cur["h"] = max(cur["h"], h); cur["l"] = min(cur["l"], l); cur["c"] = c; cur["n"] += 1
        else:
            if cur: out.append(cur)
            cur = {"k": key, "t": t, "o": o, "h": h, "l": l, "c": c, "n": 1}
    if cur: out.append(cur)
    now = datetime.now(timezone.utc).timestamp()
    out = [b for b in out if b["t"] + tf_hours * 3600 <= now + 60 and b["n"] >= max(1, tf_hours // 4)]  # closed, non-stub bars only
    return _gold_shift(pair, out[-n:], rows[-1][4] if rows else None)


def _gold_shift(pair, bars, last_fut=None):
    """Gold candles come from COMEX futures; shift them so the latest close matches OANDA spot."""
    if pair != "XAUUSD" or not bars: return bars
    spot = live(["XAUUSD"])["XAUUSD"]["price"]
    off = spot - (last_fut if last_fut is not None else bars[-1]["c"])
    return [{**b, "o": b["o"] + off, "h": b["h"] + off, "l": b["l"] + off, "c": b["c"] + off} for b in bars]


def swings(b, k=2):
    hi = [i for i in range(k, len(b) - k) if all(b[i]["h"] >= b[i + j]["h"] and b[i]["h"] >= b[i - j]["h"] for j in range(1, k + 1))]
    lo = [i for i in range(k, len(b) - k) if all(b[i]["l"] <= b[i + j]["l"] and b[i]["l"] <= b[i - j]["l"] for j in range(1, k + 1))]
    # two neighbouring candles with the same high/low are ONE swing, not two
    dedup = lambda idx, f: [i for n, i in enumerate(idx) if n == 0 or not (i - idx[n - 1] <= k and b[i][f] == b[idx[n - 1]][f])]
    return dedup(hi, "h"), dedup(lo, "l")


def detect(b, side, lo, hi, atr, need_zone=True):
    """Candlestick Trading Bible patterns completed on the LAST bar of b, in the plan direction, at or near the zone.
    Returns [{"pattern": name, "n": candles in the pattern (counted back from the last bar)}]."""
    if len(b) < 30: return []
    out = []
    add = lambda name, n: out.append({"pattern": name, "n": n})
    x, y, z = b[-3], b[-2], b[-1]
    body = lambda c: abs(c["c"] - c["o"]); rng = lambda c: max(c["h"] - c["l"], 1e-9)
    upw = lambda c: c["h"] - max(c["o"], c["c"]); low = lambda c: min(c["o"], c["c"]) - c["l"]
    tol = 0.15 * atr
    touch = lambda c: c["l"] <= hi + tol and c["h"] >= lo - tol
    if need_zone and not any(touch(c) for c in b[-5:]): return []
    bull = side == "BUY"
    at_zone = touch(z) or touch(y)
    # --- engulfing (Bible: the second body fully covers the first) ---
    if at_zone:
        if bull and z["c"] > z["o"] and y["c"] < y["o"] and z["c"] >= y["o"] and z["o"] <= y["c"] and body(z) > body(y): add("bullish engulfing", 2)
        if not bull and z["c"] < z["o"] and y["c"] > y["o"] and z["c"] <= y["o"] and z["o"] >= y["c"] and body(z) > body(y): add("bearish engulfing", 2)
    # --- single-candle rejections: dragonfly / gravestone doji first, else pin bar, else plain doji ---
    big = rng(z) >= 0.4 * atr
    if touch(z) and big:
        doji = body(z) <= 0.1 * rng(z)
        if bull and doji and upw(z) <= 0.15 * rng(z) and low(z) >= 0.6 * rng(z): add("dragonfly doji", 1)
        elif not bull and doji and low(z) <= 0.15 * rng(z) and upw(z) >= 0.6 * rng(z): add("gravestone doji", 1)
        elif bull and low(z) >= 2 * body(z) and low(z) >= 0.6 * rng(z) and z["l"] <= hi + tol and z["c"] > hi - (hi - lo) / 2: add("bullish pin bar", 1)
        elif not bull and upw(z) >= 2 * body(z) and upw(z) >= 0.6 * rng(z) and z["h"] >= lo - tol and z["c"] < lo + (hi - lo) / 2: add("bearish pin bar", 1)
        elif doji and min(upw(z), low(z)) >= 0.2 * rng(z): add("doji", 1)
    # --- morning / evening star ---
    if any(touch(c) for c in (x, y, z)):
        if bull and x["c"] < x["o"] and body(y) <= 0.35 * body(x) and z["c"] > z["o"] and z["c"] > (x["o"] + x["c"]) / 2: add("morning star", 3)
        if not bull and x["c"] > x["o"] and body(y) <= 0.35 * body(x) and z["c"] < z["o"] and z["c"] < (x["o"] + x["c"]) / 2: add("evening star", 3)
    # --- tweezers (same low / same high, opposite colours) ---
    if at_zone:
        same, sized = 0.05 * atr, rng(y) >= 0.5 * atr and rng(z) >= 0.5 * atr
        if bull and sized and y["c"] < y["o"] and z["c"] > z["o"] and abs(y["l"] - z["l"]) <= same: add("tweezer bottom", 2)
        if not bull and sized and y["c"] > y["o"] and z["c"] < z["o"] and abs(y["h"] - z["h"]) <= same: add("tweezer top", 2)
    # --- inside bar (harami): last bar inside a big mother bar ---
    inside = lambda c, m: c["h"] <= m["h"] and c["l"] >= m["l"]
    if at_zone and inside(z, y) and rng(y) >= 0.8 * atr and rng(z) <= 0.6 * rng(y):
        if bull and y["c"] < y["o"]: add("bullish inside bar", 2)
        if not bull and y["c"] > y["o"]: add("bearish inside bar", 2)
    # --- inside bar breakout and false breakout (mother 2-4 bars back, at least one inside bar after it) ---
    for k in (3, 4, 5):
        m, mid = b[-k], b[-k + 1:-1]
        if not mid or rng(m) < 0.6 * atr or not any(touch(c) for c in b[-k:]): continue
        if all(inside(c, m) for c in mid):
            if bull and z["c"] > m["h"]: add("inside bar breakout (bullish)", k); break
            if not bull and z["c"] < m["l"]: add("inside bar breakout (bearish)", k); break
            if bull and z["l"] < m["l"] and z["c"] > m["l"] and z["c"] > z["o"]: add("inside bar false breakout (bullish)", k); break
            if not bull and z["h"] > m["h"] and z["c"] < m["h"] and z["c"] < z["o"]: add("inside bar false breakout (bearish)", k); break
        # trap over two bars: the bar before broke out, the last bar closed back inside
        if len(mid) >= 2 and all(inside(c, m) for c in mid[:-1]):
            if bull and y["c"] < m["l"] and z["c"] > m["l"] and z["c"] > z["o"]: add("inside bar false breakout (bullish)", k); break
            if not bull and y["c"] > m["h"] and z["c"] < m["h"] and z["c"] < z["o"]: add("inside bar false breakout (bearish)", k); break
    # --- break and retest of the zone ---
    # (closed beyond the zone earlier, came back to test it on one of the last 2 candles, last candle closes away from it)
    broke = b[-14:-2]
    if bull and any(c["c"] > hi for c in broke) and min(y["l"], z["l"]) <= hi + 0.1 * atr and z["c"] > hi and z["c"] > z["o"] and y["c"] >= lo: add("break and retest (support held)", 2)
    if not bull and any(c["c"] < lo for c in broke) and max(y["h"], z["h"]) >= lo - 0.1 * atr and z["c"] < lo and z["c"] < z["o"] and y["c"] <= hi: add("break and retest (resistance held)", 2)
    # --- change of character on CLOSES: trend was against the plan, the last close breaks the last swing close ---
    c = [bb["c"] for bb in b]
    piv_h = [j for j in range(2, len(c) - 3) if all(c[j] >= c[j + m] and c[j] >= c[j - m] for m in (1, 2))]
    piv_l = [j for j in range(2, len(c) - 3) if all(c[j] <= c[j + m] and c[j] <= c[j - m] for m in (1, 2))]
    before = trend(b[:-1], 2)
    if bull and before == "Bearish" and piv_h and c[-1] > c[piv_h[-1]] >= c[-2]: add("change of character (bullish)", len(c) - piv_h[-1])
    if not bull and before == "Bullish" and piv_l and c[-1] < c[piv_l[-1]] <= c[-2]: add("change of character (bearish)", len(c) - piv_l[-1])
    # --- chart patterns on the neckline break ---
    sh, sl = swings(b[:-1])
    dtol = 0.35 * atr
    if not bull and len(sh) >= 2:
        a, c2 = sh[-2], sh[-1]
        if abs(b[a]["h"] - b[c2]["h"]) <= dtol and c2 - a >= 4:
            neck = min(bb["l"] for bb in b[a:c2 + 1])
            if z["c"] < neck <= y["c"]: add("double top (neckline break)", len(b) - a)
    if bull and len(sl) >= 2:
        a, c2 = sl[-2], sl[-1]
        if abs(b[a]["l"] - b[c2]["l"]) <= dtol and c2 - a >= 4:
            neck = max(bb["h"] for bb in b[a:c2 + 1])
            if z["c"] > neck >= y["c"]: add("double bottom (neckline break)", len(b) - a)
    if not bull and len(sh) >= 3:
        l_, h_, r_ = sh[-3:]
        if b[h_]["h"] > max(b[l_]["h"], b[r_]["h"]) and abs(b[l_]["h"] - b[r_]["h"]) <= 0.6 * atr:
            neck = min(min(bb["l"] for bb in b[l_:h_ + 1]), min(bb["l"] for bb in b[h_:r_ + 1]))
            if z["c"] < neck <= y["c"]: add("head and shoulders (neckline break)", len(b) - l_)
    if bull and len(sl) >= 3:
        l_, h_, r_ = sl[-3:]
        if b[h_]["l"] < min(b[l_]["l"], b[r_]["l"]) and abs(b[l_]["l"] - b[r_]["l"]) <= 0.6 * atr:
            neck = max(max(bb["h"] for bb in b[l_:h_ + 1]), max(bb["h"] for bb in b[h_:r_ + 1]))
            if z["c"] > neck >= y["c"]: add("inverse head and shoulders (neckline break)", len(b) - l_)
    return out


def patterns(b, side, lo, hi, atr):
    """Pattern names completed on the LAST closed bar (kept for the bot's existing commands)."""
    return [d["pattern"] for d in detect(b, side, lo, hi, atr)]


ENTRY_PATTERNS = {"bullish engulfing", "bearish engulfing", "bullish pin bar", "bearish pin bar", "morning star", "evening star",
                  "tweezer bottom", "tweezer top", "inside bar breakout (bullish)", "inside bar breakout (bearish)",
                  "inside bar false breakout (bullish)", "inside bar false breakout (bearish)",
                  "break and retest (support held)", "break and retest (resistance held)",
                  "double top (neckline break)", "double bottom (neckline break)",
                  "head and shoulders (neckline break)", "inverse head and shoulders (neckline break)"}
TF_NAME = {1: "1-hour", 4: "4-hour", 24: "Daily"}


def pattern_cards(pair, side, lo, hi, atr, bars_by_tf):
    """Confirmed (last closed candle) and forming (candle still open) patterns on the 1-hour, 4-hour and Daily,
    with the last 8 candles for the Desk diagram. 'kind' = entry (a trigger) or warning (doji, inside bar, change of character)."""
    cards = []
    for tf in (24, 4, 1):
        b = bars_by_tf.get(tf) or []
        if len(b) < 30: continue
        fb = _TV_FORMING.get((pair, tf))
        for state, bars in (("confirmed", b), ("forming", b + [fb] if fb else None)):
            if not bars: continue
            for d in detect(bars, side, lo, hi, atr_of(b) or atr):
                if state == "forming" and any(c["pattern"] == d["pattern"] and c["tf"] == TF_NAME[tf] for c in cards): continue
                win = 8 if d["n"] <= 6 else min(40, d["n"] + 4)  # chart patterns need the whole shape on screen
                last = bars[-win:]; n = min(d["n"], win)
                closes = datetime.fromtimestamp(bars[-1]["t"] + tf * 3600, timezone.utc).astimezone(COT)
                cards.append({"tf": TF_NAME[tf], "pattern": d["pattern"], "state": state,
                              "kind": "entry" if d["pattern"] in ENTRY_PATTERNS else "warning",
                              "candle_close_cot": closes.strftime("%a %H:%M") if tf < 24 else closes.strftime("%a %b %d"),
                              "candles": [[round(v, 5) for v in (c["o"], c["h"], c["l"], c["c"])] for c in last],
                              "mark": list(range(len(last) - n, len(last)))})
    return cards


# ---------------- Perfect Trade checklist auto-grade (same boxes and weights as theperfecttrade checklist) ----------------
CHECK = [("W", [("trend", 10), ("aoi", 10), ("ema", 5), ("round", 5), ("prev", 10), ("rej", 10), ("bnr", 10)]),
         ("D", [("trend", 10), ("aoi", 10), ("ema", 5), ("round", 5), ("prev", 10), ("rej", 10), ("bnr", 10)]),
         ("H4", [("trend", 5), ("aoi", 5), ("ema", 5), ("round", 5), ("prev", 10), ("rej", 5), ("bnr", 10)]),
         ("LTF", [("trend", 5), ("ema", 5), ("bnr", 5)]),
         ("ENTRY", [("sos", 10), ("eng", 10)]),
         ("RISK", [("sl", 0), ("tp", 0)])]
TIERS = [(160, "Perfect trade"), (140, "Excellent"), (120, "Outstanding"), (100, "Very strong"), (90, "Strong"),
         (80, "Good"), (70, "Acceptable"), (60, "Moderate"), (0, "Weak")]
REJECTION = {"bullish engulfing", "bearish engulfing", "bullish pin bar", "bearish pin bar", "dragonfly doji", "gravestone doji",
             "morning star", "evening star", "tweezer bottom", "tweezer top", "inside bar false breakout (bullish)", "inside bar false breakout (bearish)"}
STRUCT = {"break and retest (support held)", "break and retest (resistance held)", "head and shoulders (neckline break)",
          "inverse head and shoulders (neckline break)", "double top (neckline break)", "double bottom (neckline break)"}


def ema(vals, n=50):
    if len(vals) < n: return None
    k, e = 2 / (n + 1), sum(vals[:n]) / n
    for v in vals[n:]: e = v * k + e * (1 - k)
    return e


def atr_of(b, n=14):
    tr = [max(b[i]["h"], b[i - 1]["c"]) - min(b[i]["l"], b[i - 1]["c"]) for i in range(1, len(b))]
    return sum(tr[-n:]) / min(n, len(tr)) if tr else 0


def recent_any(b, side, lo, hi, atr, names, look=3):
    """Pattern from `names` on any of the last `look` closed bars of b."""
    for i in range(look):
        bars = b[:len(b) - i]
        for d in detect(bars, side, lo, hi, atr):
            if d["pattern"] in names: return d["pattern"], i
    return None, None


def checklist_grade(pair, side, lo, hi, plan, price, bars):
    """Grade one setup against Matt's Perfect Trade checklist. bars: {168, 24, 4, 2, 1, 0.5: candles}. Returns score out of 200."""
    want = "Bullish" if side == "BUY" else "Bearish"
    word = "bullish" if side == "BUY" else "bearish"
    items, P = [], pip(pair)
    fmt = lambda v: f"{v:.{2 if pair == 'XAUUSD' else 3 if 'JPY' in pair else 5}f}"
    mid = (lo + hi) / 2
    dist_zone = 0.0 if lo <= price <= hi else min(abs(price - lo), abs(price - hi))
    step = 50.0 if pair == "XAUUSD" else 1.0 if "JPY" in pair else 0.01  # big round numbers: 00 levels (gold: every $50)
    rnd = round(mid / step) * step
    tfs = {"W": 168, "D": 24, "H4": 4}
    names = {"W": "Weekly", "D": "Daily", "H4": "4-hour", "LTF": "1-hour"}

    def it(sec, iid, w, ok, why): items.append({"id": f"{sec}:{iid}", "w": w, "ok": bool(ok), "why": why})

    for sec, tf in tfs.items():
        b = bars.get(tf) or []
        w = dict(CHECK[[s for s, _ in CHECK].index(sec)][1])
        if len(b) < 20:
            for iid in w: it(sec, iid, w[iid], False, "not enough candles")
            continue
        a = atr_of(b) or 1e-9
        tr = trend(b, 1 if tf == 168 else 2)
        it(sec, "trend", w["trend"], tr == want, f"{names[sec]} structure on closes: {tr.lower()}")
        st = touch_stats(b[-(104 if tf == 168 else 250 if tf == 24 else 360):], lo, hi)
        near = dist_zone <= 0.5 * a
        it(sec, "aoi", w["aoi"], near and st["visits"] >= 3,
           f"zone touched {st['visits']} times on the {names[sec]}, price {'at' if dist_zone == 0 else f'{dist_zone / P:.0f} pips from'} the zone")
        e = ema([x["c"] for x in b] + [price])
        it(sec, "ema", w["ema"], e is not None and abs(price - e) <= 0.5 * a,
           f"50 EMA {fmt(e)} is {abs(price - e) / P:.0f} pips from price" if e else "not enough candles for the 50 EMA")
        rnd_ok = lo - 10 * P <= rnd <= hi + 10 * P  # a 00 level inside the zone, give or take 10 pips
        it(sec, "round", w["round"], rnd_ok, f"round number {fmt(rnd)} {'is inside' if rnd_ok else 'is not inside'} the zone")
        c = [x["c"] for x in b[:-5]]
        piv = [c[j] for j in range(2, len(c) - 2) if (c[j] >= max(c[j - 2:j + 3])) or (c[j] <= min(c[j - 2:j + 3]))]
        hit = [v for v in piv if lo - 0.3 * a <= v <= hi + 0.3 * a]
        it(sec, "prev", w["prev"], bool(hit), f"{len(hit)} old swing close{'s' if len(hit) != 1 else ''} inside the zone" if hit else "no old high or low lines up with the zone")
        p, ago = recent_any(b, side, lo, hi, a, REJECTION, 2)
        it(sec, "rej", w["rej"], p, f"{p} on the {names[sec]}" + (" (last candle)" if ago == 0 else " (previous candle)") if p else f"no {word} rejection candle at the zone yet")
        p, ago = recent_any(b, side, lo, hi, a, STRUCT, 6)
        it(sec, "bnr", w["bnr"], p, f"{p} on the {names[sec]}" if p else "no break and retest or head and shoulders yet")

    h1 = bars.get(1) or []
    a1 = atr_of(h1) or 1e-9
    t1 = trend(h1, 2) if len(h1) > 20 else "Not enough data"
    it("LTF", "trend", 5, t1 == want, f"1-hour structure on closes: {t1.lower()}")
    e1 = ema([x["c"] for x in h1] + [price])
    it("LTF", "ema", 5, e1 is not None and abs(price - e1) <= 0.5 * a1, f"1-hour 50 EMA {abs(price - e1) / P:.0f} pips from price" if e1 else "no 1-hour EMA")
    p, _ = recent_any(h1, side, lo, hi, a1, STRUCT, 6) if len(h1) > 30 else (None, None)
    it("LTF", "bnr", 5, p, f"{p} on the 1-hour" if p else "no 1-hour break and retest yet")

    # shift of structure: the 1-hour close-trend turned the plan's way within the last 24 hours
    sos = len(h1) > 60 and t1 == want and trend(h1[:-24], 2) != want
    it("ENTRY", "sos", 10, sos, "1-hour structure shifted your way in the last 24 hours" if sos else
       ("1-hour already trending your way (no fresh shift)" if t1 == want else "no shift of structure your way on the 1-hour yet"))
    eng = None
    for tf in (0.5, 1, 2, 4):
        b = bars.get(tf) or []
        if len(b) > 30 and any(d["pattern"].endswith("engulfing") for d in detect(b, side, lo, hi, atr_of(b))):
            eng = {0.5: "30-minute", 1: "1-hour", 2: "2-hour", 4: "4-hour"}[tf]; break
    it("ENTRY", "eng", 10, eng, f"closed {word} engulfing on the {eng}" if eng else f"no closed {word} engulfing at the zone (30-minute to 4-hour)")
    entry, sl, tp = (plan + [None] * 3)[:3]
    it("RISK", "sl", 0, sl, f"stop loss {fmt(sl)}" if sl else "no stop loss in the plan")
    it("RISK", "tp", 0, tp, f"take profit {fmt(tp)}" if tp else "no take profit in the plan")

    on = {x["id"] for x in items if x["ok"]}
    score = sum(x["w"] for x in items if x["ok"])
    tier = next(n for m, n in TIERS if score >= m)
    gates = [("2 of 3 trends (Weekly / Daily / 4-hour)", sum(f"{s}:trend" in on for s in ("W", "D", "H4")) >= 2),
             ("At a Weekly or Daily zone", "W:aoi" in on or "D:aoi" in on),
             ("Entry signal fired", "ENTRY:sos" in on or "ENTRY:eng" in on),
             ("Stop loss + take profit set", "RISK:sl" in on and "RISK:tp" in on),
             ("Pair + direction picked", True)]
    return {"score": score, "max": 200, "tier": tier, "go": all(ok for _, ok in gates),
            "gates": [{"label": g, "ok": ok} for g, ok in gates], "items": items,
            "prefill": {"pair": pair, "dir": side, "on": sorted(on), "entry": entry, "sl": sl, "tp": tp}}


def pip(pair): return 0.1 if pair == "XAUUSD" else 0.01 if "JPY" in pair else 0.0001


def pip_value_usd(pair, q):
    quote = pair[3:]
    if pair == "XAUUSD" or quote == "USD": return 10.0
    if quote == "JPY": return 1000 / q["USDJPY"]["price"]
    return 10 / q[f"USD{quote}"]["price"]


def lots(cfg, pair, grade, sl_pips, q):
    r = cfg.get("risk", {})
    if not r.get("balance") or not r.get("pct_a"): return None
    pct = r.get("pct_ctt") or r["pct_a"] / 2 if "CTT" in grade else r["pct_a"]
    raw = r["balance"] * pct / 100 / (sl_pips * pip_value_usd(pair, q))
    return max(0.01, int(raw * 100 + 1e-9) / 100)  # rounded DOWN; 0.01 is the broker minimum


def session(now):
    l, n = 8 <= now.astimezone(LDN).hour < 17, 8 <= now.astimezone(NY).hour < 17
    return "LONDON / NY OVERLAP" if l and n else "LONDON (priority 1)" if l else "NEW YORK (priority 2)" if n else None


def status(price, lo, hi, atr, inval, hh=None, hl=None):
    side, lvl = inval
    if (side == "above" and price > lvl) or (side == "below" and price < lvl): return "INVALID", 0.0
    dist = 0.0 if lo <= price <= hi else min(abs(price - lo), abs(price - hi))
    if dist <= 0.25 * atr or (hh is not None and hh >= lo and hl <= hi): return "AT ZONE", dist
    return ("NEAR", dist) if dist <= atr else ("FAR", dist)


def news(currencies, hours=3, impacts=("High", "Medium")):
    try:
        ev = http("https://nfs.faireconomy.media/ff_calendar_thisweek.json")
    except Exception as e:
        return {"error": str(e)}
    now = datetime.now(timezone.utc); out = []
    for e in ev:
        t = datetime.fromisoformat(e["date"]).astimezone(timezone.utc)
        if e["impact"] in impacts and e["country"] in currencies and now - timedelta(minutes=30) <= t <= now + timedelta(hours=hours):
            out.append(f'{t.astimezone(COT):%a %H:%M} COT {e["country"]} {e["impact"]}: {e["title"]} (f {e["forecast"] or "-"}, p {e["previous"] or "-"})')
    return out


def scan():
    _, cfg = find_config()
    if not cfg: raise SystemExit("CONFIG message not found in #general")
    now = datetime.now(timezone.utc)
    sess = session(now)
    setups = cfg["setups"]
    q = live([s[0] for s in setups])
    rows, alerts, signals, heads_up = [], [], [], []
    for pair, side, lo, hi, entry, sl, tp, iside, ilvl, grade in setups:
        d = q[pair]; atr = d["atr4"]
        st, dist = status(d["price"], lo, hi, atr, (iside, ilvl), d["hhi"], d["hlo"])
        pst, _ = status(d["prev"], lo, hi, atr, (iside, ilvl))
        rows.append({"pair": pair, "side": side, "grade": grade, "price": d["price"], "status": st, "prev": pst,
                     "pips_away": round(dist / pip(pair)), "zone": [lo, hi], "plan": [entry, sl, tp]})
        if RANK[st] > RANK[pst] and st != "FAR": alerts.append(rows[-1])
        if st in ("AT ZONE", "NEAR"):
            g = checklist_grade(pair, side, lo, hi, [entry, sl, tp], d["price"], all_bars(pair))
            rows[-1]["checklist"] = {"score": g["score"], "tier": g["tier"], "go": g["go"],
                                 "ticked": [x["why"] for x in g["items"] if x["ok"] and x["w"]],
                                 "missing": [x["why"] for x in g["items"] if not x["ok"] and x["w"]],
                                 "gates_failed": [x["label"] for x in g["gates"] if not x["ok"]]}
            for tf in (1, 4, 24):
                bars = candles(pair, tf, 250)
                for p in patterns(bars, side, lo, hi, atr_of(bars) or atr):
                    if p not in ENTRY_PATTERNS:  # doji, inside bar, change of character = heads-up, not an entry
                        heads_up.append({"pair": pair, "side": side, "tf": {1: "1-hour", 4: "4-hour", 24: "Daily"}[tf], "pattern": p}); continue
                    z = bars[-1]
                    stop = (max(z["h"], hi) + 0.1 * atr) if side == "SELL" else (min(z["l"], lo) - 0.1 * atr)
                    ent = d["price"]; slp = abs(ent - stop) / pip(pair)
                    signals.append({"pair": pair, "side": side, "grade": grade, "tf": {1: "1H", 4: "4H", 24: "Daily"}[tf], "pattern": p,
                                    "checklist": rows[-1]["checklist"],
                                    "candle_close_cot": datetime.fromtimestamp(z["t"] + tf * 3600, timezone.utc).astimezone(COT).strftime("%a %H:%M" if tf < 24 else "%a %b %d"),
                                    "entry_now": ent, "stop": round(stop, 5), "stop_pips": round(slp, 1),
                                    "r_levels": [round(ent + k * (ent - stop), 5) for k in (1, 2, 3)],
                                    "lots": lots(cfg, pair, grade, slp, q), "your_plan_entry_sl_tp": [entry, sl, tp]})
                    lt = signals[-1]["lots"]
                    if lt: signals[-1]["risk_usd"] = round(lt * slp * pip_value_usd(pair, q), 2)
    ccy = sorted({s[0][:3] for s in setups} | {s[0][3:] for s in setups})
    return {"now_cot": now.astimezone(COT).strftime("%a %b %d %H:%M"), "session": sess, "paused": cfg.get("paused", False),
            "expires": cfg.get("expires"), "rows": rows, "proximity_alerts": alerts, "entry_signals": signals, "heads_up": heads_up,
            "news_next_3h": news(ccy), "risk_set": bool(cfg.get("risk", {}).get("balance"))}


def trend(b, k):
    """Market structure on candle BODY CLOSES only (line-chart view, wicks ignored).
    A swing high/low is a close that stands above/below the k closes on each side.
    A close above the last swing-high close = bullish, below the last swing-low close = bearish."""
    if len(b) < 2 * k + 3: return "Not enough data"
    c = [x["c"] for x in b]
    state, last_h, last_l = "Ranging", None, None
    for i in range(len(c)):
        j = i - k
        if j >= k:
            if all(c[j] >= c[j + m] and c[j] >= c[j - m] for m in range(1, k + 1)): last_h = c[j]
            if all(c[j] <= c[j + m] and c[j] <= c[j - m] for m in range(1, k + 1)): last_l = c[j]
        if last_h is not None and c[i] > last_h: state, last_h = "Bullish", None
        if last_l is not None and c[i] < last_l: state, last_l = "Bearish", None
    return state


def all_bars(pair):
    """Every timeframe the checklist and the pattern cards need (cached per run)."""
    want = {168: 104, 24: 250, 4: 400, 2: 200, 1: 200, 0.5: 200}
    out = {}
    for tf, n in want.items():
        try: out[tf] = candles(pair, tf, n)
        except Exception: out[tf] = []
    return out


def snapshot():
    _, cfg = find_config()
    sc = scan()
    ccy = sorted({p for s in cfg["setups"] for p in (s[0][:3], s[0][3:])})
    pairs = []
    for row, s in zip(sc["rows"], cfg["setups"]):
        pair, side = s[0], s[1]
        bars = all_bars(pair)
        h1, h4, d1, w1 = bars[1], bars[4], bars[24], bars[168]
        atr = live([pair])[pair]["atr4"]
        pats = [{"tf": "1-hour", "pattern": p} for p in patterns(h1, side, s[2], s[3], atr)] + [{"tf": "4-hour", "pattern": p} for p in patterns(h4, side, s[2], s[3], atr)]
        pairs.append({**row, "invalid": {"side": s[7], "level": s[8]},
                      "trend": {"weekly": trend(w1, 1), "daily": trend(d1, 2), "h4": trend(h4, 2)}, "patterns_now": pats,
                      "cards": pattern_cards(pair, side, s[2], s[3], atr, bars),
                      "checklist": checklist_grade(pair, side, s[2], s[3], [s[4], s[5], s[6]], row["price"], bars)})
    nx = news(ccy, 48, ("High", "Medium"))
    lq = live([])
    rates = {k: lq[k]["price"] for k in ("USDJPY", "USDCAD", "USDCHF", "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD") if k in lq}
    return {"updated_utc": datetime.now(timezone.utc).isoformat(timespec="minutes"), "updated_cot": sc["now_cot"], "session": sc["session"],
            "week": cfg.get("week"), "expires": cfg.get("expires"), "paused": cfg.get("paused", False), "risk_set": sc["risk_set"],
            "pairs": pairs, "signals": sc["entry_signals"], "news": nx if isinstance(nx, list) else [], "rates": rates, "strength": strength(), "rules": cfg.get("rules", {}), "risk": cfg.get("risk", {}), "ideas": [], "open_trades": [], "watch_first": "", "notes": ""}


MAJORS = ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]
ORDER = ["EUR", "GBP", "AUD", "NZD", "USD", "CAD", "CHF", "JPY"]  # market convention for which currency is the base
PAIRS28 = [a + b for i, a in enumerate(ORDER) for b in ORDER[i + 1:]]


def strength():
    """Rank the 8 majors by average % move against the other 7 (today and this week)."""
    d = http("https://scanner.tradingview.com/forex/scan", "POST",
             {"symbols": {"tickers": [f"OANDA:{p}" for p in PAIRS28]}, "columns": ["change", "Perf.W"]})
    sc = {c: {"day": [], "week": []} for c in MAJORS}
    for r in d["data"]:
        p = r["s"].split(":")[1]; day, wk = r["d"]
        for cur, sign in ((p[:3], 1), (p[3:], -1)):
            if day is not None: sc[cur]["day"].append(sign * day)
            if wk is not None: sc[cur]["week"].append(sign * wk)
    avg = lambda v: round(sum(v) / len(v), 2) if v else 0.0
    return {"today": sorted(((c, avg(v["day"])) for c, v in sc.items()), key=lambda x: -x[1]),
            "this_week": sorted(((c, avg(v["week"])) for c, v in sc.items()), key=lambda x: -x[1])}


def touch_stats(bars, lo, hi):
    side = lambda c: "above" if c > hi else "below" if c < lo else "in"
    ov = lambda b: b["h"] >= lo and b["l"] <= hi
    visits, i = [], 0
    while i < len(bars):
        if ov(bars[i]):
            s0 = i
            while i < len(bars) and ov(bars[i]): i += 1
            before = side(bars[s0 - 1]["c"]) if s0 > 0 else "na"; after = side(bars[i]["c"]) if i < len(bars) else "live"
            visits.append((datetime.fromtimestamp(bars[s0]["t"], timezone.utc).astimezone(COT).strftime("%b %d"), before, after))
        else: i += 1
    done = [v for v in visits if v[1] in ("above", "below") and v[2] in ("above", "below")]
    return {"visits": len(visits), "held": sum(v[1] == v[2] for v in done), "broke": sum(v[1] != v[2] for v in done),
            "first": visits[0][0] if visits else None, "last": visits[-1][0] if visits else None}


def touches(pair, lo, hi):
    return {"pair": pair, "zone": [lo, hi], "daily_60d": touch_stats(candles(pair, 24, 60), lo, hi),
            "four_hour_60d": touch_stats(candles(pair, 4, 400), lo, hi),
            "weekly_2y": touch_stats(candles(pair, 168, 104), lo, hi)}


def find_zones(bars, atr, min_points=3):
    """Cluster 4-hour swing highs/lows into zones with at least min_points touches."""
    hi_i, lo_i = swings(bars, 3)
    pts = sorted([bars[i]["h"] for i in hi_i] + [bars[i]["l"] for i in lo_i])
    zones, cur = [], []
    for p in pts:
        if cur and p - cur[0] > 0.6 * atr:
            if len(cur) >= min_points: zones.append((min(cur), max(cur), len(cur)))
            cur = []
        cur.append(p)
    if len(cur) >= min_points: zones.append((min(cur), max(cur), len(cur)))
    return zones


def scout(exclude=()):
    """Off-list ideas: Weekly and Daily trend agree and price sits within 2x 4-hour ATR of a 3+ touch zone in the trend direction."""
    q = live(PAIRS28 + ["XAUUSD"]); ideas = []
    for pair in PAIRS28 + ["XAUUSD"]:
        if pair in exclude or pair not in q: continue
        try:
            w, dd, h4 = trend(candles(pair, 168, 104), 1), trend(candles(pair, 24, 60), 2), candles(pair, 4, 360)
        except Exception:
            continue
        if w != dd or w not in ("Bullish", "Bearish") or len(h4) < 60: continue
        price, atr = q[pair]["price"], q[pair]["atr4"]
        for lo, hi, n in find_zones(h4, atr):
            if w == "Bullish" and hi <= price and price - hi <= 2 * atr or w == "Bearish" and lo >= price and lo - price <= 2 * atr:
                ideas.append({"pair": pair, "side": "BUY" if w == "Bullish" else "SELL", "trend": f"Weekly {w}, Daily {dd}",
                              "zone": [round(lo, 5), round(hi, 5)], "touches_4h": n, "price": price,
                              "pips_away": round((price - hi if w == "Bullish" else lo - price) / pip(pair))})
    best = {}
    for i in ideas:  # one zone per pair: most touches, then nearest
        b = best.get(i["pair"])
        if not b or (i["touches_4h"], -i["pips_away"]) > (b["touches_4h"], -b["pips_away"]): best[i["pair"]] = i
    return sorted(best.values(), key=lambda x: (-x["touches_4h"], x["pips_away"]))[:5]


def review():
    """Sunday review data for every loaded setup: zone touches, trends, news this week, currency overlap."""
    _, cfg = find_config(); out = []
    ccy_count = {}
    for s in cfg["setups"]:
        for c in (s[0][:3], s[0][3:]): ccy_count[c] = ccy_count.get(c, 0) + 1
    try:
        ev = http("https://nfs.faireconomy.media/ff_calendar_thisweek.json")
    except Exception:
        ev = []
    for s in cfg["setups"]:
        pair = s[0]
        news_w = [f'{datetime.fromisoformat(e["date"]).astimezone(COT):%a %H:%M} {e["country"]} {e["impact"]}: {e["title"]}'
                  for e in ev if e["impact"] == "High" and e["country"] in (pair[:3], pair[3:])]
        out.append({"setup": s, "touches": touches(pair, s[2], s[3]),
                    "trend": {"weekly": trend(candles(pair, 168, 104), 1), "daily": trend(candles(pair, 24, 60), 2), "four_hour": trend(candles(pair, 4, 200), 2)},
                    "high_impact_news_this_week": news_w,
                    "shared_currency_with_other_setups": {c: ccy_count[c] - 1 for c in (pair[:3], pair[3:]) if ccy_count[c] > 1}})
    return {"setups": out, "strength": strength(), "rules": cfg.get("rules", {}), "risk": cfg.get("risk", {})}


def inbox():
    out = {}
    for name in ("ask", "log", "alerts"):
        msgs = dc(f"/channels/{CH[name]}/messages?limit=50")
        new = []
        for m in msgs:  # newest first
            if m["author"]["id"] == BOT: break
            if m["author"]["id"] == MATT and (m["content"].strip() or m.get("attachments")):
                new.append({"id": m["id"], "time_cot": datetime.fromisoformat(m["timestamp"]).astimezone(COT).strftime("%a %b %d %H:%M"), "text": m["content"],
                            "images": [a["url"] for a in m.get("attachments", []) if (a.get("content_type") or "").startswith("image")]})
        out[name] = list(reversed(new))
    return out


def tradelog(days=14):
    since = datetime.now(timezone.utc) - timedelta(days=days); out, before = [], ""
    for _ in range(5):
        msgs = dc(f"/channels/{CH['log']}/messages?limit=100" + (f"&before={before}" if before else ""))
        for m in msgs:
            t = datetime.fromisoformat(m["timestamp"])
            if t < since: return list(reversed(out))
            if m["author"]["id"] in (MATT, BOT):
                out.append({"who": "Matt" if m["author"]["id"] == MATT else "bot", "time_cot": t.astimezone(COT).strftime("%a %b %d %H:%M"), "text": m["content"]})
        if len(msgs) < 100: break
        before = msgs[-1]["id"]
    return list(reversed(out))


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "scan"
    if cmd == "scan": print(json.dumps(scan(), indent=1))
    elif cmd == "inbox": print(json.dumps(inbox(), indent=1))
    elif cmd == "log": print(json.dumps(tradelog(), indent=1))
    elif cmd == "config": print(json.dumps(find_config()[1], indent=1))
    elif cmd == "config-set": print("saved", save_config(json.load(open(sys.argv[2]))))
    elif cmd == "post": print("posted", post(sys.argv[2], open(sys.argv[3]).read()))
    elif cmd == "candles": print(json.dumps(candles(sys.argv[2], int(sys.argv[3]))[-int(sys.argv[4]):], default=str))
    elif cmd == "news-day":
        _, cfg = find_config(); ccy = sorted({p for s in cfg["setups"] for p in (s[0][:3], s[0][3:])})
        print(json.dumps(news(ccy, 24, ("High",)), indent=1))
    elif cmd == "image":
        req = urllib.request.Request(sys.argv[2], headers={"User-Agent": "DiscordBot (matt-fx-agent, 1.0)"})
        open(sys.argv[3], "wb").write(urllib.request.urlopen(req, timeout=30).read()); print("saved", sys.argv[3])
    elif cmd == "live": print(json.dumps(live(sys.argv[2:])))
    elif cmd == "strength": print(json.dumps(strength(), indent=1))
    elif cmd == "touches": print(json.dumps(touches(sys.argv[2], float(sys.argv[3]), float(sys.argv[4])), indent=1))
    elif cmd == "scout":
        _, cfg = find_config(); print(json.dumps(scout({s[0] for s in cfg["setups"]}), indent=1))
    elif cmd == "review": print(json.dumps(review(), indent=1))
    elif cmd == "snapshot":
        snap = snapshot(); json.dump(snap, open(sys.argv[2], "w")); print("wrote", sys.argv[2], len(snap["pairs"]), "pairs")
    else: raise SystemExit(__doc__)
