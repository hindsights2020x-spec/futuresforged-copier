#!/usr/bin/env python3
"""
FuturesForged — Trade Copier backend  (Handoff #28, retail entry-tier client)

Serves the REST API that copier_dashboard.html polls/posts to, on http://localhost:7332.
Implements the full #28 API contract. Copy fan-out + ratio + instrument override +
emergency flatten + TradingView webhook are ALL implemented here.

>>> THE ONLY THING TO WIRE: submit_order() / flatten_account() — the live broker calls.
    By default they SIMULATE (return a fake fill) so the whole app runs and is testable.
    Code/Tom replaces the body of those two functions with the real NT8/broker submit and
    verifies against real accounts. Everything else is done.

Run:  python3 copier_server.py        (state persists to copier_state.json)
"""
import json, os, threading, random, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("COPIER_PORT", "7332"))
STATE_FILE = os.environ.get("COPIER_STATE", "copier_state.json")
DASHBOARD = os.environ.get("COPIER_DASHBOARD", "copier_dashboard.html")  # served at / if present
_LOCK = threading.RLock()

# --- Live-execution gating (see EXECUTION INTERFACE block below) ------------
# Real broker submits are OFF unless COPIER_LIVE=1. Even when on, an order only
# routes to a real account if an explicit route exists for its copier key in
# COPIER_LIVE_ROUTES (default: NONE -> every account fails closed).
COPIER_LIVE = os.environ.get("COPIER_LIVE", "") == "1"
# JSON map: { "<copier_account_key>": {"connector": "rithmic", "broker_account_id": "..."} }
# Empty by default -> no account has a live route -> nothing can submit for real.
try:
    COPIER_LIVE_ROUTES = json.loads(os.environ.get("COPIER_LIVE_ROUTES", "") or "{}")
except Exception:
    COPIER_LIVE_ROUTES = {}
# Broker account IDs the copier must NEVER touch (ff-bot owns these; a second
# session on the same credentials would fight the running bot over a funded
# account). Hard block regardless of routing config.
COPIER_BLOCKED_BROKER_ACCOUNTS = {
    a.strip() for a in os.environ.get("COPIER_BLOCKED_BROKER_ACCOUNTS", "229107").split(",") if a.strip()
}


# --- Chart bars proxy (Handoff #30) ----------------------------------------
# The copier has no market-data feed of its own; the live Rithmic 1m bars live
# on ff-bot (:7331). We source them through the read-only view (:7333), which
# proxies /api/bars and is gated by a STATIC, reboot-safe DASHBOARD_VIEW_TOKEN
# (no rotating session token needed). 1m bars are aggregated to the requested
# interval here. Upstream has NQ/ES buffers only -> micros map to their mini
# (MNQ->NQ, MES->ES), matching the Handoff #29 L2 mapping.
BARS_SOURCE  = os.environ.get("BARS_SOURCE_URL", "http://127.0.0.1:7333")
BOT_ENV_PATH = os.environ.get("BOT_ENV_PATH", "/home/tom/futuresforged-bot/.env")
_BARS_ROOT_MAP = {"NQ": "NQ", "MNQ": "NQ", "ES": "ES", "MES": "ES"}

def _bars_token():
    """Static view token: copier env first, else parse ff-bot's .env."""
    t = os.environ.get("DASHBOARD_VIEW_TOKEN", "").strip()
    if t:
        return t
    try:
        with open(BOT_ENV_PATH) as f:
            for line in f:
                s = line.strip()
                if s.startswith("DASHBOARD_VIEW_TOKEN="):
                    return s.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""

def fetch_bars(root, interval):
    """Proxy ff-bot's live 1m Rithmic bars (via the :7333 read-only view) and
    aggregate to `interval` minutes. Returns Handoff #30 shape: list of
    {time(epoch s), open, high, low, close, volume}, oldest->newest.
    NOTE: upstream 1m buffer raised to 3000 (Handoff #30) -> a 5m chart can hold
    up to ~600 bars, meeting the spec's 300-500. The buffer fills live (no
    historical backfill), so depth accrues over ~1-2 trading days after a bot
    restart; deep instant history would still need a Rithmic historical pull."""
    root = (root or "NQ").upper()
    sym  = _BARS_ROOT_MAP.get(root, "NQ")
    try:
        interval = max(1, int(interval))
    except (TypeError, ValueError):
        interval = 5
    qs  = urllib.parse.urlencode({"symbol": sym, "tf": "1m", "n": "3000",
                                  "k": _bars_token()})
    url = f"{BARS_SOURCE}/api/bars?{qs}"
    with urllib.request.urlopen(url, timeout=4) as r:
        raw = json.loads(r.read() or b"{}")
    span, buckets, order = interval * 60, {}, []
    for b in raw.get("bars", []):
        try:
            epoch = int(datetime.fromisoformat(b["t"]).timestamp())
            o, h, l, c = float(b["o"]), float(b["h"]), float(b["l"]), float(b["c"])
            v = int(b.get("v", 0) or 0)
        except Exception:
            continue
        key = (epoch // span) * span
        agg = buckets.get(key)
        if agg is None:
            buckets[key] = {"time": key, "open": o, "high": h,
                            "low": l, "close": c, "volume": v}
            order.append(key)
        else:
            agg["high"]   = max(agg["high"], h)
            agg["low"]    = min(agg["low"], l)
            agg["close"]  = c
            agg["volume"] += v
    return [buckets[k] for k in sorted(order)]


def fetch_book(root):
    """Phase 1 (#1) — proxy ff-bot's live L2 order book via the :7333 read-only view,
    so the copier is a single front-door for the client's order-flow panel. Returns the
    bot's /api/book shape UNCHANGED: {bids:[[px,sz]...], asks:[...], mid} — or the bot's
    {ok:false, message:...} when the depth digester (SENSORY_V2_DEPTH) is off. Micros map
    to their full-size book (MNQ->NQ, MES->ES), matching the bars/L2 mapping above."""
    root = (root or "NQ").upper()
    sym  = _BARS_ROOT_MAP.get(root, "NQ")
    qs   = urllib.parse.urlencode({"symbol": sym, "k": _bars_token()})
    url  = f"{BARS_SOURCE}/api/book?{qs}"
    with urllib.request.urlopen(url, timeout=4) as r:
        return json.loads(r.read() or b"{}")


# --- Market synopsis proxy (Handoff #53) -----------------------------------
# The synopsis is generated on ff-bot (:7331) from the engine's in-process
# state. Unlike bars/L2 we do NOT route this through the public :7333 view:
# :7333 is internet-exposed behind a static view token, and /synopsis spends
# real API budget per cache miss. We go straight to :7331 (loopback, tailnet-
# gated) and authenticate with the bot's STATIC RECORDER_AUTH_TOKEN, which
# bot_engine accepts as X-Bot-Token (bypassing the browser Basic-Auth prompt).
SYNOPSIS_SOURCE = os.environ.get("SYNOPSIS_SOURCE_URL", "http://127.0.0.1:7331")

def _bot_token():
    """Static bot API token: copier env first, else parse ff-bot's .env
    (same resolution order as _bars_token)."""
    t = os.environ.get("RECORDER_AUTH_TOKEN", "").strip()
    if t:
        return t
    try:
        with open(BOT_ENV_PATH) as f:
            for line in f:
                s = line.strip()
                if s.startswith("RECORDER_AUTH_TOKEN="):
                    return s.split("=", 1)[1].strip().strip('"').strip("'")
    except Exception:
        pass
    return ""

def fetch_synopsis(symbol, tf, entitlement=""):
    """Proxy ff-bot's /synopsis. Returns the bot's JSON body UNCHANGED so the
    client renders it as-is; on transport failure returns a shaped error
    rather than raising. Micros are passed through — the bot maps them."""
    params = {"symbol": (symbol or "MNQ").upper(), "tf": (tf or "5m").lower()}
    if entitlement:
        params["entitlement"] = entitlement
    url = f"{SYNOPSIS_SOURCE}/synopsis?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"X-Bot-Token": _bot_token()})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read() or b"{}"), 200
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read() or b"{}"), e.code
        except Exception:
            return {"ok": False, "error": "upstream_error",
                    "reason": f"http_{e.code}"}, e.code
    except Exception as e:
        return {"ok": False, "error": "upstream_unreachable",
                "reason": str(e)[:200]}, 502


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def hhmmss():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Default state (replace accounts/instruments with the real set, or load from file)
# ---------------------------------------------------------------------------
DEFAULT_STATE = {
    "master": "tom_cash",
    "emergency": False,
    "followers": {
        "mffu_rapid_50k":   {"enabled": True,  "ratio": 1.0, "instrument_override": None},
        "mffu_builder_50k": {"enabled": False, "ratio": 1.0, "instrument_override": None},
    },
    "accounts": {
        "tom_cash":         {"label": "TOM — CASH ACCOUNT", "credential_set": "personal", "live_balance": 24810, "rules": {"plan": "AMP", "stage": "Live"}},
        "mffu_rapid_50k":   {"label": "MFFU RAPID 50K",      "credential_set": "mffu",     "live_balance": 51240, "rules": {"plan": "Rapid", "stage": "Stage 1"}},
        "mffu_builder_50k": {"label": "MFFU BUILDER 50K",    "credential_set": "mffu",     "live_balance": 50000, "rules": {"plan": "Builder", "stage": "Stage 1"}},
    },
    "instruments": {
        "NQ":  {"name": "Nasdaq Futures", "category": "Futures", "tick": 0.25, "tick_value": 5.0},
        "ES":  {"name": "S&P 500 Futures", "category": "Futures", "tick": 0.25, "tick_value": 12.5},
        "MNQ": {"name": "Micro Nasdaq",    "category": "Micro",   "tick": 0.25, "tick_value": 0.5},
        "MES": {"name": "Micro S&P",       "category": "Micro",   "tick": 0.25, "tick_value": 1.25},
    },
    "orders": [],
    "positions": {},   # account_key -> {qty(signed), avg, instrument} for per-trade P&L
    # Handoff #31 — per-account kill switch + daily loss/win limits. A missing
    # entry == active with no limits (preserves prior behavior). kill_switch
    # True=active / False=halted. Enforced server-side as an EXTRA fail-closed
    # gate before any order routes to an account (see _route_to / _account_active).
    "account_controls": {},  # account_key -> {kill_switch, max_loss, max_win, day_pnl, day_key, halted_reason}
    # Phase 1 — copier trading groups. Each: {id, name, enabled, master(account_key|None),
    # subs:{account_key:{ratio}}, signals:[tag,...]}. A group fans a master's order out to
    # its subs (copy-trade); place_order routes by group_id. Empty by default.
    "groups": [],
    # Phase 1 — per-account signal routing config (dashboard GROUPS tab). Config storage
    # only for now; signal-DRIVEN auto-routing (strategy tag -> group) is not yet wired.
    "signal_accounts": {},  # account_key -> {enabled, contracts, signals:[]}
    "log": [{"ts": hhmmss(), "level": "INFO", "msg": "Session started"}],
}


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            s.setdefault("accounts", DEFAULT_STATE["accounts"])
            # Instruments are STATIC contract specs, not user state — nothing
            # mutates them at runtime. They must come from DEFAULT_STATE every
            # start, not from the persisted file: setdefault only fills a MISSING
            # key, so a spec baked into an old copier_state.json would override
            # the source forever. That is exactly how the MES tick value stayed
            # at 0.625 (half the real $/point) after the source was corrected —
            # the fix looked deployed and wasn't. Overwriting here also repairs
            # any state file already carrying the wrong numbers.
            s["instruments"] = json.loads(json.dumps(DEFAULT_STATE["instruments"]))
            s.setdefault("orders", []); s.setdefault("log", []); s.setdefault("positions", {})
            s.setdefault("account_controls", {})  # Handoff #31
            s.setdefault("groups", [])            # Phase 1 — copier groups
            s.setdefault("signal_accounts", {})   # Phase 1 — signal-account routing config
            return s
        except Exception:
            pass
    return json.loads(json.dumps(DEFAULT_STATE))

STATE = load_state()

def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(STATE, f, indent=2)
    except Exception as e:
        log(f"state save failed: {e}", "WARN")

def log(msg, level="INFO"):
    STATE["log"].insert(0, {"ts": hhmmss(), "level": level, "msg": msg})
    STATE["log"] = STATE["log"][:200]


# ===========================================================================
# >>> EXECUTION INTERFACE — wire these two to the real broker / NT8.  <<<
#     Default = simulation. Return {"ok": bool, "msg": str}.
# ===========================================================================
#
# WIRING NOTE (Handoff #28): the handoff specified the "NT8/NT-TCP connector
# path", but NT8 is down on this host (NT8_MODE=remote, bridge offline) and live
# execution runs through Rithmic (rithmic_connector.py). The live path below is
# therefore wired to Rithmic, not NT8 — the faithful adaptation. It is FAIL-CLOSED:
# real submits require COPIER_LIVE=1 AND an explicit per-account route, and the
# bot's funded account (229107) is hard-blocked. As shipped, with no routes
# configured, every account simulates. See report for live-enable prerequisites.

_RITHMIC = None  # lazily-created singleton; only built when a real route is used

def _resolve_route(account_key):
    """Return (broker_account_id, err). err is a user-safe string or None."""
    route = COPIER_LIVE_ROUTES.get(account_key)
    if not route:
        return None, f"no live route configured for '{account_key}'"
    bid = str(route.get("broker_account_id", "")).strip()
    if not bid:
        return None, f"route for '{account_key}' missing broker_account_id"
    if bid in COPIER_BLOCKED_BROKER_ACCOUNTS:
        return None, f"broker account {bid} is blocked (in use by ff-bot)"
    if (route.get("connector") or "rithmic") != "rithmic":
        return None, f"unsupported connector '{route.get('connector')}' for '{account_key}'"
    return bid, None

def _rithmic():
    """Lazily import + connect the Rithmic connector. Only reached for a valid,
    non-blocked route. Never imported/connected in SIM."""
    global _RITHMIC
    if _RITHMIC is None:
        import sys
        sys.path.insert(0, os.path.expanduser("~/futuresforged-bot"))
        from rithmic_connector import RithmicConnector
        c = RithmicConnector()
        c.connect()
        _RITHMIC = c
    return _RITHMIC

def submit_order(account_key, order):
    """Place ONE order on ONE account. order = {instrument,direction,order_type,contracts,limit_price,stop_price,trail_pts}.
    SIM by default; real Rithmic submit only when COPIER_LIVE=1 and a route exists."""
    if not COPIER_LIVE:
        # --- SIMULATION (default) ---
        # Fill near the caller-supplied reference price (live mid from Chart Studio)
        # so SIM P&L is realistic; fall back to a static level if none provided.
        try:    ref = float(order.get("ref_price") or 0)
        except (TypeError, ValueError): ref = 0
        base = ref if ref > 0 else 21450.0
        px = round(base + random.uniform(-0.5, 0.5), 2)
        return {"ok": True, "msg": f"Filled {order['contracts']}x @ {px} (SIM)", "fill_price": px}
    # --- LIVE (gated, fail-closed) ---
    bid, err = _resolve_route(account_key)
    if err:
        log(f"submit blocked [{account_key}]: {err}", "WARN")
        return {"ok": False, "msg": err}
    try:
        rc = _rithmic()
        action = "BUY" if str(order.get("direction", "")).upper() in ("LONG", "BUY") else "SELL"
        qty = int(order["contracts"])
        instrument = order["instrument"]
        otype = str(order.get("order_type", "Market")).lower().replace(" ", "")
        if otype in ("market",):
            r = rc.place_market(instrument, action, qty, account=bid)
        elif otype in ("limit",):
            r = rc.place_limit(instrument, action, qty, float(order["limit_price"]), account=bid)
        elif otype in ("stop", "stopmarket"):
            r = rc.place_stop_market(instrument, action, qty, float(order["stop_price"]), account=bid)
        else:
            # Fail closed — never silently downgrade stoplimit/trailing to a market order.
            msg = f"unsupported order_type '{order.get('order_type')}' for live submit"
            log(f"submit blocked [{account_key}]: {msg}", "WARN")
            return {"ok": False, "msg": msg}
        ok = bool(r.get("ok"))
        return {"ok": ok, "msg": (f"Submitted {qty}x {instrument} {action} (LIVE {bid})" if ok
                                  else f"Broker rejected order on {bid}")}
    except Exception as e:
        log(f"submit error [{account_key}]: {e}", "ERROR")
        return {"ok": False, "msg": f"submit error: {e}"}

def flatten_account(account_key):
    """Flatten ALL positions on ONE account. SIM by default; real Rithmic flatten
    only when COPIER_LIVE=1 and a route exists."""
    if not COPIER_LIVE:
        # --- SIMULATION (default) ---
        return {"ok": True, "msg": "Flattened (SIM)"}
    # --- LIVE (gated, fail-closed) ---
    bid, err = _resolve_route(account_key)
    if err:
        log(f"flatten blocked [{account_key}]: {err}", "WARN")
        return {"ok": False, "msg": err}
    try:
        ok = bool(_rithmic().flatten(account=bid))
        return {"ok": ok, "msg": (f"Flattened {bid} (LIVE)" if ok else f"Flatten failed on {bid}")}
    except Exception as e:
        log(f"flatten error [{account_key}]: {e}", "ERROR")
        return {"ok": False, "msg": f"flatten error: {e}"}
# ===========================================================================


def _acct_short(key):
    lbl = STATE["accounts"].get(key, {}).get("label", key)
    return lbl.split("—")[-1].strip() if "—" in lbl else lbl


def _point_value(instrument):
    """$ per 1.0 point for an instrument = tick_value / tick."""
    inst = STATE.get("instruments", {}).get(instrument) or {}
    tick = inst.get("tick") or 0.25
    tv   = inst.get("tick_value") or 0.0
    return (tv / tick) if tick else 0.0


def _apply_fill(account_key, instrument, side_sign, qty, price):
    """Update an account's net position and return the REALIZED P&L for the
    portion of this fill that closes existing position (per-trade P&L).
    Opening/adding returns 0.0. Handles partial closes and reversals."""
    positions = STATE.setdefault("positions", {})
    pos = positions.get(account_key)
    if not pos or pos.get("instrument") != instrument or pos.get("qty", 0) == 0:
        pos = {"qty": 0, "avg": 0.0, "instrument": instrument}  # fresh / flat / switched symbol
    cur, pv, realized = pos["qty"], _point_value(instrument), None
    if cur == 0 or (cur > 0) == (side_sign > 0):
        # same direction (or flat) -> add; weighted-average the entry. No closed trade.
        denom = abs(cur) + qty
        pos["avg"] = ((pos["avg"] * abs(cur) + price * qty) / denom) if denom else price
    else:
        # opposite direction -> close the overlap (that's a completed trade)
        closing  = min(qty, abs(cur))
        realized = (price - pos["avg"]) * (1 if cur > 0 else -1) * closing * pv
        if (cur + side_sign * qty) == 0:
            pos["avg"] = 0.0
        elif ((cur + side_sign * qty) > 0) != (cur > 0):
            pos["avg"] = price          # flipped past flat -> new position at this price
    pos["qty"] = cur + side_sign * qty
    pos["instrument"] = instrument
    positions[account_key] = pos
    return None if realized is None else round(realized, 2)


def _fill_pnl(account_key, ord_dict, r):
    """Realized per-trade P&L for one filled order, or None if no fill price."""
    if not (r.get("ok") and r.get("fill_price") is not None):
        return None
    sgn = 1 if str(ord_dict.get("direction", "")).upper() in ("LONG", "BUY") else -1
    return _apply_fill(account_key, ord_dict["instrument"], sgn,
                       int(ord_dict["contracts"]), r["fill_price"])


# ---------------------------------------------------------------------------
# Handoff #31 — per-account kill switch + daily loss/win limits
# ---------------------------------------------------------------------------
# These add an EXTRA, fail-closed gate IN FRONT of submit_order(): an account
# that is killed or has breached its daily loss/win cap is skipped before the
# (separately gated) live/SIM execution path is even reached. They never touch
# COPIER_LIVE, the route map, or COPIER_BLOCKED_BROKER_ACCOUNTS.
def _today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _get_controls(account_key):
    """Controls dict for an account, creating a default (active, no limits) entry
    on first use. Rolls the day over — zeroes day P&L and clears an AUTO halt
    (max-loss / target) — when the UTC date changes. A manual OFF is preserved."""
    ac = STATE.setdefault("account_controls", {})
    c = ac.get(account_key)
    if c is None:
        c = {"kill_switch": True, "max_loss": 0.0, "max_win": 0.0,
             "day_pnl": 0.0, "day_key": _today(), "halted_reason": None}
        ac[account_key] = c
    if c.get("day_key") != _today():
        c["day_key"] = _today()
        c["day_pnl"] = 0.0
        if c.get("halted_reason") in ("MAX LOSS", "TARGET HIT"):
            c["kill_switch"] = True
            c["halted_reason"] = None
    return c

def _account_active(account_key):
    """(active, reason). Missing controls -> active. Kill switch OFF -> halted."""
    c = _get_controls(account_key)
    if not c.get("kill_switch", True):
        return False, (c.get("halted_reason") or "OFF")
    return True, None

def _record_pnl_and_check(account_key, pnl):
    """Add realized per-trade P&L to the account's running day total and auto-halt
    (flip kill switch OFF) if a daily loss/win cap is hit or exceeded."""
    if pnl is None:
        return
    c = _get_controls(account_key)
    c["day_pnl"] = round(c.get("day_pnl", 0.0) + float(pnl), 2)
    max_loss = float(c.get("max_loss") or 0)
    max_win  = float(c.get("max_win") or 0)
    if c.get("kill_switch", True) and max_loss > 0 and c["day_pnl"] <= -max_loss:
        c["kill_switch"] = False; c["halted_reason"] = "MAX LOSS"
        log(f"{_acct_short(account_key)} HALTED — max daily loss hit (${c['day_pnl']})", "WARN")
    elif c.get("kill_switch", True) and max_win > 0 and c["day_pnl"] >= max_win:
        c["kill_switch"] = False; c["halted_reason"] = "TARGET HIT"
        log(f"{_acct_short(account_key)} HALTED — daily target hit (${c['day_pnl']})", "WARN")

def _route_to(account_key, order):
    """Per-account gate, then submit. Returns (result, realized_pnl). A killed or
    limit-breached account is skipped WITHOUT reaching submit_order()."""
    active, reason = _account_active(account_key)
    if not active:
        log(f"Order not routed to {_acct_short(account_key)} — halted ({reason})", "WARN")
        return {"ok": False, "msg": f"halted ({reason}) — not routed"}, None
    r = submit_order(account_key, order)
    pnl = _fill_pnl(account_key, order, r)
    _record_pnl_and_check(account_key, pnl)
    return r, pnl


# ---------------------------------------------------------------------------
# Phase 1 — copier trading groups (helpers). A group fans a master's order out to
# its subs (copy-trade). CRUD routes live in do_POST; routing lives in place_order.
# ---------------------------------------------------------------------------
def _new_group_id():
    existing = {g.get("id") for g in STATE.get("groups", [])}
    while True:
        gid = "g_" + format(random.randint(0, 0xffffff), "06x")
        if gid not in existing:
            return gid

def _find_group(gid):
    for g in STATE.get("groups", []):
        if g.get("id") == gid:
            return g
    return None


# ---------------------------------------------------------------------------
# Order-input normalization (durable boundary fix — ports futuresforged-bot PR #9
# into the copier that actually serves :7332). Every order-placing client speaks a
# different vocabulary: Chart Studio sends BUY/SELL + MKT/LMT/STP, the copier
# dashboard sends LONG/SHORT + full names, TradingView varies. submit_order() keys
# off direction in ("LONG","BUY") and order_type "market"/"limit"/"stopmarket", so
# an unmapped value silently mis-fires (a "MKT" fell through to "unsupported
# order_type" and failed closed; a bad direction defaulted to SELL). Canonicalize
# once here so no client can drift the contract, and fail LOUD (HTTP 400) instead
# of the old silent ok:true. No sizing change — `contracts` is placed verbatim and
# ratio'd per follower downstream, exactly as before.
# ---------------------------------------------------------------------------
_DIRECTION_ALIASES = {
    "BUY": "LONG",  "B": "LONG",  "LONG": "LONG",  "L": "LONG",
    "SELL": "SHORT", "S": "SHORT", "SHORT": "SHORT",
}
_ORDER_TYPE_ALIASES = {
    "MKT": "Market", "MARKET": "Market",
    "LMT": "Limit", "LIMIT": "Limit",
    "STP": "Stop Market", "STOP": "Stop Market", "STOP MARKET": "Stop Market", "STOPMARKET": "Stop Market",
    "STPLMT": "Stop Limit", "STOP LIMIT": "Stop Limit", "STOPLIMIT": "Stop Limit",
    "TRAIL": "Trailing Stop", "TRAILING": "Trailing Stop", "TRAILING STOP": "Trailing Stop", "TRAILINGSTOP": "Trailing Stop",
}

def canon_direction(value):
    """Map any accepted direction alias to 'LONG'/'SHORT'; None if unrecognised."""
    return _DIRECTION_ALIASES.get(str(value or "").strip().upper())

def canon_order_type(value):
    """Map any accepted order-type alias to a canonical value; None if unrecognised."""
    return _ORDER_TYPE_ALIASES.get(str(value or "").strip().upper())

def normalize_order(order):
    """Canonicalize + validate an inbound order at the boundary. Returns
    (normalized_order, None) on success or (None, error_msg) on bad input so the
    caller can fail loud (HTTP 400). Accepts Chart Studio's `side` (BUY/SELL) as
    well as `direction`. Does NOT size: `contracts` is coerced to a positive int
    and placed verbatim (per-follower ratio is applied later in place_order)."""
    o = dict(order)
    direction = canon_direction(o.get("direction") or o.get("side"))
    if direction is None:
        return None, f"Invalid direction: {(o.get('direction') or o.get('side'))!r}"
    order_type = canon_order_type(o.get("order_type", "Market"))
    if order_type is None:
        return None, f"Invalid order type: {o.get('order_type')!r}"
    try:
        contracts = max(1, int(o.get("contracts", 1)))
    except (TypeError, ValueError):
        return None, f"Invalid contracts: {o.get('contracts')!r}"
    limit_price = o.get("limit_price")
    stop_price  = o.get("stop_price")
    if order_type in ("Limit", "Stop Limit") and not limit_price:
        return None, "Limit price required"
    if order_type in ("Stop Market", "Stop Limit") and not stop_price:
        return None, "Stop price required"
    if order_type == "Trailing Stop" and not o.get("trail_pts"):
        return None, "Trail points required"
    o["direction"]  = direction
    o["order_type"] = order_type
    o["contracts"]  = contracts
    o["instrument"] = str(o.get("instrument") or "NQ").upper()
    return o, None


def place_order(order):
    """Place on master, then copy to each enabled follower applying ratio + instrument override.
    Expects a boundary-normalized order (see normalize_order); re-normalizes defensively so a
    direct/legacy caller can't inject a raw vocabulary and mis-fire."""
    with _LOCK:
        order, err = normalize_order(order)
        if err:
            log(f"Order rejected — {err}", "WARN")
            return {"ok": False, "results": [], "msg": err}
        if STATE.get("emergency"):
            log("Order blocked — emergency stop active", "WARN")
            return {"ok": False, "results": [], "msg": "emergency active"}

        # Routing target: a specific group (group_id) fans out to that group's master
        # + subs; otherwise the global master + enabled followers (legacy behavior).
        gid = order.get("group_id")
        if gid:
            g = _find_group(gid)
            if not g:
                log(f"Order rejected — unknown group '{gid}'", "WARN")
                return {"ok": False, "results": [], "msg": f"unknown group '{gid}'"}
            if not g.get("enabled", True):
                log(f"Order blocked — group '{g.get('name', gid)}' disabled", "WARN")
                return {"ok": False, "results": [], "msg": f"group '{g.get('name', gid)}' disabled"}
            master = g.get("master")
            if not master:
                return {"ok": False, "results": [], "msg": f"group '{g.get('name', gid)}' has no master"}
            follower_items = list((g.get("subs") or {}).items())   # subs: {key: {ratio}}
            ctx = f"group '{g.get('name', gid)}'"
        else:
            master = STATE["master"]
            follower_items = [(k, c) for k, c in STATE["followers"].items() if c.get("enabled")]
            ctx = "global"

        results = []
        r, m_pnl = _route_to(master, order)
        results.append({"role": "master", "account": _acct_short(master), "ok": r["ok"], "msg": r["msg"], "pnl": m_pnl})
        for fk, fcfg in follower_items:
            contracts = max(1, round(order["contracts"] * float(fcfg.get("ratio", 1.0))))
            forder = dict(order)
            forder["contracts"] = contracts
            if fcfg.get("instrument_override"):
                forder["instrument"] = fcfg["instrument_override"]
            r, f_pnl = _route_to(fk, forder)
            results.append({"role": "follower", "account": _acct_short(fk), "ok": r["ok"], "msg": r["msg"], "pnl": f_pnl})
        STATE["orders"].insert(0, {
            "dt": now_iso(), "instrument": order["instrument"], "direction": order["direction"],
            "order_type": order["order_type"], "contracts": order["contracts"],
            "group_id": gid,
            "pnl": m_pnl,   # per-trade realized P&L (master): null on opening orders, $ on closing trades
            "results": results,
        })
        STATE["orders"] = STATE["orders"][:100]
        log(f"Order placed ({ctx}): {order['direction']} {order['contracts']}x {order['instrument']}")
        save_state()
        return {"ok": True, "results": results}


def emergency_flatten():
    with _LOCK:
        results = []
        for key in STATE["accounts"]:
            r = flatten_account(key)
            results.append({"account": _acct_short(key), "ok": r["ok"], "msg": r["msg"]})
        STATE["emergency"] = True
        log("EMERGENCY FLATTEN — all accounts", "WARN")
        save_state()
        return {"ok": True, "results": results}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body=None, ctype="application/json"):
        data = (json.dumps(body) if body is not None and ctype == "application/json" else (body or b"")).encode() if not isinstance(body, bytes) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

    def log_message(self, *a):  # quiet
        pass

    def do_OPTIONS(self):
        self._send(204, b"")

    def do_GET(self):
        if self.path.startswith("/api/state"):
            with _LOCK:
                self._send(200, STATE)
        elif self.path.startswith("/api/bars"):
            # Handoff #30 — Rithmic-backed bars (proxied from ff-bot via :7333).
            q        = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            root     = (q.get("root", ["NQ"])[0] or "NQ").upper()
            interval = q.get("interval", ["5"])[0]
            try:
                bars = fetch_bars(root, interval)
                self._send(200, {"root": root, "interval": str(interval),
                                 "bars": bars, "count": len(bars),
                                 "source": "ff-bot:1m-agg"})
            except Exception as e:
                self._send(200, {"root": root, "interval": str(interval),
                                 "bars": [], "count": 0, "error": str(e)})
        elif self.path.startswith("/api/book"):
            # Phase 1 (#1) — live L2 order book, proxied from ff-bot via :7333. Accepts
            # ?symbol= (client Chart Studio) or ?root= (dashboard). Returns the bot's
            # /api/book shape unchanged so the client's renderL2() works as-is.
            q    = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            root = (q.get("symbol", q.get("root", ["NQ"]))[0] or "NQ").upper()
            try:
                self._send(200, fetch_book(root))
            except Exception as e:
                self._send(200, {"ok": False, "bids": [], "asks": [], "error": str(e)})
        elif self.path.startswith("/api/synopsis"):
            # Handoff #53 — AI market synopsis, proxied from ff-bot :7331.
            # Accepts ?symbol= (client Chart Studio) or ?root= (dashboard),
            # plus an optional ?entitlement= b64-JSON from the licensing layer.
            q    = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            sym  = (q.get("symbol", q.get("root", ["MNQ"]))[0] or "MNQ").upper()
            tf   = (q.get("tf", ["5m"])[0] or "5m")
            ent  = (q.get("entitlement", [""])[0] or "")
            body, status = fetch_synopsis(sym, tf, ent)
            self._send(status if status in (200, 400, 429, 503) else 200, body)
        elif self.path in ("/", "/index.html") and os.path.exists(DASHBOARD):
            with open(DASHBOARD, "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        p, b = self.path, self._body()
        with _LOCK:
            if p.startswith("/api/set_master"):
                if b.get("account_key") in STATE["accounts"]:
                    STATE["master"] = b["account_key"]; log(f"Master set: {_acct_short(b['account_key'])}"); save_state()
                    return self._send(200, {"ok": True})
                return self._send(400, {"ok": False, "msg": "unknown account"})
            if p.startswith("/api/set_follower"):
                k = b.get("account_key")
                if k:
                    STATE["followers"].setdefault(k, {"enabled": False, "ratio": 1.0, "instrument_override": None})
                    STATE["followers"][k].update({
                        "enabled": bool(b.get("enabled", False)),
                        "ratio": float(b.get("ratio", 1.0)),
                        "instrument_override": b.get("instrument_override"),
                    })
                    log(f"Follower {_acct_short(k)}: {'ON' if b.get('enabled') else 'OFF'} x{b.get('ratio',1.0)}"); save_state()
                    return self._send(200, {"ok": True})
                return self._send(400, {"ok": False})
            if p.startswith("/api/clear_emergency"):
                STATE["emergency"] = False; log("Emergency cleared"); save_state()
                return self._send(200, {"ok": True})
            # Handoff #31 — save kill switch + daily loss/win limits for one account
            if p.startswith("/api/copier/account_controls"):
                k = b.get("account_id") or b.get("account_key")
                if not k or k not in STATE["accounts"]:
                    return self._send(400, {"ok": False, "msg": "unknown account"})
                c = _get_controls(k)
                if "kill_switch" in b:
                    c["kill_switch"] = bool(b["kill_switch"])
                    # Manual toggle: ON clears any halt reason; OFF marks it manual
                    c["halted_reason"] = None if c["kill_switch"] else "MANUAL"
                for fld in ("max_loss", "max_win"):
                    if fld in b:
                        try:
                            c[fld] = max(0.0, float(b.get(fld) or 0))
                        except (TypeError, ValueError):
                            pass
                log(f"Controls {_acct_short(k)}: kill={'ON' if c['kill_switch'] else 'OFF'} "
                    f"maxLoss=${c['max_loss']:.0f} maxWin=${c['max_win']:.0f}")
                save_state()
                return self._send(200, {"ok": True, "controls": c})
            # Handoff #31 — clear halt state + zero day P&L, re-enable the account
            if p.startswith("/api/copier/reset_day"):
                k = b.get("account_id") or b.get("account_key")
                if not k or k not in STATE["accounts"]:
                    return self._send(400, {"ok": False, "msg": "unknown account"})
                c = _get_controls(k)
                c["day_pnl"] = 0.0; c["day_key"] = _today()
                c["halted_reason"] = None; c["kill_switch"] = True
                log(f"Reset day {_acct_short(k)} — halt cleared, re-enabled")
                save_state()
                return self._send(200, {"ok": True, "controls": c})
            # ── Phase 1 — copier trading groups (create / delete / configure) ──
            if p.startswith("/api/add_group"):
                gid = _new_group_id()
                name = (b.get("name") or f"Group {len(STATE.get('groups', [])) + 1}").strip() or "Group"
                g = {"id": gid, "name": name, "enabled": True,
                     "master": None, "subs": {}, "signals": []}
                STATE.setdefault("groups", []).append(g)
                log(f"Group created: {name}"); save_state()
                return self._send(200, {"ok": True, "id": gid, "group": g})
            if p.startswith("/api/remove_group"):
                gid = b.get("id")
                before = len(STATE.get("groups", []))
                STATE["groups"] = [g for g in STATE.get("groups", []) if g.get("id") != gid]
                if len(STATE["groups"]) == before:
                    return self._send(400, {"ok": False, "msg": "unknown group"})
                log(f"Group removed: {gid}"); save_state()
                return self._send(200, {"ok": True})
            # NOTE: the specific set_group_* routes MUST precede the generic set_group
            # below — startswith("/api/set_group") would otherwise swallow them.
            if p.startswith("/api/set_group_master"):
                g = _find_group(b.get("id"))
                if not g: return self._send(400, {"ok": False, "msg": "unknown group"})
                k = b.get("account_key")
                if k not in STATE["accounts"]:
                    return self._send(400, {"ok": False, "msg": "unknown account"})
                g["master"] = k; log(f"Group {g['name']} master: {_acct_short(k)}"); save_state()
                return self._send(200, {"ok": True, "group": g})
            if p.startswith("/api/set_group_sub"):
                g = _find_group(b.get("id"))
                if not g: return self._send(400, {"ok": False, "msg": "unknown group"})
                k = b.get("account_key")
                if not k: return self._send(400, {"ok": False, "msg": "missing account_key"})
                subs = g.setdefault("subs", {})
                if b.get("remove"):
                    subs.pop(k, None); log(f"Group {g['name']} sub removed: {_acct_short(k)}")
                else:
                    if k not in STATE["accounts"]:
                        return self._send(400, {"ok": False, "msg": "unknown account"})
                    try: ratio = float(b.get("ratio", 1.0))
                    except (TypeError, ValueError): ratio = 1.0
                    subs[k] = {"ratio": ratio}
                    log(f"Group {g['name']} sub {_acct_short(k)} x{ratio}")
                save_state(); return self._send(200, {"ok": True, "group": g})
            if p.startswith("/api/set_group_signals"):
                g = _find_group(b.get("id"))
                if not g: return self._send(400, {"ok": False, "msg": "unknown group"})
                sigs = b.get("signals")
                g["signals"] = list(sigs) if isinstance(sigs, list) else []
                save_state(); return self._send(200, {"ok": True, "group": g})
            if p.startswith("/api/set_group"):
                g = _find_group(b.get("id"))
                if not g: return self._send(400, {"ok": False, "msg": "unknown group"})
                if b.get("name") is not None:
                    g["name"] = str(b["name"]).strip() or g["name"]
                if "enabled" in b:
                    g["enabled"] = bool(b["enabled"])
                log(f"Group updated: {g['name']} (enabled={g.get('enabled', True)})"); save_state()
                return self._send(200, {"ok": True, "group": g})
            # Signal-account config storage (dashboard GROUPS tab). Persist only —
            # signal-DRIVEN auto-routing is not yet wired (needs the bot->copier feed).
            if p.startswith("/api/set_signal_account"):
                k = b.get("account_key")
                if not k: return self._send(400, {"ok": False, "msg": "missing account_key"})
                sa = STATE.setdefault("signal_accounts", {})
                cfg = sa.setdefault(k, {"enabled": False, "contracts": 1, "signals": []})
                if "enabled" in b: cfg["enabled"] = bool(b["enabled"])
                if "contracts" in b:
                    try: cfg["contracts"] = max(1, int(b["contracts"]))
                    except (TypeError, ValueError): pass
                if isinstance(b.get("signals"), list): cfg["signals"] = list(b["signals"])
                save_state(); return self._send(200, {"ok": True, "config": cfg})
            # ── Phase 2 — bot -> copier account sync (bot = source of truth) ──
            # The bot discovers accounts + refreshes balances (it holds the broker
            # sessions; the copier must not double-connect — single-session rule) and
            # pushes them here. DISPLAY/AVAILABILITY sync only: execution routing stays
            # separately gated (COPIER_LIVE + COPIER_LIVE_ROUTES + blocked-broker list).
            if p.startswith("/api/sync_accounts_from_bot"):
                incoming = b.get("accounts")
                if not isinstance(incoming, dict):
                    return self._send(400, {"ok": False, "msg": "accounts dict required"})
                accts = STATE.setdefault("accounts", {})
                # Bot is truth for its OWN accounts: drop bot-sourced entries no longer
                # pushed. Copier-only accounts (source != 'bot') are preserved.
                for k in [k for k, v in list(accts.items())
                          if (v or {}).get("source") == "bot" and k not in incoming]:
                    accts.pop(k, None)
                for k, v in incoming.items():
                    v = v or {}
                    entry = accts.get(k, {})
                    entry.update({
                        "label":          v.get("label", entry.get("label", k)),
                        "credential_set": v.get("credential_set", entry.get("credential_set", "")),
                        "live_balance":   v.get("live_balance", entry.get("live_balance")),
                        "rules":          v.get("rules", entry.get("rules", {})),
                        "source":         "bot",
                        "synced_at":      now_iso(),
                    })
                    if "enabled" in v:
                        entry["enabled"] = bool(v["enabled"])
                    accts[k] = entry
                log(f"Accounts synced from bot: {len(incoming)}")
                save_state()
                return self._send(200, {"ok": True, "count": len(incoming)})
        if p.startswith("/api/place_order"):
            norm, err = normalize_order(b)
            if err:
                # Fail LOUD — the old handler returned ok:true even when nothing
                # executed, so bad input (e.g. Chart Studio "MKT") was invisible.
                return self._send(400, {"ok": False, "results": [], "msg": err})
            return self._send(200, place_order(norm))
        if p.startswith("/api/emergency"):
            return self._send(200, emergency_flatten())
        if p.startswith("/webhook/tradingview"):
            log("TradingView webhook received")
            norm, err = normalize_order(b)
            if err:
                return self._send(400, {"ok": False, "results": [], "msg": err})
            return self._send(200, place_order(norm))
        self._send(404, {"error": "not found"})


if __name__ == "__main__":
    print(f"FuturesForged Copier server on http://localhost:{PORT}  (state: {STATE_FILE})")
    if COPIER_LIVE:
        n = len(COPIER_LIVE_ROUTES)
        print(f"EXECUTION = LIVE (COPIER_LIVE=1). {n} route(s) configured; "
              f"blocked broker accounts: {sorted(COPIER_BLOCKED_BROKER_ACCOUNTS)}.")
        print("  Accounts without an explicit route fail closed (no real submit).")
    else:
        print("EXECUTION = SIMULATION (default). Set COPIER_LIVE=1 + COPIER_LIVE_ROUTES to enable real submits.")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
