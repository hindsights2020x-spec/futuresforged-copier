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
import json, os, threading, random, urllib.request, urllib.parse
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


# ---------------------------------------------------------------------------
# Real accounts — pulled live from the bot (option A)
# ---------------------------------------------------------------------------
# The copier has no broker connection of its own for account discovery. The bot
# (:7331) discovers the customer's real accounts; its read-only view (:7333,
# same static DASHBOARD_VIEW_TOKEN we already use for bars) proxies /api/state,
# which carries each account's key + label (financials are redacted there — we
# only need identity/label). We cache the result and refresh on a short TTL so
# the group UI always shows the customer's real accounts, never placeholders.
_ACCTS_TTL_SEC = 20
_accts_cache = {"at": 0.0, "data": None}

def _now_epoch():
    return datetime.now(timezone.utc).timestamp()

def fetch_bot_accounts():
    """GET the bot's accounts from the read-only view. Returns {key: {...}} or None."""
    qs  = urllib.parse.urlencode({"k": _bars_token()})
    url = f"{BARS_SOURCE}/api/state?{qs}"
    with urllib.request.urlopen(url, timeout=4) as r:
        data = json.loads(r.read() or b"{}")
    raw = data.get("accounts") or {}
    out = {}
    for key, a in raw.items():
        if not isinstance(a, dict):
            continue
        out[key] = {
            "label":          a.get("label", key),
            "credential_set": a.get("credential_set") or a.get("connector") or "account",
            "live_balance":   a.get("live_balance") or a.get("balance") or 0,
            "rules":          a.get("rules") or {},
        }
    return out or None

def refresh_accounts(force=False):
    """Refresh STATE['accounts'] from the bot (cached, TTL-bounded). Never raises."""
    now = _now_epoch()
    if not force and _accts_cache["data"] is not None and (now - _accts_cache["at"]) < _ACCTS_TTL_SEC:
        return
    try:
        accts = fetch_bot_accounts()
    except Exception as e:
        if _accts_cache["data"] is None:
            log(f"account fetch failed (bot :7333 view): {e}", "WARN")
        _accts_cache["at"] = now
        return
    _accts_cache["at"] = now
    if accts is None:
        return
    changed = accts != _accts_cache["data"]
    _accts_cache["data"] = accts
    with _LOCK:
        STATE["accounts"] = accts
        # prune group references to accounts that no longer exist
        for g in STATE.get("groups", []):
            if g.get("master") not in accts:
                g["master"] = None
            g["subs"] = {k: v for k, v in (g.get("subs") or {}).items() if k in accts}
    if changed:
        log(f"accounts refreshed from bot: {len(accts)} account(s)")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def hhmmss():
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


# ---------------------------------------------------------------------------
# Default state (replace accounts/instruments with the real set, or load from file)
# ---------------------------------------------------------------------------
# Copy-trading is organised into GROUPS the customer creates: each group has ONE
# master account and any number of sub accounts. A trade on the master copies to
# every enabled sub, scaled by the sub's ratio (ratio is the only per-sub setting).
#
#   group = {"id": "g1", "name": "Group 1", "enabled": True,
#            "master": <account_key|None>, "subs": {<account_key>: {"ratio": 1.0}}}
#
# Accounts are NO LONGER hardcoded — they are pulled live from the bot's real,
# broker-discovered set via the read-only view (:7333), see refresh_accounts().
DEFAULT_STATE = {
    "emergency": False,
    "groups": [],            # list of group dicts (see above)
    "next_group": 1,         # monotonic counter for group ids
    "accounts": {},          # {account_key: {label, credential_set, live_balance, rules}} — from the bot
    "instruments": {
        "NQ":  {"name": "Nasdaq Futures", "category": "Futures", "tick": 0.25, "tick_value": 5.0},
        "ES":  {"name": "S&P 500 Futures", "category": "Futures", "tick": 0.25, "tick_value": 12.5},
        "MNQ": {"name": "Micro Nasdaq",    "category": "Micro",   "tick": 0.25, "tick_value": 0.5},
        "MES": {"name": "Micro S&P",       "category": "Micro",   "tick": 0.25, "tick_value": 0.625},
    },
    "orders": [],
    "positions": {},   # account_key -> {qty(signed), avg, instrument} for per-trade P&L
    # Handoff #31 — per-account kill switch + daily loss/win limits. A missing
    # entry == active with no limits (preserves prior behavior). kill_switch
    # True=active / False=halted. Enforced server-side as an EXTRA fail-closed
    # gate before any order routes to an account (see _route_to / _account_active).
    "account_controls": {},  # account_key -> {kill_switch, max_loss, max_win, day_pnl, day_key, halted_reason}
    "log": [{"ts": hhmmss(), "level": "INFO", "msg": "Session started"}],
}


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            s.setdefault("instruments", DEFAULT_STATE["instruments"])
            s.setdefault("accounts", {})
            s.setdefault("groups", [])
            s.setdefault("next_group", len(s.get("groups", [])) + 1)
            s.setdefault("orders", []); s.setdefault("log", []); s.setdefault("positions", {})
            s.setdefault("account_controls", {})  # Handoff #31
            # Drop the retired flat master/followers model if an old state file has it.
            s.pop("master", None); s.pop("followers", None)
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


_ORDER_TYPE_ALIASES = {
    "MKT": "Market", "MARKET": "Market", "LMT": "Limit", "LIMIT": "Limit",
    "STP": "Stop Market", "STOP": "Stop Market", "STOP MARKET": "Stop Market",
    "STOPLIMIT": "Stop Limit", "STOP LIMIT": "Stop Limit",
    "TRAIL": "Trailing Stop", "TRAILING STOP": "Trailing Stop",
}

def _normalize_order(b):
    """Build a clean order dict, accepting both the copier UI's fields
    (direction/order_type full names) and Chart Studio's (side/MKT codes)."""
    direction = str(b.get("direction") or b.get("side") or "LONG").upper()
    if direction == "BUY":  direction = "LONG"
    if direction == "SELL": direction = "SHORT"
    ot = b.get("order_type", "Market")
    order_type = _ORDER_TYPE_ALIASES.get(str(ot).strip().upper(), ot)
    order = {
        "instrument":  (b.get("instrument") or "NQ").upper(),
        "direction":   direction,
        "order_type":  order_type,
        "contracts":   max(1, int(b.get("contracts", 1) or 1)),
    }
    for fld in ("limit_price", "stop_price", "trail_pts", "ref_price"):
        if b.get(fld) not in (None, ""):
            order[fld] = b[fld]
    return order


def get_group(group_id):
    for g in STATE.get("groups", []):
        if g.get("id") == group_id:
            return g
    return None


def _record_order(gname, order, results, m_pnl):
    STATE["orders"].insert(0, {
        "dt": now_iso(), "group": gname,
        "instrument": order["instrument"], "direction": order["direction"],
        "order_type": order["order_type"], "contracts": order["contracts"],
        "pnl": m_pnl,   # per-trade realized P&L (master): null on opening, $ on closing
        "results": results,
    })
    STATE["orders"] = STATE["orders"][:100]


def place_order_group(group, order):
    """Place on the group's master, then copy to each sub scaled by ratio (same
    instrument as the master — subs carry ratio only)."""
    with _LOCK:
        if STATE.get("emergency"):
            log("Order blocked — emergency stop active", "WARN")
            return {"ok": False, "results": [], "msg": "emergency active"}
        master = group.get("master")
        if not master or master not in STATE["accounts"]:
            return {"ok": False, "results": [], "msg": "group has no valid master"}
        gname = group.get("name", group.get("id"))
        results = []
        r, m_pnl = _route_to(master, order)
        results.append({"group": gname, "role": "master", "account": _acct_short(master),
                        "ok": r["ok"], "msg": r["msg"], "pnl": m_pnl, "contracts": order["contracts"]})
        for sk, scfg in (group.get("subs") or {}).items():
            if sk == master or sk not in STATE["accounts"]:
                continue
            contracts = max(1, round(order["contracts"] * float(scfg.get("ratio", 1.0))))
            sorder = dict(order); sorder["contracts"] = contracts
            r, s_pnl = _route_to(sk, sorder)
            results.append({"group": gname, "role": "sub", "account": _acct_short(sk),
                            "ok": r["ok"], "msg": r["msg"], "pnl": s_pnl, "contracts": contracts})
        _record_order(gname, order, results, m_pnl)
        log(f"[{gname}] {order['direction']} {order['contracts']}x {order['instrument']}")
        save_state()
        return {"ok": True, "results": results}


def place_order_account(account_key, order):
    """Place ONE order on ONE account (Chart Studio picks a specific account)."""
    with _LOCK:
        if STATE.get("emergency"):
            return {"ok": False, "results": [], "msg": "emergency active"}
        if account_key not in STATE["accounts"]:
            return {"ok": False, "results": [], "msg": "unknown account"}
        r, pnl = _route_to(account_key, order)
        results = [{"group": "—", "role": "master", "account": _acct_short(account_key),
                    "ok": r["ok"], "msg": r["msg"], "pnl": pnl, "contracts": order["contracts"]}]
        _record_order("direct", order, results, pnl)
        log(f"[direct] {_acct_short(account_key)} {order['direction']} {order['contracts']}x {order['instrument']}")
        save_state()
        return {"ok": True, "results": results}


def place_order_all(order):
    """Fan a signal out to EVERY enabled group with a master (webhook path)."""
    active = [g for g in STATE.get("groups", []) if g.get("enabled") and g.get("master")]
    if not active:
        log("Webhook signal ignored — no enabled group with a master", "WARN")
        return {"ok": False, "results": [], "msg": "no enabled group with a master"}
    all_results = []
    for g in active:
        all_results += place_order_group(g, order).get("results", [])
    return {"ok": True, "results": all_results}


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
            refresh_accounts()          # pull the customer's real accounts from the bot (TTL-cached)
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
        elif self.path in ("/", "/index.html") and os.path.exists(DASHBOARD):
            with open(DASHBOARD, "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        p, b = self.path, self._body()
        with _LOCK:
            # ── Trading groups ────────────────────────────────────────────
            if p.startswith("/api/add_group"):
                name = (b.get("name") or "").strip() or f"Group {STATE['next_group']}"
                gid  = f"g{STATE['next_group']}"; STATE["next_group"] += 1
                STATE["groups"].append({"id": gid, "name": name, "enabled": True, "master": None, "subs": {}})
                log(f"Group created: {name}"); save_state()
                return self._send(200, {"ok": True, "id": gid})
            if p.startswith("/api/remove_group"):
                before = len(STATE["groups"])
                STATE["groups"] = [g for g in STATE["groups"] if g.get("id") != b.get("id")]
                if len(STATE["groups"]) < before:
                    log(f"Group removed: {b.get('id')}"); save_state()
                    return self._send(200, {"ok": True})
                return self._send(404, {"ok": False, "msg": "group not found"})
            if p.startswith("/api/set_group_master"):
                g = get_group(b.get("id"))
                if not g: return self._send(404, {"ok": False, "msg": "group not found"})
                k = b.get("account_key") or None
                if k and k not in STATE["accounts"]:
                    return self._send(400, {"ok": False, "msg": "unknown account"})
                g["master"] = k
                if k: g["subs"].pop(k, None)
                log(f"Group '{g['name']}' master: {_acct_short(k) if k else '—'}"); save_state()
                return self._send(200, {"ok": True})
            if p.startswith("/api/set_group_sub"):
                g = get_group(b.get("id"))
                if not g: return self._send(404, {"ok": False, "msg": "group not found"})
                k = b.get("account_key")
                if not k or k not in STATE["accounts"]:
                    return self._send(400, {"ok": False, "msg": "unknown account"})
                if b.get("remove"):
                    g["subs"].pop(k, None); log(f"Group '{g['name']}' sub removed: {_acct_short(k)}")
                elif k == g.get("master"):
                    return self._send(400, {"ok": False, "msg": "master cannot be its own sub"})
                else:
                    ratio = float(b.get("ratio", g["subs"].get(k, {}).get("ratio", 1.0)))
                    g["subs"][k] = {"ratio": max(0.1, ratio)}
                    log(f"Group '{g['name']}' sub {_acct_short(k)}: ratio={ratio}x")
                save_state()
                return self._send(200, {"ok": True})
            if p.startswith("/api/set_group"):   # meta: name / enabled
                g = get_group(b.get("id"))
                if not g: return self._send(404, {"ok": False, "msg": "group not found"})
                if "name" in b:    g["name"] = (b.get("name") or g["name"]).strip() or g["name"]
                if "enabled" in b: g["enabled"] = bool(b.get("enabled"))
                log(f"Group '{g['name']}': enabled={g['enabled']}"); save_state()
                return self._send(200, {"ok": True})
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
        if p.startswith("/api/place_order"):
            # Normalise the order: accept side/BUY-SELL + MKT/LMT/STP aliases (Chart Studio)
            order = _normalize_order(b)
            account_key = b.get("account_key")
            if account_key and not b.get("group_id"):
                return self._send(200, place_order_account(account_key, order))
            g = get_group(b.get("group_id"))
            if not g:
                return self._send(400, {"ok": False, "msg": "unknown group"})
            return self._send(200, place_order_group(g, order))
        if p.startswith("/api/emergency"):
            return self._send(200, emergency_flatten())
        if p.startswith("/webhook/tradingview"):
            log("TradingView webhook received")
            return self._send(200, place_order_all(_normalize_order(b)))
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
    # Pull the customer's real accounts from the bot, and seed a starter group
    # on first run so the dashboard isn't empty.
    refresh_accounts(force=True)
    if not STATE["groups"]:
        gid = f"g{STATE['next_group']}"; STATE["next_group"] += 1
        STATE["groups"].append({"id": gid, "name": "Group 1", "enabled": True, "master": None, "subs": {}})
        save_state()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
