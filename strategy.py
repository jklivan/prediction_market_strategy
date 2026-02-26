#!/usr/bin/env python3
"""
Prediction Market → Stock Strategy
====================================
Scans Kalshi and Polymarket for big price moves, maps them to equity
themes, cross-references with news/broker signals, and outputs daily
stock BUY/SELL recommendations.

Run daily:  python3 strategy.py
"""

import os
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY = os.environ.get("PRIMARYLOGIC_API_KEY", "")
BASE_URL = "https://primarylogic--pulse-backend-external-api-app.modal.run"

# Alpaca Paper Trading
ALPACA_KEY_ID = os.environ.get("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.environ.get("APCA_API_SECRET_KEY", "")
ALPACA_BASE = "https://paper-api.alpaca.markets"
ALPACA_DATA_BASE = "https://data.alpaca.markets"

MIN_PRICE_DELTA_PCT = 8
MIN_VOLUME_POLYMARKET = 50000
LOOKBACK_HOURS = 24
MAX_PAGES = 10

# Position sizing
PORTFOLIO_ALLOCATION = 0.60     # Use 60% of portfolio for trades
MAX_POSITIONS = 10              # Max simultaneous positions
MIN_CONVICTION_TO_TRADE = 50    # Only trade conviction >= 50
MAX_HOLD_DAYS = 20              # Max days to hold a position
REVERSAL_DELTA = 8              # Price must move 8pp against entry to trigger exit
# Use persistent volume on Railway, local directory otherwise
_DATA_DIR = Path(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "")) if os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") else Path(__file__).parent
POSITIONS_FILE = _DATA_DIR / "positions.json"
OUTPUT_DIR = _DATA_DIR / "daily_reports"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_positions():
    """Load persistent position state from JSON file."""
    if POSITIONS_FILE.exists():
        try:
            return json.loads(POSITIONS_FILE.read_text())
        except (json.JSONDecodeError, IOError):
            return {"positions": {}, "last_run": None}
    return {"positions": {}, "last_run": None}


def save_positions(data):
    """Save position state to JSON file."""
    data["last_run"] = datetime.now(timezone.utc).isoformat()
    POSITIONS_FILE.write_text(json.dumps(data, indent=2, default=str))

# ── Theme → Stock Mapping ──────────────────────────────────────────────────
# Each theme maps prediction market categories/keywords to tradeable equities
# with a directional relationship (positive = market up → stock up)
THEME_MAP = {
    "fed_hawkish": {
        "keywords": ["fed funds rate", "fed decision", "fomc", "interest rate"],
        "signal": "rate expectations rising",
        "longs": [
            {"ticker": "KRE", "name": "Regional Banks ETF", "reason": "Higher rates = wider net interest margins"},
            {"ticker": "JPM", "name": "JPMorgan", "reason": "Largest US bank, benefits from steeper yield curve"},
            {"ticker": "GS", "name": "Goldman Sachs", "reason": "Trading revenue + NII boost from higher rates"},
        ],
        "shorts": [
            {"ticker": "TLT", "name": "20+ Year Treasury ETF", "reason": "Long duration bonds fall when rates rise"},
            {"ticker": "XLU", "name": "Utilities ETF", "reason": "Rate-sensitive dividend proxies sell off"},
            {"ticker": "ARKK", "name": "ARK Innovation ETF", "reason": "High-growth, long-duration equities compressed by higher discount rates"},
        ],
    },
    "fed_dovish": {
        "keywords": ["fed funds rate", "fed decision", "rate cut"],
        "signal": "rate expectations falling",
        "longs": [
            {"ticker": "TLT", "name": "20+ Year Treasury ETF", "reason": "Long bonds rally on rate cuts"},
            {"ticker": "XLU", "name": "Utilities ETF", "reason": "Yield proxies re-rate higher when rates drop"},
            {"ticker": "ARKK", "name": "ARK Innovation ETF", "reason": "Growth/duration assets benefit from lower discount rates"},
        ],
        "shorts": [
            {"ticker": "KRE", "name": "Regional Banks ETF", "reason": "NIM compression from lower rates"},
        ],
    },
    "tariff_escalation": {
        "keywords": ["tariff", "trade war", "import duties", "tariff rate", "tariff revenue"],
        "signal": "tariff expectations rising / trade war escalating",
        "longs": [
            {"ticker": "STLD", "name": "Steel Dynamics", "reason": "Domestic steel benefits from import tariffs"},
            {"ticker": "NUE", "name": "Nucor", "reason": "US steel producer shielded by tariffs"},
            {"ticker": "DBA", "name": "Agriculture ETF", "reason": "Domestic ag benefits from reduced foreign competition"},
        ],
        "shorts": [
            {"ticker": "FXI", "name": "China Large-Cap ETF", "reason": "Chinese exporters directly hit by tariffs"},
            {"ticker": "AAPL", "name": "Apple", "reason": "Heavy China supply chain exposure, tariff cost pass-through risk"},
            {"ticker": "NKE", "name": "Nike", "reason": "Manufacturing in tariff-exposed countries, margin pressure"},
            {"ticker": "WMT", "name": "Walmart", "reason": "Import-heavy inventory, cost pressures from tariffs"},
        ],
    },
    "tariff_deescalation": {
        "keywords": ["tariff", "tariff revenue"],
        "signal": "tariff expectations falling / trade tensions easing",
        "longs": [
            {"ticker": "FXI", "name": "China Large-Cap ETF", "reason": "Chinese exporters benefit from reduced tariffs"},
            {"ticker": "AAPL", "name": "Apple", "reason": "Supply chain cost relief"},
            {"ticker": "WMT", "name": "Walmart", "reason": "Lower import costs, margin expansion"},
        ],
        "shorts": [
            {"ticker": "STLD", "name": "Steel Dynamics", "reason": "Loses tariff protection on imports"},
            {"ticker": "NUE", "name": "Nucor", "reason": "Domestic premium erodes without tariffs"},
        ],
    },
    "inflation_hot": {
        "keywords": ["cpi", "inflation", "consumer price", "pce"],
        "signal": "inflation expectations rising",
        "longs": [
            {"ticker": "TIP", "name": "TIPS Bond ETF", "reason": "Inflation-protected treasuries benefit directly"},
            {"ticker": "GLD", "name": "Gold ETF", "reason": "Traditional inflation hedge"},
            {"ticker": "XLE", "name": "Energy ETF", "reason": "Commodities and energy are inflation beneficiaries"},
            {"ticker": "COST", "name": "Costco", "reason": "Pricing power + membership model insulates from inflation"},
        ],
        "shorts": [
            {"ticker": "TLT", "name": "20+ Year Treasury ETF", "reason": "Fixed coupons lose value in inflationary environment"},
        ],
    },
    "gas_prices_up": {
        "keywords": ["gas price", "gasoline", "oil price", "crude"],
        "signal": "gas/energy prices rising",
        "longs": [
            {"ticker": "XOM", "name": "Exxon Mobil", "reason": "Largest US oil major, direct revenue benefit"},
            {"ticker": "CVX", "name": "Chevron", "reason": "Integrated oil major with upstream leverage"},
            {"ticker": "OXY", "name": "Occidental Petroleum", "reason": "High operating leverage to oil prices"},
            {"ticker": "XLE", "name": "Energy ETF", "reason": "Broad energy sector exposure"},
        ],
        "shorts": [
            {"ticker": "AAL", "name": "American Airlines", "reason": "Fuel is largest cost — direct margin hit"},
            {"ticker": "DAL", "name": "Delta Air Lines", "reason": "Fuel cost headwinds compress margins"},
            {"ticker": "UBER", "name": "Uber", "reason": "Higher gas = higher driver costs, demand elasticity"},
        ],
    },
    "gas_prices_down": {
        "keywords": ["gas price", "gasoline"],
        "signal": "gas/energy prices falling",
        "longs": [
            {"ticker": "AAL", "name": "American Airlines", "reason": "Lower fuel costs = margin expansion"},
            {"ticker": "DAL", "name": "Delta Air Lines", "reason": "Fuel tailwind boosts profitability"},
            {"ticker": "UBER", "name": "Uber", "reason": "Lower driver costs, increased consumer demand"},
        ],
        "shorts": [
            {"ticker": "XOM", "name": "Exxon Mobil", "reason": "Revenue falls with oil prices"},
            {"ticker": "OXY", "name": "Occidental Petroleum", "reason": "High leverage to oil prices works both ways"},
        ],
    },
    "nuclear_bullish": {
        "keywords": ["nuclear", "reactor", "criticality", "nuclear power", "nuclear license"],
        "signal": "nuclear sector catalysts accelerating",
        "longs": [
            {"ticker": "CEG", "name": "Constellation Energy", "reason": "Largest US nuclear fleet, AI datacenter power deals"},
            {"ticker": "VST", "name": "Vistra", "reason": "Nuclear + natural gas power, AI demand beneficiary"},
            {"ticker": "CCJ", "name": "Cameco", "reason": "Largest uranium producer, benefits from nuclear expansion"},
            {"ticker": "SMR", "name": "NuScale Power", "reason": "Small modular reactor pure-play"},
            {"ticker": "OKLO", "name": "Oklo", "reason": "Advanced fission startup, Sam Altman-backed"},
        ],
        "shorts": [],
    },
    "nuclear_bearish": {
        "keywords": ["nuclear", "reactor", "criticality"],
        "signal": "nuclear sector setbacks / delays",
        "longs": [],
        "shorts": [
            {"ticker": "SMR", "name": "NuScale Power", "reason": "Pure-play most exposed to regulatory/timeline risk"},
            {"ticker": "OKLO", "name": "Oklo", "reason": "Pre-revenue, delays are existential"},
        ],
    },
    "gdp_strong": {
        "keywords": ["nominal gdp", "gdp growth", "economic growth"],
        "signal": "GDP expectations rising",
        "longs": [
            {"ticker": "SPY", "name": "S&P 500 ETF", "reason": "Broad market benefits from strong economic growth"},
            {"ticker": "IWM", "name": "Russell 2000 ETF", "reason": "Small caps have highest domestic revenue exposure"},
            {"ticker": "XLI", "name": "Industrials ETF", "reason": "Cyclicals benefit most from GDP acceleration"},
        ],
        "shorts": [
            {"ticker": "TLT", "name": "20+ Year Treasury ETF", "reason": "Strong growth = less rate cut urgency"},
        ],
    },
    "gov_shutdown": {
        "keywords": ["dhs", "government shutdown", "funded", "funding bill", "spending"],
        "signal": "government shutdown / funding disruption risk rising",
        "longs": [],
        "shorts": [
            {"ticker": "GD", "name": "General Dynamics", "reason": "Defense contractor revenue delayed by shutdowns"},
            {"ticker": "BAH", "name": "Booz Allen Hamilton", "reason": "Federal IT consulting, contract payment delays"},
            {"ticker": "LDOS", "name": "Leidos", "reason": "Government services revenue at risk during shutdowns"},
        ],
    },
    "ai_model_shift": {
        "keywords": ["ai model", "top ai", "gpt", "claude", "gemini", "grok"],
        "signal": "AI competitive landscape shifting",
        "longs": [
            {"ticker": "NVDA", "name": "NVIDIA", "reason": "All AI models need GPU compute — model competition = more training spend"},
            {"ticker": "AVGO", "name": "Broadcom", "reason": "Custom AI silicon + networking for AI clusters"},
            {"ticker": "MSFT", "name": "Microsoft", "reason": "Azure AI + OpenAI partnership + Copilot monetization"},
        ],
        "shorts": [],
    },
    "jobless_claims_up": {
        "keywords": ["jobless claims", "unemployment", "job losses", "layoffs"],
        "signal": "labor market weakening",
        "longs": [
            {"ticker": "TLT", "name": "20+ Year Treasury ETF", "reason": "Weak labor = dovish Fed = bond rally"},
            {"ticker": "XLU", "name": "Utilities ETF", "reason": "Defensive positioning + rate cut beneficiary"},
            {"ticker": "GLD", "name": "Gold ETF", "reason": "Safe haven flows in economic weakness"},
        ],
        "shorts": [
            {"ticker": "XLY", "name": "Consumer Discretionary ETF", "reason": "Weakening employment = less consumer spending"},
            {"ticker": "IWM", "name": "Russell 2000 ETF", "reason": "Small caps most exposed to domestic slowdown"},
        ],
    },
    "iran_risk": {
        "keywords": ["iran", "strait of hormuz", "middle east"],
        "signal": "geopolitical energy supply risk",
        "longs": [
            {"ticker": "XLE", "name": "Energy ETF", "reason": "Supply disruption fear premium"},
            {"ticker": "XOM", "name": "Exxon Mobil", "reason": "Oil price spike beneficiary"},
            {"ticker": "LMT", "name": "Lockheed Martin", "reason": "Defense spending catalyst"},
            {"ticker": "GLD", "name": "Gold ETF", "reason": "Geopolitical safe haven"},
        ],
        "shorts": [
            {"ticker": "AAL", "name": "American Airlines", "reason": "Fuel cost spike + travel demand risk"},
        ],
    },
    "tesla_production": {
        "keywords": ["tesla production", "tesla deliveries"],
        "signal": "Tesla production expectations shifting",
        "longs": [
            {"ticker": "TSLA", "name": "Tesla", "reason": "Direct production/delivery beat drives stock"},
        ],
        "shorts": [],
    },
    "media_ma": {
        "keywords": ["paramount", "warner", "media merger", "acquire"],
        "signal": "media M&A activity",
        "longs": [
            {"ticker": "WBD", "name": "Warner Bros Discovery", "reason": "Takeover target — M&A premium"},
            {"ticker": "PARA", "name": "Paramount Global", "reason": "Acquirer or target in media consolidation"},
        ],
        "shorts": [],
    },
    "fannie_ipo": {
        "keywords": ["fannie mae", "freddie mac", "gse", "housing finance"],
        "signal": "GSE privatization / IPO expectations",
        "longs": [
            {"ticker": "FNMA", "name": "Fannie Mae (OTC)", "reason": "Direct play on IPO/privatization"},
            {"ticker": "FMCC", "name": "Freddie Mac (OTC)", "reason": "Paired trade — both GSEs would privatize together"},
            {"ticker": "XHB", "name": "Homebuilders ETF", "reason": "GSE privatization = mortgage market changes, near-term positive for housing"},
        ],
        "shorts": [],
    },
}


def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urlencode({k: v for k, v in params.items() if v is not None})
    req = Request(url, headers={"Authorization": f"Bearer {API_KEY}"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def fetch_all_pages(path, params, max_pages=MAX_PAGES):
    all_items = []
    cursor = 0
    for _ in range(max_pages):
        p = {**params, "cursor": cursor}
        resp = api_get(path, p)
        items = resp.get("data", [])
        if not items:
            break
        all_items.extend(items)
        next_cursor = resp.get("next_cursor")
        if next_cursor is None:
            break
        cursor = next_cursor
    return all_items


# ── Parsers ─────────────────────────────────────────────────────────────────
def parse_kalshi_deltas(item):
    summary = item.get("summary", "")
    deltas = []
    pattern = r"^(.+?):\s*([\d.]+)%\s*vs\s*([\d.]+)%"
    for line in summary.split("\n"):
        m = re.match(pattern, line.strip())
        if m:
            current = float(m.group(2))
            prior = float(m.group(3))
            deltas.append({
                "outcome": m.group(1).strip(),
                "current_pct": current,
                "prior_pct": prior,
                "delta_pct": current - prior,
            })
    return deltas


def parse_kalshi_item(item):
    meta = item.get("metadata", {})
    kalshi = meta.get("kalshi_event", {})
    deltas = parse_kalshi_deltas(item)
    max_delta = max(deltas, key=lambda d: abs(d["delta_pct"]), default=None)
    return {
        "source": "Kalshi", "id": item["id"],
        "title": kalshi.get("title", item.get("summary", "")[:80]),
        "category": meta.get("category", "Unknown"),
        "event_ticker": kalshi.get("event_ticker", ""),
        "url": meta.get("url", ""),
        "is_trivial": kalshi.get("is_trivial", False),
        "deltas": deltas,
        "max_abs_delta": abs(max_delta["delta_pct"]) if max_delta else 0,
        "max_delta_detail": max_delta,
        "tickers": item.get("kg_relevant_tickers", []),
        "date": item.get("date", ""),
    }


def parse_polymarket_item(item):
    meta = item.get("metadata", {})
    poly = meta.get("polymarket", {})
    deltas_raw = poly.get("deltas", {})
    max_price_delta = abs(deltas_raw.get("max_price_delta", 0) or 0) * 100
    outcomes = poly.get("outcomes", [])
    prices = poly.get("outcome_prices", [])
    return {
        "source": "Polymarket", "id": item["id"],
        "title": meta.get("market_question", item.get("summary", "")[:80]),
        "category": meta.get("tags", ""),
        "event_slug": poly.get("event_slug", ""),
        "url": meta.get("url", ""),
        "outcomes": outcomes, "prices": prices,
        "max_abs_delta": max_price_delta,
        "volume": poly.get("volume", 0) or 0,
        "tickers": item.get("kg_relevant_tickers", []),
        "date": item.get("date", ""),
        "summary": item.get("summary", ""),
    }


# ── Theme matching ─────────────────────────────────────────────────────────
def match_themes(market):
    """Match a prediction market signal to equity themes."""
    title_lower = market["title"].lower()
    matched = []

    for theme_id, theme in THEME_MAP.items():
        for kw in theme["keywords"]:
            if kw in title_lower:
                matched.append(theme_id)
                break
    return matched


def determine_direction(market, theme_id):
    """
    Determine if the prediction market move implies the bullish or bearish
    variant of a theme. Returns 'bullish' or 'bearish'.
    """
    detail = market.get("max_delta_detail")
    if not detail:
        return None

    delta = detail["delta_pct"]
    outcome = detail["outcome"].lower()
    current = detail["current_pct"]

    # Fed rates
    if theme_id in ("fed_hawkish", "fed_dovish"):
        if "above" in outcome and delta > 0:
            return "fed_hawkish"  # Market pricing higher rates
        elif "above" in outcome and delta < 0:
            return "fed_dovish"
        elif "below" in outcome and delta > 0:
            return "fed_dovish"
        else:
            return "fed_hawkish"

    # Tariffs
    if theme_id in ("tariff_escalation", "tariff_deescalation"):
        if "above" in outcome and delta > 0:
            return "tariff_escalation"
        elif "above" in outcome and delta < 0:
            return "tariff_deescalation"
        elif delta > 0:
            return "tariff_escalation"
        else:
            return "tariff_deescalation"

    # Gas/energy
    if theme_id in ("gas_prices_up", "gas_prices_down"):
        if "above" in outcome and delta > 0:
            return "gas_prices_up"
        elif "above" in outcome and delta < 0:
            return "gas_prices_down"
        elif "below" in outcome and delta > 0:
            return "gas_prices_down"
        else:
            return "gas_prices_up"

    # CPI / inflation
    if theme_id == "inflation_hot":
        if delta > 0 and "above" in outcome:
            return "inflation_hot"
        return None

    # Nuclear
    if theme_id in ("nuclear_bullish", "nuclear_bearish"):
        # Check if criticality timelines are advancing or slipping
        if delta > 0:
            return "nuclear_bullish"  # More companies hitting milestones
        else:
            return "nuclear_bearish"

    # GDP
    if theme_id == "gdp_strong":
        if "above" in outcome and delta > 0:
            return "gdp_strong"
        return None

    # Government shutdown
    if theme_id == "gov_shutdown":
        if "before" in outcome and delta < 0:
            return "gov_shutdown"  # Funding deadline slipping = shutdown risk
        return None

    # Jobless claims
    if theme_id == "jobless_claims_up":
        if ("at least" in outcome or "above" in outcome) and delta > 0:
            return "jobless_claims_up"
        return None

    # Direct themes — just return the theme_id
    return theme_id


# ── Alpaca Paper Trading ────────────────────────────────────────────────────
def alpaca_request(method, path, body=None, data_api=False):
    """Make a request to Alpaca API."""
    base = ALPACA_DATA_BASE if data_api else ALPACA_BASE
    url = f"{base}{path}"
    data = json.dumps(body).encode() if body else None
    req = Request(url, data=data, method=method, headers={
        "APCA-API-KEY-ID": ALPACA_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
    })
    try:
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        error_body = ""
        if hasattr(e, "read"):
            error_body = e.read().decode()
        print(f"  [ALPACA ERROR] {method} {path}: {e} {error_body}")
        return None


def get_alpaca_account():
    return alpaca_request("GET", "/v2/account")


def get_alpaca_positions():
    return alpaca_request("GET", "/v2/positions") or []


def get_alpaca_orders():
    return alpaca_request("GET", "/v2/orders?status=open") or []


def close_alpaca_position(ticker):
    """Close an existing position."""
    return alpaca_request("DELETE", f"/v2/positions/{ticker}")


def get_latest_price(ticker):
    """Get latest trade price from Alpaca data API."""
    result = alpaca_request("GET", f"/v2/stocks/{ticker}/trades/latest", data_api=True)
    if result and "trade" in result:
        return result["trade"]["p"]
    return None


def submit_alpaca_order(ticker, side, notional=None, qty=None):
    """Submit a market order to Alpaca paper trading."""
    order = {
        "symbol": ticker,
        "side": side,  # "buy" or "sell"
        "type": "market",
        "time_in_force": "day",
    }
    if notional:
        order["notional"] = str(round(notional, 2))
    elif qty:
        order["qty"] = str(qty)
    return alpaca_request("POST", "/v2/orders", order)


def manage_portfolio(buys, sells, todays_markets, stock_recs, L):
    """Portfolio management with position holding up to MAX_HOLD_DAYS.

    Holds positions and only exits on signal reversal or age limit.
    Selectively rotates weaker positions for stronger candidates.
    Replaces the old execute_paper_trades (close-all / reopen daily) approach.
    """
    import time

    if not ALPACA_KEY_ID or not ALPACA_SECRET:
        L.append("")
        L.append("  [ALPACA] No API keys set — skipping portfolio management")
        return []

    account = get_alpaca_account()
    if not account:
        L.append("  [ALPACA] Could not connect to Alpaca")
        return []

    portfolio_value = float(account.get("portfolio_value", 0))
    cash = float(account.get("cash", 0))

    L.append("")
    L.append("  " + "=" * 72)
    L.append("  PORTFOLIO MANAGEMENT (Alpaca — market-neutral)")
    L.append("  " + "=" * 72)
    L.append(f"  Portfolio: ${portfolio_value:,.2f} | Cash: ${cash:,.2f}")
    L.append("")

    # ── 1. Load positions.json ──
    pos_data = load_positions()
    held_positions = pos_data.get("positions", {})

    # ── 2. Reconcile with actual Alpaca positions ──
    alpaca_positions = get_alpaca_positions()
    alpaca_held = {}
    for pos in alpaca_positions:
        sym = pos.get("symbol", "")
        alpaca_held[sym] = {
            "side": pos.get("side", "long"),
            "qty": float(pos.get("qty", 0)),
            "market_value": float(pos.get("market_value", 0)),
            "unrealized_pl": float(pos.get("unrealized_pl", 0)),
            "unrealized_plpc": float(pos.get("unrealized_plpc", 0)),
            "avg_entry_price": float(pos.get("avg_entry_price", 0)),
        }

    # First-run migration: no positions.json but Alpaca has positions
    if not held_positions and alpaca_held:
        L.append("  [MIGRATION] First run — importing existing Alpaca positions")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for sym, info in alpaca_held.items():
            side = "long" if info["side"] == "long" else "short"
            rec = stock_recs.get(sym, {})
            themes = list(set(rec.get("themes", [])))
            trigger_markets = _build_trigger_market_entries(rec.get("triggers", []))
            held_positions[sym] = {
                "side": side,
                "entry_date": today_str,
                "entry_price": info["avg_entry_price"],
                "notional": abs(info["market_value"]),
                "conviction_at_entry": rec.get("conviction", 50),
                "themes": themes,
                "trigger_markets": trigger_markets,
            }
        L.append(f"  [MIGRATION] Imported {len(alpaca_held)} positions as day-1 entries")
        L.append("")

    # Warn on mismatches and clean up stale state
    for sym in list(held_positions.keys()):
        if sym not in alpaca_held:
            L.append(f"  [WARN] {sym} in positions.json but not in Alpaca — removing from state")
    for sym in alpaca_held:
        if sym not in held_positions:
            L.append(f"  [WARN] {sym} in Alpaca but not in positions.json")

    held_positions = {sym: pos for sym, pos in held_positions.items() if sym in alpaca_held}

    # ── 5. Evaluate held positions ──
    today = datetime.now(timezone.utc).date()
    exits = {}   # ticker -> {reason, age, detail}
    kept = {}    # ticker -> position info with age_days

    for sym, pos in held_positions.items():
        entry_date = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
        age_days = (today - entry_date).days

        # 5a. Check age limit
        if age_days >= MAX_HOLD_DAYS:
            exits[sym] = {"reason": "AGED OUT", "age": age_days,
                          "detail": f"held {age_days}d (max {MAX_HOLD_DAYS})"}
            continue

        # 5b. Check signal reversal
        reversed_flag, detail = check_signal_reversal(pos, todays_markets)
        if reversed_flag:
            exits[sym] = {"reason": "REVERSED", "age": age_days, "detail": detail}
            continue

        # Surviving position
        kept[sym] = {**pos, "age_days": age_days}

    # ── 6. Build target portfolio ──
    buy_candidates = {tk: r for tk, r in buys if r["conviction"] >= MIN_CONVICTION_TO_TRADE}
    sell_candidates = {tk: r for tk, r in sells if r["conviction"] >= MIN_CONVICTION_TO_TRADE}

    long_pool = []
    short_pool = []

    # Add surviving held positions (re-score if today's data available)
    for sym, pos in kept.items():
        score = pos.get("conviction_at_entry", 50)
        if pos["side"] == "long" and sym in buy_candidates:
            score = buy_candidates[sym]["conviction"]
        elif pos["side"] == "short" and sym in sell_candidates:
            score = sell_candidates[sym]["conviction"]

        entry = {"ticker": sym, "score": score, "held": True, "position": pos}
        if pos["side"] == "long":
            long_pool.append(entry)
        else:
            short_pool.append(entry)

    # Add new candidates not already held or being exited
    for tk, r in buys:
        if r["conviction"] >= MIN_CONVICTION_TO_TRADE and tk not in kept and tk not in exits:
            long_pool.append({"ticker": tk, "score": r["conviction"], "held": False, "rec": r})

    for tk, r in sells:
        if r["conviction"] >= MIN_CONVICTION_TO_TRADE and tk not in kept and tk not in exits:
            short_pool.append({"ticker": tk, "score": r["conviction"], "held": False, "rec": r})

    # Rank and take top N from each side
    max_per_side = MAX_POSITIONS // 2
    long_pool.sort(key=lambda x: x["score"], reverse=True)
    short_pool.sort(key=lambda x: x["score"], reverse=True)

    target_longs = long_pool[:max_per_side]
    target_shorts = short_pool[:max_per_side]
    target_tickers = {e["ticker"] for e in target_longs} | {e["ticker"] for e in target_shorts}

    # ── 7. Compute diff ──
    # Held positions not in target → replaced by stronger candidates
    for sym in list(kept.keys()):
        if sym not in target_tickers:
            exits[sym] = {"reason": "REPLACED", "age": kept[sym]["age_days"],
                          "detail": "outscored by better candidate"}
            del kept[sym]

    to_close = list(exits.keys())
    to_open_long = [e for e in target_longs if not e["held"]]
    to_open_short = [e for e in target_shorts if not e["held"]]
    to_keep = [e for e in target_longs + target_shorts if e["held"]]

    # ── Report: KEPT ──
    L.append("  KEPT POSITIONS (no change):")
    if to_keep:
        for entry in to_keep:
            sym = entry["ticker"]
            pos = entry["position"]
            alpaca_info = alpaca_held.get(sym, {})
            pl = alpaca_info.get("unrealized_pl", 0)
            side_str = pos["side"].upper()
            age = pos["age_days"]
            themes_str = ", ".join(pos.get("themes", []))[:40]
            L.append(f"    {sym:<6} {side_str:<6} day {age}/{MAX_HOLD_DAYS}  "
                     f"P&L ${pl:+,.0f}  score={entry['score']}  themes: {themes_str}")
    else:
        L.append("    (none)")
    L.append("")

    # ── Report: EXITED ──
    L.append("  EXITED:")
    if exits:
        for sym, info in exits.items():
            alpaca_info = alpaca_held.get(sym, {})
            pl = alpaca_info.get("unrealized_pl", 0)
            orig_pos = held_positions.get(sym, {})
            side_str = orig_pos.get("side", "?").upper()
            reason = info["reason"]
            age = info["age"]
            detail = info["detail"]
            L.append(f"    {sym:<6} {side_str:<6} {reason:<10} held {age}d  "
                     f"P&L ${pl:+,.0f}  ({detail})")
    else:
        L.append("    (none)")
    L.append("")

    # ── 8. Execute trades ──
    executed = []

    # Close exiting positions (individual DELETE calls)
    for sym in to_close:
        result = close_alpaca_position(sym)
        if result:
            L.append(f"  CLOSED {sym}")

    if to_close:
        time.sleep(2)
        account = get_alpaca_account()
        portfolio_value = float(account.get("portfolio_value", 0))
        cash = float(account.get("cash", 0))

    # Calculate available capital for new positions
    kept_long_value = sum(
        abs(alpaca_held.get(e["ticker"], {}).get("market_value", 0))
        for e in to_keep if e["position"]["side"] == "long")
    kept_short_value = sum(
        abs(alpaca_held.get(e["ticker"], {}).get("market_value", 0))
        for e in to_keep if e["position"]["side"] == "short")

    tradeable = portfolio_value * PORTFOLIO_ALLOCATION
    half = tradeable / 2
    available_long = max(half - kept_long_value, 0)
    available_short = max(half - kept_short_value, 0)

    # ── Report: NEW ENTRIES ──
    L.append("  NEW ENTRIES:")
    new_entry_count = 0

    # Open new longs (notional orders, supports fractional)
    if to_open_long and available_long > 0:
        total_conv = sum(e["score"] for e in to_open_long) or 1
        for entry in to_open_long:
            dollars = available_long * entry["score"] / total_conv
            if dollars < 1:
                continue
            tk = entry["ticker"]
            result = submit_alpaca_order(tk, "buy", notional=dollars)
            if result and result.get("id"):
                rec = entry.get("rec", {})
                themes_str = ", ".join(set(rec.get("themes", [])))[:30]
                L.append(f"    BUY  {tk:<6} ${dollars:>8,.0f}  score={entry['score']}  "
                         f"themes: {themes_str}")
                executed.append({"action": "BUY", "ticker": tk, "notional": dollars,
                    "conviction": entry["score"], "order_id": result["id"]})
                new_entry_count += 1

                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                trigger_entries = _build_trigger_market_entries(rec.get("triggers", []))
                kept[tk] = {
                    "side": "long",
                    "entry_date": today_str,
                    "entry_price": None,
                    "notional": dollars,
                    "conviction_at_entry": entry["score"],
                    "themes": list(set(rec.get("themes", []))),
                    "trigger_markets": trigger_entries,
                    "age_days": 0,
                }
            else:
                L.append(f"    FAIL {tk:<6} — order rejected")

    # Open new shorts (qty-based, try fallback candidates if not shortable)
    if to_open_short and available_short > 0:
        total_conv = sum(e["score"] for e in to_open_short) or 1
        attempted = set()
        short_filled = 0

        # Build overflow list from all sell candidates not already held/exited
        all_sell_overflow = [
            {"ticker": tk, "score": r["conviction"], "held": False, "rec": r}
            for tk, r in sells
            if r["conviction"] >= MIN_CONVICTION_TO_TRADE
            and tk not in kept and tk not in exits
            and tk not in {e["ticker"] for e in to_open_short}
        ]
        candidates_to_try = to_open_short + all_sell_overflow

        for entry in candidates_to_try:
            if short_filled >= available_short * 0.95:
                break
            tk = entry["ticker"]
            if tk in attempted:
                continue
            attempted.add(tk)

            remaining = available_short - short_filled
            dollars = min(remaining, available_short * entry["score"] / total_conv)
            dollars = min(dollars, remaining)
            if dollars < 1:
                continue

            price = get_latest_price(tk)
            if not price or price <= 0:
                L.append(f"    SKIP {tk:<6} — no price data")
                continue
            qty = max(int(dollars / price), 1)
            actual = qty * price
            result = submit_alpaca_order(tk, "sell", qty=qty)
            if result and result.get("id"):
                rec = entry.get("rec", {})
                themes_str = ", ".join(set(rec.get("themes", [])))[:30]
                L.append(f"    SHORT {tk:<6} ${actual:>8,.0f}  score={entry['score']}  "
                         f"themes: {themes_str}")
                executed.append({"action": "SHORT", "ticker": tk, "notional": actual,
                    "conviction": entry["score"], "order_id": result["id"]})
                short_filled += actual
                new_entry_count += 1

                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                trigger_entries = _build_trigger_market_entries(rec.get("triggers", []))
                kept[tk] = {
                    "side": "short",
                    "entry_date": today_str,
                    "entry_price": price,
                    "notional": actual,
                    "conviction_at_entry": entry["score"],
                    "themes": list(set(rec.get("themes", []))),
                    "trigger_markets": trigger_entries,
                    "age_days": 0,
                }
            else:
                L.append(f"    FAIL {tk:<6} — not shortable, trying next...")

    if new_entry_count == 0:
        L.append("    (none)")
    L.append("")

    # ── Report: SUMMARY ──
    total_long = kept_long_value + sum(e["notional"] for e in executed if e["action"] == "BUY")
    total_short = kept_short_value + sum(e["notional"] for e in executed if e["action"] == "SHORT")
    net = total_long - total_short

    L.append(f"  SUMMARY:")
    L.append(f"    Long ${total_long:,.0f} / Short ${total_short:,.0f} / Net ${net:+,.0f}")
    if abs(net) > 500:
        L.append(f"    NOTE: Net exposure ${net:+,.0f} — some shorts may have failed.")
    L.append("")

    # ── 9. Save positions.json ──
    new_positions = {}
    for sym, pos in kept.items():
        new_positions[sym] = {
            "side": pos["side"],
            "entry_date": pos["entry_date"],
            "entry_price": pos.get("entry_price"),
            "notional": pos.get("notional", 0),
            "conviction_at_entry": pos.get("conviction_at_entry", 50),
            "themes": pos.get("themes", []),
            "trigger_markets": pos.get("trigger_markets", []),
        }

    save_positions({"positions": new_positions})
    L.append(f"  State saved to {POSITIONS_FILE.name}: {len(new_positions)} positions tracked")

    return executed


def _build_trigger_market_entries(triggers):
    """Convert raw market trigger objects to the format stored in positions.json."""
    entries = []
    seen = set()
    for mkt in triggers:
        mkt_id = mkt.get("id", "")
        if mkt_id in seen:
            continue
        seen.add(mkt_id)

        detail = mkt.get("max_delta_detail", {})
        if not detail:
            continue

        entry = {
            "source": mkt.get("source", ""),
            "title": mkt.get("title", ""),
            "outcome": detail.get("outcome", ""),
            "price_at_entry": detail.get("current_pct", 0),
            "delta_direction": "up" if detail.get("delta_pct", 0) > 0 else "down",
        }

        if mkt["source"] == "Kalshi":
            entry["event_ticker"] = mkt.get("event_ticker", "")
        elif mkt["source"] == "Polymarket":
            entry["event_slug"] = mkt.get("event_slug", "")

        entries.append(entry)
    return entries


def calculate_tightness(ticker, action, themes_list):
    """Calculate tightness bonus based on stock's rank within theme lists.

    1st listed stock in theme → 10 pts, 2nd → 7, 3rd → 5, 4th+ → 3.
    Sum across all pointing themes, cap at 20.
    """
    rank_points = {0: 10, 1: 7, 2: 5}
    total = 0
    for theme_id in set(themes_list):
        theme = THEME_MAP.get(theme_id)
        if not theme:
            continue
        stock_list = theme.get("longs", []) if action == "BUY" else theme.get("shorts", [])
        for idx, stock in enumerate(stock_list):
            if stock["ticker"] == ticker:
                total += rank_points.get(idx, 3)
                break
    return min(total, 20)


def check_signal_reversal(position, todays_markets):
    """Check if any trigger market has reversed against the position.

    For each stored trigger market, search today's scan for a match by
    event_ticker (Kalshi) or event_slug (Polymarket). If the same outcome's
    price has moved >= REVERSAL_DELTA in the opposite direction → reversal.

    Returns (reversed: bool, detail: str).
    """
    trigger_markets = position.get("trigger_markets", [])
    if not trigger_markets:
        return False, ""

    # Build lookup maps for today's markets
    kalshi_by_ticker = {}
    poly_by_slug = {}
    for mkt in todays_markets:
        if mkt["source"] == "Kalshi" and mkt.get("event_ticker"):
            kalshi_by_ticker[mkt["event_ticker"]] = mkt
        elif mkt["source"] == "Polymarket" and mkt.get("event_slug"):
            poly_by_slug[mkt["event_slug"]] = mkt

    for tm in trigger_markets:
        source = tm.get("source", "")
        current_price = None

        if source == "Kalshi":
            matched = kalshi_by_ticker.get(tm.get("event_ticker", ""))
            if matched:
                for d in matched.get("deltas", []):
                    if d["outcome"] == tm.get("outcome"):
                        current_price = d["current_pct"]
                        break
        elif source == "Polymarket":
            matched = poly_by_slug.get(tm.get("event_slug", ""))
            if matched:
                outcomes = matched.get("outcomes", [])
                prices = matched.get("prices", [])
                for i, o in enumerate(outcomes):
                    if o == tm.get("outcome") and i < len(prices):
                        try:
                            current_price = float(prices[i]) * 100
                        except (ValueError, TypeError):
                            pass
                        break

        if current_price is None:
            continue  # Market not in today's scan — keep holding

        price_at_entry = tm.get("price_at_entry", 0)
        delta_direction = tm.get("delta_direction", "up")
        price_change = current_price - price_at_entry

        if delta_direction == "up" and price_change <= -REVERSAL_DELTA:
            detail = (f'"{tm.get("title", "")}" {tm.get("outcome", "")} '
                      f'dropped {price_at_entry:.0f}%→{current_price:.0f}%')
            return True, detail
        elif delta_direction == "down" and price_change >= REVERSAL_DELTA:
            detail = (f'"{tm.get("title", "")}" {tm.get("outcome", "")} '
                      f'rose {price_at_entry:.0f}%→{current_price:.0f}%')
            return True, detail

    return False, ""


def get_ticker_signals(tickers, since_iso):
    """Pull corroborating signals for specific stock tickers."""
    signals = []
    for ticker in tickers[:5]:
        try:
            items = api_get(f"/v1/tickers/{ticker}/content", {
                "since": since_iso, "limit": 5,
                "min_relevance": 0.4, "min_abs_impact": 3,
                "sort_by": "abs_impact", "sort_direction": "desc",
            }).get("data", [])
            for it in items:
                ct = it.get("content", {})
                sig = it.get("ticker_signal", {})
                if ct.get("source_type") in ("Kalshi", "Polymarket"):
                    continue
                signals.append({
                    "ticker": ticker,
                    "source_type": ct.get("source_type", ""),
                    "summary": ct.get("summary", "")[:200],
                    "impact": sig.get("impact_score", 0) or 0,
                    "relevance": sig.get("relevance_score", 0) or 0,
                    "sentiment": sig.get("sentiment", ""),
                })
        except Exception:
            pass
    return signals


# ── Main ────────────────────────────────────────────────────────────────────
def run_strategy():
    if not API_KEY:
        print("ERROR: Set PRIMARYLOGIC_API_KEY environment variable")
        sys.exit(1)

    health = api_get("/v1/health")
    if health.get("data", {}).get("status") != "ok":
        print(f"API health check failed: {health}")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=LOOKBACK_HOURS)
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Scanning prediction markets... ({since_iso} → now)")

    # ── Fetch ──
    kalshi_raw = fetch_all_pages("/v1/content", {
        "source_types": "Kalshi", "since": since_iso, "limit": 100,
        "sort_by": "date", "sort_direction": "desc",
    })
    poly_raw = fetch_all_pages("/v1/content", {
        "source_types": "Polymarket", "since": since_iso, "limit": 100,
        "sort_by": "date", "sort_direction": "desc",
    })
    print(f"  Kalshi: {len(kalshi_raw)} | Polymarket: {len(poly_raw)}")

    # ── Parse & filter ──
    markets = []
    for item in kalshi_raw:
        p = parse_kalshi_item(item)
        if p["max_abs_delta"] >= MIN_PRICE_DELTA_PCT and not p["is_trivial"]:
            markets.append(p)
    for item in poly_raw:
        p = parse_polymarket_item(item)
        if p["max_abs_delta"] >= MIN_PRICE_DELTA_PCT and p["volume"] >= MIN_VOLUME_POLYMARKET:
            markets.append(p)

    print(f"  {len(markets)} markets with significant moves")

    # ── Map to equity themes ──
    # Collect: for each resolved theme, track the prediction market signals that triggered it
    theme_triggers = {}  # theme_id → list of market signals

    for mkt in markets:
        raw_themes = match_themes(mkt)
        for raw_theme in raw_themes:
            resolved = determine_direction(mkt, raw_theme)
            if resolved and resolved in THEME_MAP:
                if resolved not in theme_triggers:
                    theme_triggers[resolved] = []
                theme_triggers[resolved].append(mkt)

    print(f"  {len(theme_triggers)} equity themes triggered")

    # ── Build stock recommendations ──
    # Aggregate: for each stock, collect all themes pointing to it, and sum conviction
    stock_recs = {}  # ticker → {action, themes, total_conviction, signals}

    for theme_id, trigger_markets in theme_triggers.items():
        theme = THEME_MAP[theme_id]
        # Conviction from this theme = sum of move magnitudes from trigger markets
        theme_strength = sum(m["max_abs_delta"] for m in trigger_markets)

        for stock in theme.get("longs", []):
            tk = stock["ticker"]
            if tk not in stock_recs:
                stock_recs[tk] = {"action": "BUY", "name": stock["name"],
                    "themes": [], "total_strength": 0, "reasons": [], "triggers": []}
            stock_recs[tk]["themes"].append(theme_id)
            stock_recs[tk]["total_strength"] += theme_strength
            stock_recs[tk]["reasons"].append(stock["reason"])
            stock_recs[tk]["triggers"].extend(trigger_markets)

        for stock in theme.get("shorts", []):
            tk = stock["ticker"]
            if tk not in stock_recs:
                stock_recs[tk] = {"action": "SELL/SHORT", "name": stock["name"],
                    "themes": [], "total_strength": 0, "reasons": [], "triggers": []}
            # If already a BUY, it's conflicting — mark as MIXED
            if stock_recs[tk]["action"] == "BUY":
                stock_recs[tk]["action"] = "MIXED"
            stock_recs[tk]["themes"].append(theme_id)
            stock_recs[tk]["total_strength"] += theme_strength
            stock_recs[tk]["reasons"].append(stock["reason"])
            stock_recs[tk]["triggers"].extend(trigger_markets)

    # Score conviction 0-100
    if stock_recs:
        max_strength = max(r["total_strength"] for r in stock_recs.values())
    else:
        max_strength = 1

    for tk, rec in stock_recs.items():
        # Base: normalized strength (0-40)
        base = (rec["total_strength"] / max_strength) * 40
        # Bonus: number of distinct themes (0-25)
        theme_bonus = min(len(set(rec["themes"])) * 8, 25)
        # Bonus: number of distinct trigger markets (0-15)
        trigger_bonus = min(len(set(m["id"] for m in rec["triggers"])) * 5, 15)
        # Bonus: tightness — how closely the stock ties to the theme (0-20)
        tightness = calculate_tightness(tk, rec["action"], rec["themes"])
        rec["conviction"] = min(int(base + theme_bonus + trigger_bonus + tightness), 100)
        rec["tightness"] = tightness

    # Sort by conviction
    sorted_recs = sorted(stock_recs.items(), key=lambda x: x[1]["conviction"], reverse=True)

    # ── Get corroborating signals for top picks ──
    print("  Fetching corroborating signals for top stock picks...")
    top_tickers = [tk for tk, _ in sorted_recs[:15] if not any(c in tk for c in [" ", "."])]
    corroborating = get_ticker_signals(top_tickers, since_iso)
    corr_by_ticker = {}
    for s in corroborating:
        corr_by_ticker.setdefault(s["ticker"], []).append(s)

    # ── Build report ──
    L = []
    L.append("")
    L.append("=" * 76)
    L.append("  DAILY STOCK RECOMMENDATIONS (from Prediction Market Signals)")
    L.append(f"  {now.strftime('%Y-%m-%d %H:%M UTC')} | Window: {LOOKBACK_HOURS}h")
    L.append(f"  Scanned: {len(kalshi_raw)} Kalshi + {len(poly_raw)} Polymarket")
    L.append(f"  Themes triggered: {len(theme_triggers)} | Stocks flagged: {len(stock_recs)}")
    L.append("=" * 76)

    # ── Active themes summary ──
    L.append("")
    L.append("  ACTIVE MACRO THEMES (from prediction market moves)")
    L.append("  " + "-" * 72)
    for theme_id, triggers in sorted(theme_triggers.items(),
            key=lambda x: sum(m["max_abs_delta"] for m in x[1]), reverse=True):
        theme = THEME_MAP[theme_id]
        total_delta = sum(m["max_abs_delta"] for m in triggers)
        n = len(triggers)
        L.append(f"  [{theme_id.upper()}] {theme['signal']}")
        L.append(f"    Triggered by {n} market(s), total move magnitude: {total_delta:.0f}pp")
        for t in triggers[:3]:
            d = t.get("max_delta_detail", {})
            if d:
                L.append(f"      - {t['title'][:55]} ({d.get('delta_pct',0):+.0f}pp)")
        L.append("")

    # ── Stock recommendations ──
    buys = [(tk, r) for tk, r in sorted_recs if r["action"] == "BUY" and r["conviction"] >= 40]
    sells = [(tk, r) for tk, r in sorted_recs if r["action"] == "SELL/SHORT" and r["conviction"] >= 40]
    mixed = [(tk, r) for tk, r in sorted_recs if r["action"] == "MIXED"]

    if buys:
        L.append("  " + "=" * 72)
        L.append("  BUY RECOMMENDATIONS")
        L.append("  " + "=" * 72)
        L.append(f"  {'#':>2}  {'Conv':>4}  {'Ticker':<8}  {'Name':<25}  Themes")
        L.append("  " + "-" * 72)
        for i, (tk, r) in enumerate(buys[:20], 1):
            themes_short = ", ".join(set(r["themes"]))[:30]
            L.append(f"  {i:>2}  {r['conviction']:>4}  {tk:<8}  {r['name'][:25]:<25}  {themes_short}")

        L.append("")
        for i, (tk, r) in enumerate(buys[:10], 1):
            L.append(f"  {'━' * 72}")
            conv_bar = "█" * (r["conviction"] // 10) + "░" * (10 - r["conviction"] // 10)
            L.append(f"  BUY #{i}: {tk} ({r['name']})  |  Conviction: {r['conviction']}/100 [{conv_bar}]")
            L.append("")
            L.append("  WHY:")
            for reason in list(dict.fromkeys(r["reasons"]))[:3]:
                L.append(f"    - {reason}")
            L.append("")
            L.append("  PREDICTION MARKET TRIGGERS:")
            seen = set()
            for t in r["triggers"]:
                if t["id"] in seen:
                    continue
                seen.add(t["id"])
                d = t.get("max_delta_detail", {})
                if d:
                    L.append(f"    [{t['source']}] {t['title'][:55]}")
                    L.append(f"      {d.get('outcome','')}: {d.get('prior_pct',0):.0f}% → {d.get('current_pct',0):.0f}% ({d.get('delta_pct',0):+.0f}pp)")
            L.append("")

            # Corroborating signals
            sigs = corr_by_ticker.get(tk, [])
            if sigs:
                L.append("  CORROBORATING SIGNALS:")
                for s in sigs[:3]:
                    sent_icon = {"positive": "+", "negative": "-"}.get(s["sentiment"], "~")
                    L.append(f"    [{sent_icon}] {s['source_type']} (impact={s['impact']}): {s['summary'][:100]}")
                L.append("")

    if sells:
        L.append("  " + "=" * 72)
        L.append("  SELL / SHORT RECOMMENDATIONS")
        L.append("  " + "=" * 72)
        L.append(f"  {'#':>2}  {'Conv':>4}  {'Ticker':<8}  {'Name':<25}  Themes")
        L.append("  " + "-" * 72)
        for i, (tk, r) in enumerate(sells[:20], 1):
            themes_short = ", ".join(set(r["themes"]))[:30]
            L.append(f"  {i:>2}  {r['conviction']:>4}  {tk:<8}  {r['name'][:25]:<25}  {themes_short}")

        L.append("")
        for i, (tk, r) in enumerate(sells[:10], 1):
            L.append(f"  {'━' * 72}")
            conv_bar = "█" * (r["conviction"] // 10) + "░" * (10 - r["conviction"] // 10)
            L.append(f"  SELL #{i}: {tk} ({r['name']})  |  Conviction: {r['conviction']}/100 [{conv_bar}]")
            L.append("")
            L.append("  WHY:")
            for reason in list(dict.fromkeys(r["reasons"]))[:3]:
                L.append(f"    - {reason}")
            L.append("")
            L.append("  PREDICTION MARKET TRIGGERS:")
            seen = set()
            for t in r["triggers"]:
                if t["id"] in seen:
                    continue
                seen.add(t["id"])
                d = t.get("max_delta_detail", {})
                if d:
                    L.append(f"    [{t['source']}] {t['title'][:55]}")
                    L.append(f"      {d.get('outcome','')}: {d.get('prior_pct',0):.0f}% → {d.get('current_pct',0):.0f}% ({d.get('delta_pct',0):+.0f}pp)")
            L.append("")

            sigs = corr_by_ticker.get(tk, [])
            if sigs:
                L.append("  CORROBORATING SIGNALS:")
                for s in sigs[:3]:
                    sent_icon = {"positive": "+", "negative": "-"}.get(s["sentiment"], "~")
                    L.append(f"    [{sent_icon}] {s['source_type']} (impact={s['impact']}): {s['summary'][:100]}")
                L.append("")

    if mixed:
        L.append("")
        L.append("  CONFLICTING SIGNALS (do not trade):")
        for tk, r in mixed:
            L.append(f"    {tk} ({r['name']}) — conflicting themes: {', '.join(set(r['themes']))}")

    if not buys and not sells:
        L.append("")
        L.append("  No actionable stock trades today — prediction market moves")
        L.append("  did not map to clear equity themes.")

    # ── Manage portfolio (hold positions, rotate selectively) ──
    executed = manage_portfolio(buys, sells, markets, stock_recs, L)

    # ── Footer ──
    L.append("")
    L.append("=" * 76)
    L.append("  HOW TO READ THIS REPORT")
    L.append("  " + "-" * 72)
    L.append("  1. Prediction markets moved → we detected a macro theme shift")
    L.append("  2. Theme maps to stocks that benefit (BUY) or suffer (SELL)")
    L.append("  3. Conviction = strength (0-40) + themes (0-25) + triggers (0-15) + tightness (0-20)")
    L.append("  4. Conviction 70+: Strong. 50-69: Moderate. 40-49: Speculative.")
    L.append("  5. MIXED = conflicting signals from different themes — avoid.")
    L.append(f"  6. Positions held up to {MAX_HOLD_DAYS} days. Exits on signal reversal ({REVERSAL_DELTA}pp) or age limit.")
    L.append("  7. KEPT = still held from prior day. EXITED = closed today. NEW = opened today.")
    L.append("=" * 76)
    L.append("  DISCLAIMER: Decision-support context only. Not financial advice.")
    L.append("  You are responsible for your own trades and risk management.")
    L.append("=" * 76)

    report = "\n".join(L)
    print(report)

    date_str = now.strftime("%Y-%m-%d")
    (OUTPUT_DIR / f"trades_{date_str}.txt").write_text(report)
    (OUTPUT_DIR / f"trades_{date_str}.json").write_text(json.dumps({
        "generated_at": now.isoformat(),
        "config": {"lookback_hours": LOOKBACK_HOURS, "min_delta_pct": MIN_PRICE_DELTA_PCT},
        "scanned": {"kalshi": len(kalshi_raw), "polymarket": len(poly_raw)},
        "themes_triggered": list(theme_triggers.keys()),
        "buys": [{"ticker": tk, "conviction": r["conviction"], "name": r["name"],
                   "themes": list(set(r["themes"])), "reasons": r["reasons"]}
                 for tk, r in buys],
        "sells": [{"ticker": tk, "conviction": r["conviction"], "name": r["name"],
                    "themes": list(set(r["themes"])), "reasons": r["reasons"]}
                  for tk, r in sells],
        "executed_trades": executed,
    }, indent=2, default=str))
    print(f"\nSaved to: {OUTPUT_DIR / f'trades_{date_str}.txt'}")


if __name__ == "__main__":
    try:
        run_strategy()
    except Exception as e:
        print(f"FATAL: {e}")
        sys.exit(1)
    sys.exit(0)
