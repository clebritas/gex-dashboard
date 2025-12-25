# app.py
import os
import time
import requests
import pandas as pd
import streamlit as st

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# -----------------------
# Page
# -----------------------
st.set_page_config(page_title="0DTE Absolute GEX by Strike", layout="wide")
st.title("0DTE Absolute GEX by Strike (Abs(CallGEX) + Abs(PutGEX))")

NY = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()

DEFAULT_TTL = 60
DEFAULT_UNDERLYING = "SPY"


# -----------------------
# Time helpers (ET)
# -----------------------
def et_now() -> datetime:
    return datetime.now(NY)

def et_today() -> date:
    return et_now().date()

def is_weekend(d: date) -> bool:
    return d.weekday() >= 5

def prev_weekday(d: date) -> date:
    d -= timedelta(days=1)
    while is_weekend(d):
        d -= timedelta(days=1)
    return d

def last_weekday(d: date) -> date:
    while is_weekend(d):
        d -= timedelta(days=1)
    return d


# -----------------------
# Polygon helpers
# -----------------------
def _polygon_get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=30)
    if r.status_code in (401, 403):
        raise PermissionError(f"{r.status_code} {r.text}")
    r.raise_for_status()
    return r.json()

def polygon_market_is_open_best_effort() -> bool | None:
    """Returns True/False if accessible, else None."""
    if not POLYGON_API_KEY:
        return None
    try:
        data = _polygon_get("https://api.polygon.io/v1/marketstatus/now", {"apiKey": POLYGON_API_KEY})
        m = (data.get("market") or "").lower()
        if m in ("open", "closed"):
            return m == "open"
        return None
    except Exception:
        return None

def get_effective_et_trade_date() -> date:
    """
    'Broker-like' behavior:
    - If market open -> today (ET, weekday)
    - If market closed -> previous weekday
    Notes:
    - We don't try to model holidays here; if a day is a holiday, snapshot will simply have no 0DTE,
      and you'd still see "No rows". For true holiday handling, we’d need a trading calendar or store history.
    """
    today = last_weekday(et_today())
    market_open = polygon_market_is_open_best_effort()
    if market_open is False:
        return prev_weekday(today)
    return today


# -----------------------
# Data: snapshot 0DTE
# -----------------------
def fetch_chain_0dte_snapshot(underlying: str, exp_date: date, max_pages: int = 10) -> pd.DataFrame:
    """
    Snapshot is LIVE data. It only knows about contracts that exist now.
    So exp_date must be "today's 0DTE" (or the last trading day we can still access in snapshot).
    """
    if not POLYGON_API_KEY:
        raise ValueError("POLYGON_API_KEY is empty. Add it in Railway Variables.")

    base_url = f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
    params = {"apiKey": POLYGON_API_KEY, "limit": 250}

    rows = []
    url = base_url
    pages = 0

    while url and pages < max_pages:
        pages += 1
        data = _polygon_get(url, params=params)

        results = data.get("results", []) or []
        for item in results:
            details = item.get("details", {}) or {}
            greeks = item.get("greeks", {}) or {}

            exp = details.get("expiration_date")
            if exp != exp_date.isoformat():
                continue

            strike = details.get("strike_price")
            opt_type = details.get("contract_type")  # "call"/"put"
            gamma = greeks.get("gamma")
            oi = item.get("open_interest")

            if strike is None or opt_type not in ("call", "put"):
                continue
            if gamma is None or oi is None:
                continue

            rows.append(
                {"strike": float(strike), "type": opt_type, "gamma": float(gamma), "open_interest": float(oi)}
            )

        next_url = data.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": POLYGON_API_KEY}
        else:
            url = None

    return pd.DataFrame(rows)

def compute_abs_gex_by_strike(df: pd.DataFrame) -> pd.DataFrame:
    """
    CallGEX = sum(gamma*OI) on calls
    PutGEX  = sum(gamma*OI) on puts
    AbsGEX  = abs(CallGEX) + abs(PutGEX)
    """
    if df.empty:
        return pd.DataFrame(columns=["strike", "CallGEX", "PutGEX", "AbsGEX"])

    df = df.copy()
    df["gex"] = df["gamma"] * df["open_interest"]

    calls = df[df["type"] == "call"].groupby("strike")["gex"].sum().rename("CallGEX")
    puts = df[df["type"] == "put"].groupby("strike")["gex"].sum().rename("PutGEX")

    out = pd.concat([calls, puts], axis=1).fillna(0.0).reset_index()
    out["AbsGEX"] = out["CallGEX"].abs() + out["PutGEX"].abs()
    out = out.sort_values("strike").reset_index(drop=True)
    return out


# -----------------------
# Cache
# -----------------------
@st.cache_data(show_spinner=False)
def cached_abs_gex(underlying: str, exp_date_iso: str, refresh_nonce: int, ttl_bucket: int):
    exp_date = date.fromisoformat(exp_date_iso)
    df_chain = fetch_chain_0dte_snapshot(underlying, exp_date)
    df_abs = compute_abs_gex_by_strike(df_chain)
    return df_abs


# -----------------------
# UI sidebar
# -----------------------
if "refresh_nonce" not in st.session_state:
    st.session_state.refresh_nonce = 0

with st.sidebar:
    st.header("Inputs")
    underlying = st.text_input("Underlying", value=DEFAULT_UNDERLYING).upper().strip()

    top_n = st.slider("Top N strikes (by AbsGEX)", min_value=5, max_value=50, value=15, step=1)
    ttl = st.number_input("Cache TTL (sec)", min_value=0, max_value=3600, value=DEFAULT_TTL, step=10)

    col1, col2 = st.columns(2)
    with col1:
        refresh = st.button("🔄 Refresh now", use_container_width=True)
    with col2:
        clear_cache = st.button("🧹 Clear cache", use_container_width=True)

    st.caption("Snapshot is LIVE. Historical day selection requires storing snapshots or a historical plan.")


# TTL bucketing
if ttl <= 0:
    ttl_bucket = int(time.time())
else:
    ttl_bucket = int(time.time() // ttl)

if clear_cache:
    st.cache_data.clear()
    st.success("Cache cleared.")

if refresh:
    st.session_state.refresh_nonce += 1

# Effective date (broker-like) + status
market_open = polygon_market_is_open_best_effort()
effective_date = get_effective_et_trade_date()

status_text = "Unknown" if market_open is None else ("Open" if market_open else "Closed")
st.caption(f"ET time now: {et_now().strftime('%Y-%m-%d %H:%M:%S')} • Market: **{status_text}** • Using 0DTE expiration date: **{effective_date.isoformat()}**")

err_box = st.empty()

try:
    if not underlying:
        raise ValueError("Underlying is empty.")

    df_abs = cached_abs_gex(
        underlying=underlying,
        exp_date_iso=effective_date.isoformat(),
        refresh_nonce=st.session_state.refresh_nonce,
        ttl_bucket=ttl_bucket,
    )

    if df_abs.empty:
        st.warning(
            "No rows returned. Most common reasons:\n"
            "- Polygon snapshot did not include greeks/open_interest for these contracts right now\n"
            "- Expiration date has no 0DTE contracts in snapshot (holiday / special schedule)\n"
            "- Endpoint limits / partial data\n\n"
            "If you want to browse past days reliably, we should store snapshots in Railway Postgres."
        )
    else:
        top = df_abs.sort_values("AbsGEX", ascending=False).head(int(top_n)).copy()
        top = top.sort_values("strike")

        st.subheader(f"{underlying} • 0DTE {effective_date.isoformat()} (ET)")
        st.caption("AbsoluteGEX per strike = abs(sum(gamma*OI calls)) + abs(sum(gamma*OI puts))")

        st.bar_chart(top.set_index("strike")[["AbsGEX"]])

        with st.expander("Show table"):
            st.dataframe(
                top[["strike", "CallGEX", "PutGEX", "AbsGEX"]],
                use_container_width=True,
                hide_index=True,
            )

except PermissionError as e:
    err_box.error(f"Polygon auth/plan error: {e}")
except Exception as e:
    err_box.error(f"Data fetch failed: {e}")
