# app.py
import os
import time
import requests
import pandas as pd
import streamlit as st

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


# -----------------------
# Config
# -----------------------
st.set_page_config(page_title="0DTE Absolute GEX by Strike", layout="wide")

NY = ZoneInfo("America/New_York")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()

DEFAULT_TTL = 60
DEFAULT_UNDERLYING = "SPY"
MAX_FALLBACK_DAYS = 10  # how far back we search for last available data


# -----------------------
# Helpers: dates in ET
# -----------------------
def et_now() -> datetime:
    return datetime.now(NY)


def et_today() -> date:
    return et_now().date()


def last_weekday(d: date) -> date:
    # Simple weekend fallback only (holidays are handled by "walk back until data exists")
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def parse_iso_date(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except Exception:
        return None


def qp_get_str(key: str, fallback: str) -> str:
    try:
        v = st.query_params.get(key)
        if isinstance(v, list):
            v = v[0] if v else None
        return (v or fallback).strip()
    except Exception:
        return fallback


def qp_get_date(key: str, fallback: date) -> date:
    try:
        v = st.query_params.get(key)
        if isinstance(v, list):
            v = v[0] if v else None
        parsed = parse_iso_date(v)
        return parsed or fallback
    except Exception:
        return fallback


# -----------------------
# Polygon low-level
# -----------------------
def _polygon_get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, timeout=30)
    if r.status_code in (401, 403):
        raise PermissionError(f"{r.status_code} {r.text}")
    r.raise_for_status()
    return r.json()


def polygon_market_is_open_best_effort() -> bool | None:
    """
    Best-effort market open check.
    Returns True/False if endpoint is accessible.
    Returns None if endpoint not accessible or errors.
    """
    if not POLYGON_API_KEY:
        return None

    try:
        url = "https://api.polygon.io/v1/marketstatus/now"
        data = _polygon_get(url, {"apiKey": POLYGON_API_KEY})
        # Typical shape: {"market":"open"/"closed", ...}
        m = (data.get("market") or "").lower()
        if m in ("open", "closed"):
            return m == "open"
        return None
    except Exception:
        return None


# -----------------------
# Polygon options snapshot (0DTE)
# -----------------------
def fetch_chain_0dte_snapshot(underlying: str, as_of: date, max_pages: int = 10) -> pd.DataFrame:
    """
    Pulls options snapshot for underlying and filters to expiration_date == as_of (0DTE).
    Requires greeks.gamma and open_interest per contract.
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
            if exp != as_of.isoformat():
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
                {
                    "strike": float(strike),
                    "type": opt_type,
                    "gamma": float(gamma),
                    "open_interest": float(oi),
                }
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
    AbsoluteGEX = abs(CallGEX) + abs(PutGEX)
    CallGEX = sum(gamma*OI) for calls at strike
    PutGEX  = sum(gamma*OI) for puts at strike
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


def find_latest_available_date(underlying: str, requested: date, max_back: int = MAX_FALLBACK_DAYS):
    """
    Try requested date first.
    If no data, step back day-by-day until data appears or we hit max_back.
    Returns (effective_date, dataframe)
    """
    d = requested
    for _ in range(max_back + 1):
        d = last_weekday(d)  # skip weekends
        df_chain = fetch_chain_0dte_snapshot(underlying, d)
        df_abs = compute_abs_gex_by_strike(df_chain)
        if not df_abs.empty:
            return d, df_abs
        d = d - timedelta(days=1)

    return requested, pd.DataFrame(columns=["strike", "CallGEX", "PutGEX", "AbsGEX"])


# -----------------------
# Caching with refresh
# -----------------------
@st.cache_data(show_spinner=False)
def cached_abs_gex_with_fallback(underlying: str, as_of_iso: str, refresh_nonce: int):
    req = date.fromisoformat(as_of_iso)
    effective, df_abs = find_latest_available_date(underlying, req)
    return effective, df_abs


# -----------------------
# UI
# -----------------------
st.title("0DTE Absolute GEX by Strike (Abs(CallGEX) + Abs(PutGEX))")

default_as_of = last_weekday(et_today())

default_underlying = qp_get_str("u", DEFAULT_UNDERLYING).upper()
default_as_of = qp_get_date("asof", default_as_of)

if "refresh_nonce" not in st.session_state:
    st.session_state.refresh_nonce = 0

with st.sidebar:
    st.header("Inputs")

    underlying = st.text_input("Underlying", value=default_underlying).upper().strip()
    as_of = st.date_input("As of (ET)", value=default_as_of)

    top_n = st.slider("Top N strikes (by AbsGEX)", min_value=5, max_value=50, value=15, step=1)
    ttl = st.number_input("Cache TTL (sec)", min_value=0, max_value=3600, value=DEFAULT_TTL, step=10)

    col1, col2 = st.columns(2)
    with col1:
        refresh = st.button("🔄 Refresh now", use_container_width=True)
    with col2:
        clear_cache = st.button("🧹 Clear cache", use_container_width=True)

    st.caption("Refresh forces a new Polygon request even if TTL hasn't expired.")

# Cache-bucketing for TTL
if ttl <= 0:
    time_bucket = int(time.time())
else:
    time_bucket = int(time.time() // ttl)

if clear_cache:
    st.cache_data.clear()
    st.success("Cache cleared.")

if refresh:
    st.session_state.refresh_nonce += 1

# Persist in URL
st.query_params["u"] = underlying
st.query_params["asof"] = as_of.isoformat()

err_box = st.empty()

try:
    if not underlying:
        raise ValueError("Underlying is empty.")

    # optional: market status hint (won't block anything)
    market_open = polygon_market_is_open_best_effort()

    effective_date, df_abs = cached_abs_gex_with_fallback(
        underlying=underlying,
        as_of_iso=as_of.isoformat(),
        refresh_nonce=(st.session_state.refresh_nonce * 10_000_000) + time_bucket,
    )

    if df_abs.empty:
        st.warning(
            "No rows returned even after fallback. Possible reasons: "
            "no 0DTE contracts for that period, greeks/OI missing, or endpoint limits."
        )
    else:
        if effective_date != as_of:
            st.info(
                f"Market appears closed / no 0DTE data for {as_of.isoformat()} — "
                f"showing last available: {effective_date.isoformat()} (ET)."
            )
        else:
            # If it's today and market is closed, still we might have data; show a gentle hint
            if effective_date == et_today() and market_open is False:
                st.caption("Market is currently closed (ET), showing last snapshot available.")

        top = df_abs.sort_values("AbsGEX", ascending=False).head(int(top_n)).copy()
        top = top.sort_values("strike")

        st.subheader(f"{underlying} • 0DTE {effective_date.isoformat()} (ET date)")
        st.caption("AbsoluteGEX per strike: abs(sum(gamma*OI calls)) + abs(sum(gamma*OI puts))")

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
