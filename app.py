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


# -----------------------
# Helpers: dates in ET
# -----------------------
def et_today() -> date:
    return datetime.now(NY).date()


def last_weekday(d: date) -> date:
    # Super-simple "trading day" fallback (handles Sat/Sun only).
    # Holidays (like Dec 25) are not handled here on purpose.
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
# Polygon fetch
# -----------------------
def _polygon_get(url: str, params: dict) -> dict:
    # Important: Polygon returns 401/403 if plan doesn't include endpoint, or bad key.
    r = requests.get(url, params=params, timeout=30)
    if r.status_code in (401, 403):
        raise PermissionError(f"{r.status_code} {r.text}")
    r.raise_for_status()
    return r.json()


def fetch_chain_0dte_snapshot(underlying: str, as_of: date, max_pages: int = 10) -> pd.DataFrame:
    """
    Pulls options snapshot for underlying and filters to expiration_date == as_of (0DTE).
    We expect Polygon snapshot to include greeks.gamma and open_interest per contract.
    """
    if not POLYGON_API_KEY:
        raise ValueError("POLYGON_API_KEY is empty. Add it in Railway Variables.")

    # Polygon Snapshot endpoint (commonly used pattern):
    # https://api.polygon.io/v3/snapshot/options/{underlying}
    # Returns {"results":[...], "next_url": "..."} (pagination).
    base_url = f"https://api.polygon.io/v3/snapshot/options/{underlying.upper()}"
    params = {
        "apiKey": POLYGON_API_KEY,
        # Many snapshot endpoints support "limit"; safe to send.
        "limit": 250,
    }

    rows = []
    url = base_url
    pages = 0

    while url and pages < max_pages:
        pages += 1
        data = _polygon_get(url, params=params)

        results = data.get("results", []) or []
        for item in results:
            # Typical structure fields (best-effort):
            # item["details"]["strike_price"], item["details"]["expiration_date"], item["details"]["contract_type"]
            # item["greeks"]["gamma"], item["open_interest"]
            details = item.get("details", {}) or {}
            greeks = item.get("greeks", {}) or {}

            exp = details.get("expiration_date")
            if exp != as_of.isoformat():
                continue

            strike = details.get("strike_price")
            opt_type = details.get("contract_type")  # "call" / "put"
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
        # Polygon next_url often requires apiKey again
        if next_url:
            url = next_url
            params = {"apiKey": POLYGON_API_KEY}
        else:
            url = None

    df = pd.DataFrame(rows)
    return df


def compute_abs_gex_by_strike(df: pd.DataFrame) -> pd.DataFrame:
    """
    Variant 2: AbsoluteGEX = abs(CallGEX) + abs(PutGEX)
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


# -----------------------
# Caching with manual refresh
# -----------------------
@st.cache_data(show_spinner=False)
def cached_abs_gex(underlying: str, as_of_iso: str, refresh_nonce: int) -> pd.DataFrame:
    # refresh_nonce exists ONLY to bust cache when user clicks Refresh
    as_of = date.fromisoformat(as_of_iso)
    chain = fetch_chain_0dte_snapshot(underlying, as_of)
    return compute_abs_gex_by_strike(chain)


# -----------------------
# UI
# -----------------------
st.title("0DTE Absolute GEX by Strike (Abs(CallGEX) + Abs(PutGEX))")

# defaults in ET
default_as_of = last_weekday(et_today())

# query params defaults
default_underlying = qp_get_str("u", DEFAULT_UNDERLYING).upper()
default_as_of = qp_get_date("asof", default_as_of)

# session init
if "refresh_nonce" not in st.session_state:
    st.session_state.refresh_nonce = 0

# Sidebar inputs
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

    st.caption("Tip: Refresh forces a new Polygon request even if TTL hasn't expired.")

# Apply TTL dynamically
# NOTE: Streamlit cache ttl is fixed per function definition in older versions.
# We'll simulate TTL by including a time-bucket in the nonce unless user presses Refresh.
# If ttl=0 => always refresh.
time_bucket = 0
if ttl <= 0:
    time_bucket = int(time.time())
else:
    time_bucket = int(time.time() // ttl)

# Handle buttons
if clear_cache:
    st.cache_data.clear()
    st.success("Cache cleared.")

if refresh:
    st.session_state.refresh_nonce += 1

# Persist inputs to URL (survives refresh)
st.query_params["u"] = underlying
st.query_params["asof"] = as_of.isoformat()

# Fetch + render
err_box = st.empty()
placeholder = st.empty()

try:
    if not underlying:
        raise ValueError("Underlying is empty.")

    df_abs = cached_abs_gex(
        underlying=underlying,
        as_of_iso=as_of.isoformat(),
        refresh_nonce=(st.session_state.refresh_nonce * 10_000_000) + time_bucket,
    )

    if df_abs.empty:
        st.warning("No rows returned. Possible reasons: market closed, no 0DTE contracts, or greeks/OI missing.")
    else:
        # Top N by AbsGEX
        top = df_abs.sort_values("AbsGEX", ascending=False).head(int(top_n)).copy()
        top = top.sort_values("strike")

        st.subheader(f"{underlying} • 0DTE {as_of.isoformat()} (ET date)")
        st.caption("AbsoluteGEX per strike: abs(sum(gamma*OI calls)) + abs(sum(gamma*OI puts))")

        # Chart
        chart_df = top.set_index("strike")[["AbsGEX"]]
        st.bar_chart(chart_df)

        # Table
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
