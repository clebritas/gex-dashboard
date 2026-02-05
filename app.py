# app.py
import os
import time
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple

import requests
import pandas as pd
import streamlit as st


# -----------------------
# Config
# -----------------------
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BASE = "https://api.polygon.io"
ET = ZoneInfo("America/New_York")


# -----------------------
# Helpers
# -----------------------
def et_now() -> dt.datetime:
    return dt.datetime.now(tz=ET)


def _polygon_get(url: str, params: Dict[str, Any] | None = None, timeout: int = 30) -> Dict[str, Any]:
    if not POLYGON_API_KEY:
        raise RuntimeError("POLYGON_API_KEY is empty. Add it in Railway Variables.")
    params = dict(params or {})
    params["apiKey"] = POLYGON_API_KEY

    r = requests.get(url, params=params, timeout=timeout)
    if r.status_code in (401, 403):
        # Show Polygon error body (often contains 'NOT_AUTHORIZED' message)
        raise PermissionError(f"Polygon auth error ({r.status_code}): {r.text}")
    r.raise_for_status()
    return r.json()


def fetch_snapshot_chain(
    underlying: str,
    expiration_date: str,
    ttl_sec: int,
    force_refresh: bool,
) -> List[Dict[str, Any]]:
    """
    Uses Polygon Snapshot Options Chain endpoint.
    NOTE: Snapshot is "current" and typically includes only active contracts.
    """
    cache_key = f"snapshot_chain::{underlying}::{expiration_date}"

    # Manual cache in session_state (survives reruns; not app restarts)
    now_ts = time.time()
    cache = st.session_state.get("cache", {})
    entry = cache.get(cache_key)

    if (not force_refresh) and entry:
        age = now_ts - entry["ts"]
        if age <= ttl_sec:
            return entry["data"]

    # Pull snapshot chain (paginate if needed)
    url = f"{BASE}/v3/snapshot/options/{underlying}"
    params = {
        "expiration_date": expiration_date,
        "limit": 250,
    }

    data: List[Dict[str, Any]] = []
    j = _polygon_get(url, params=params)

    data.extend(j.get("results", []) or [])

    # Pagination: Polygon sometimes returns next_url
    next_url = j.get("next_url")
    while next_url:
        # next_url might be relative or full; normalize
        if next_url.startswith("/"):
            next_url_full = BASE + next_url
        else:
            next_url_full = next_url

        # Ensure apiKey is present; easiest: call with _polygon_get which injects apiKey
        j = _polygon_get(next_url_full, params={})
        data.extend(j.get("results", []) or [])
        next_url = j.get("next_url")

    cache[cache_key] = {"ts": now_ts, "data": data}
    st.session_state["cache"] = cache
    return data


def compute_abs_gex_by_strike(snapshot_rows: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Variant 2:
    - CallGEX = gamma * open_interest
    - PutGEX  = -gamma * open_interest   (negative sign for puts)
    - AbsGEX per strike = abs(sum(CallGEX)) + abs(sum(PutGEX))
    """
    records = []
    missing_gamma = 0
    missing_oi = 0

    for row in snapshot_rows:
        details = row.get("details") or {}
        greeks = row.get("greeks") or {}

        strike = details.get("strike_price")
        ctype = details.get("contract_type")  # "call" / "put"
        gamma = greeks.get("gamma")
        oi = row.get("open_interest")

        if gamma is None:
            missing_gamma += 1
            continue
        if oi is None:
            missing_oi += 1
            continue
        if strike is None or ctype not in ("call", "put"):
            continue

        gex = float(gamma) * float(oi)
        if ctype == "put":
            gex = -gex

        records.append(
            {
                "strike": float(strike),
                "type": ctype,
                "gamma": float(gamma),
                "open_interest": float(oi),
                "gex": float(gex),
            }
        )

    df = pd.DataFrame(records)
    stats = {
        "rows_total": len(snapshot_rows),
        "rows_used": len(df),
        "missing_gamma": missing_gamma,
        "missing_oi": missing_oi,
    }

    if df.empty:
        return df, stats

    # Aggregate by strike: split calls/puts then absolute formula
    agg = df.groupby(["strike", "type"], as_index=False)["gex"].sum()

    calls = agg[agg["type"] == "call"][["strike", "gex"]].rename(columns={"gex": "call_gex"})
    puts = agg[agg["type"] == "put"][["strike", "gex"]].rename(columns={"gex": "put_gex"})  # already negative

    out = pd.merge(calls, puts, on="strike", how="outer").fillna(0.0)
    out["abs_gex"] = out["call_gex"].abs() + out["put_gex"].abs()
    out = out.sort_values("abs_gex", ascending=False).reset_index(drop=True)
    return out, stats


def demo_data() -> pd.DataFrame:
    # Simple synthetic example to verify computation & chart
    # (call_gex positive, put_gex negative, abs = abs(call) + abs(put))
    data = [
        {"strike": 470, "call_gex": 1200.0, "put_gex": -800.0},
        {"strike": 475, "call_gex": 300.0, "put_gex": -2200.0},
        {"strike": 480, "call_gex": 900.0, "put_gex": -900.0},
        {"strike": 485, "call_gex": 1500.0, "put_gex": -200.0},
    ]
    df = pd.DataFrame(data)
    df["abs_gex"] = df["call_gex"].abs() + df["put_gex"].abs()
    return df.sort_values("abs_gex", ascending=False).reset_index(drop=True)


# -----------------------
# UI
# -----------------------
st.set_page_config(page_title="0DTE Absolute GEX by Strike", layout="wide")

st.title("0DTE Absolute GEX by Strike (Abs(CallGEX) + Abs(PutGEX))")

# Session defaults
if "asof" not in st.session_state:
    st.session_state["asof"] = et_now().date()

if "force_refresh" not in st.session_state:
    st.session_state["force_refresh"] = False

if "cache" not in st.session_state:
    st.session_state["cache"] = {}

with st.sidebar:
    st.header("Inputs")

    underlying = st.text_input("Underlying", value="SPY").strip().upper()

    asof = st.date_input("As of (ET)", value=st.session_state["asof"])
    st.session_state["asof"] = asof

    top_n = st.slider("Top N strikes (by AbsGEX)", 5, 50, 15)
    ttl_sec = st.number_input("Cache TTL (sec)", min_value=0, max_value=3600, value=60, step=10)

    demo_mode = st.toggle("Demo mode (no API)", value=False)

    col1, col2 = st.columns(2)
    with col1:
        refresh = st.button("🔄 Refresh now", use_container_width=True)
    with col2:
        clear_cache = st.button("🧹 Clear cache", use_container_width=True)

    if clear_cache:
        st.session_state["cache"] = {}
        st.toast("Cache cleared")

    # Force refresh triggers a new fetch on this run
    st.session_state["force_refresh"] = bool(refresh)


# Status line
now = et_now()
st.caption(f"ET time now: {now.strftime('%Y-%m-%d %H:%M:%S')}")

# IMPORTANT NOTE about Snapshot vs Historical
st.info(
    "Важно: Polygon Snapshot обычно отдаёт **только текущие активные опционы**. "
    "Если ты выбираешь дату, которая уже **прошла/экспирнулась**, по ней часто будет пусто. "
    "Для стабильного просмотра прошлых дней нужно **сохранять snapshot в базе (Postgres)** или иметь отдельный исторический источник."
)

# Determine 0DTE expiration date:
# Here we treat 'asof' as the intended 0DTE expiration date.
expiration_date = asof.strftime("%Y-%m-%d")

# Fetch / compute
if demo_mode:
    out = demo_data()
    stats = {"rows_total": 0, "rows_used": len(out), "missing_gamma": 0, "missing_oi": 0}
else:
    try:
        rows = fetch_snapshot_chain(
            underlying=underlying,
            expiration_date=expiration_date,
            ttl_sec=int(ttl_sec),
            force_refresh=bool(st.session_state["force_refresh"]),
        )
        out, stats = compute_abs_gex_by_strike(rows)
    except Exception as e:
        st.error(f"Data fetch failed: {e}")
        st.stop()

# Render
if out is None or out.empty:
    st.warning(
        "No rows returned.\n\n"
        "Самые частые причины:\n"
        "• выбранная дата уже в прошлом (контракты не активны → snapshot пустой)\n"
        "• snapshot по этим контрактам сейчас не включает greeks/open_interest\n"
        "• лимиты/частичная выдача\n\n"
        "Для проверки логики включи **Demo mode** в сайдбаре."
    )

    with st.expander("Debug details"):
        st.write(stats)
else:
    show = out.head(int(top_n)).copy()
    show = show.sort_values("strike")  # nicer left-to-right chart

    left, right = st.columns([2, 1], gap="large")

    with left:
        st.subheader("Absolute GEX by Strike (0DTE)")
        chart_df = show.set_index("strike")[["abs_gex"]]
        st.bar_chart(chart_df)

        st.subheader("Call & Put GEX by Strike (0DTE)")
        cp_df = show.set_index("strike")[["call_gex", "put_gex"]]
        st.line_chart(cp_df)

    with right:
        st.subheader("Top strikes")
        # Display table sorted by abs_gex desc
        st.dataframe(out.head(int(top_n)), use_container_width=True, hide_index=True)

    with st.expander("Debug details"):
        st.write(stats)
        st.caption(
            "rows_used = количество контрактов, где нашлись И gamma, и open_interest. "
            "Если rows_used=0 — значит Polygon не отдал нужные поля в snapshot (или контракты не активны)."
        )
