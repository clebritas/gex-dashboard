import os
import time
import requests
import pandas as pd
import streamlit as st
import altair as alt


POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BASE = "https://api.polygon.io"


class PolygonAuthError(Exception):
    pass


class PolygonRequestError(Exception):
    pass


def _get_json(url: str, params=None, timeout: int = 30) -> dict:
    if not POLYGON_API_KEY:
        raise PolygonAuthError("POLYGON_API_KEY is empty. Add it to Railway Variables.")

    params = dict(params or {})
    params["apiKey"] = POLYGON_API_KEY

    r = requests.get(url, params=params, timeout=timeout)

    if r.status_code in (401, 403):
        raise PolygonAuthError(f"Polygon auth error ({r.status_code}): {r.text}")

    if r.status_code >= 400:
        raise PolygonRequestError(f"Polygon request failed ({r.status_code}): {r.text}")

    return r.json()


def fetch_0dte_abs_gex_by_strike(
    underlying: str,
    as_of: str,
    limit: int = 250,
) -> pd.DataFrame:

    underlying = underlying.upper().strip()
    url = f"{BASE}/v3/snapshot/options/{underlying}"

    params = {
        "expiration_date": as_of,
        "limit": limit,
    }

    rows = []
    next_url = None

    while True:
        data = _get_json(next_url or url, params=None if next_url else params)

        for item in data.get("results", []):
            details = item.get("details") or {}
            greeks = item.get("greeks") or {}

            strike = details.get("strike_price")
            ctype = details.get("contract_type")
            gamma = greeks.get("gamma")
            oi = item.get("open_interest")

            if strike is None or gamma is None or oi is None:
                continue

            gex = gamma * oi
            if ctype == "put":
                gex *= -1

            rows.append(
                {
                    "strike": float(strike),
                    "type": ctype,
                    "gex": gex,
                }
            )

        next_url = data.get("next_url")
        if not next_url:
            break

        if "apiKey=" not in next_url:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={POLYGON_API_KEY}"

    if not rows:
        return pd.DataFrame(columns=["strike", "call_gex", "put_gex", "abs_gex"])

    df = pd.DataFrame(rows)
    pivot = df.pivot_table(index="strike", columns="type", values="gex", aggfunc="sum").fillna(0)

    pivot["call_gex"] = pivot.get("call", 0.0)
    pivot["put_gex"] = pivot.get("put", 0.0)

    out = pivot[["call_gex", "put_gex"]].reset_index().sort_values("strike")
    out["abs_gex"] = out["call_gex"].abs() + out["put_gex"].abs()

    return out


# =========================
# Streamlit UI
# =========================

st.set_page_config(layout="wide")
st.title("0DTE Absolute GEX by Strike (Abs(CallGEX) + Abs(PutGEX))")

with st.sidebar:
    underlying = st.text_input("Underlying", "SPY")
    as_of = st.date_input("As of (ET)")
    top_n = st.slider("Top N strikes (by AbsGEX)", 5, 50, 15)
    refresh = st.button("Refresh now")

if refresh or "data" not in st.session_state:
    with st.spinner("Loading data…"):
        st.session_state.data = fetch_0dte_abs_gex_by_strike(
            underlying=underlying,
            as_of=str(as_of),
        )

df = st.session_state.data
show = df.sort_values("abs_gex", ascending=False).head(top_n).sort_values("strike")

# -------- Abs GEX chart --------
st.subheader("Absolute GEX by Strike (0DTE)")
st.bar_chart(show.set_index("strike")[["abs_gex"]])

# -------- Call / Put GEX chart (bars up / down) --------
st.subheader("Call & Put GEX by Strike (0DTE)")

cp_long = show.melt(
    id_vars="strike",
    value_vars=["call_gex", "put_gex"],
    var_name="type",
    value_name="gex",
)

cp_long["type"] = cp_long["type"].map(
    {"call_gex": "Call GEX", "put_gex": "Put GEX"}
)

bars = (
    alt.Chart(cp_long)
    .mark_bar()
    .encode(
        x=alt.X("strike:O", title="Strike"),
        y=alt.Y("gex:Q", title="GEX"),
        color=alt.Color(
            "type:N",
            scale=alt.Scale(
                domain=["Call GEX", "Put GEX"],
                range=["#1f77b4", "#ff7f0e"],
            ),
            legend=alt.Legend(orient="bottom"),
        ),
        tooltip=["strike", "type", alt.Tooltip("gex", format=",.2f")],
    )
)

zero = alt.Chart(pd.DataFrame({"y": [0]})).mark_rule(color="gray").encode(y="y:Q")

st.altair_chart((bars + zero).properties(height=260), use_container_width=True)

# -------- Table --------
st.subheader("Top strikes")
st.dataframe(show.reset_index(drop=True))
