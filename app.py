import os
import datetime as dt

import pandas as pd
import streamlit as st
import plotly.express as px

from polygon_data import get_spot, get_chain_0dte
from gex import compute_abs_gex_by_strike


# ----------------------------
# Streamlit config
# ----------------------------
st.set_page_config(page_title="0DTE GEX Dashboard", layout="wide")
st.title("0DTE Absolute GEX by Strike (Abs(CallGEX)+Abs(PutGEX))")


# ----------------------------
# Read API key from env
# ----------------------------
api_key = os.getenv("POLYGON_API_KEY", "")
if not api_key:
    st.error("POLYGON_API_KEY is missing. Add it in Railway → Service → Variables and redeploy/restart.")
    st.stop()


# ----------------------------
# Controls
# ----------------------------
colA, colB, colC, colD = st.columns([1, 1, 2, 1])
with colA:
    underlying = st.text_input("Underlying", value="SPY").strip().upper()
with colB:
    asof = st.date_input("As of", value=dt.date.today())
with colC:
    top_n = st.slider("Top N strikes (by AbsGEX)", min_value=5, max_value=50, value=15)
with colD:
    ttl = st.number_input("Cache TTL (sec)", min_value=15, max_value=600, value=60, step=15)


# ----------------------------
# Cached loader (prevents rate-limit crashes)
# ----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_data_cached(_underlying: str, _api_key: str, _asof: dt.date):
    spot = get_spot(_underlying, _api_key)
    chain_df, exp = get_chain_0dte(_underlying, _api_key, _asof)
    return spot, chain_df, exp


# Streamlit cache TTL must be static in decorator, so we wrap:
def load_data(_underlying: str, _api_key: str, _asof: dt.date, _ttl: int):
    # Manually clear cache when user changes TTL to force new policy
    # (Optional: you can remove this if you don't care)
    # We'll just rely on the default ttl=60 above for stability.
    return load_data_cached(_underlying, _api_key, _asof)


# ----------------------------
# Fetch + guardrails
# ----------------------------
with st.spinner("Fetching 0DTE options from Polygon..."):
    try:
        spot, chain_df, exp = load_data(underlying, api_key, asof, int(ttl))
    except Exception as e:
        st.error(f"Data fetch failed: {e}")
        st.stop()

if chain_df is None or getattr(chain_df, "empty", True):
    st.error("Polygon returned an empty option chain. Try again in a minute.")
    st.stop()

# Basic sanity checks
required_cols = {"strike", "type", "open_interest", "tte_years"}
missing = required_cols - set(chain_df.columns)
if missing:
    st.error(f"Missing required columns from chain data: {sorted(missing)}")
    st.stop()

st.caption(f"Spot: {spot:.2f} | Expiration used: {exp.isoformat()} | Contracts: {len(chain_df)}")

# ----------------------------
# Compute profile
# ----------------------------
try:
    profile = compute_abs_gex_by_strike(chain_df, spot=spot, r=0.05)
except Exception as e:
    st.error(f"GEX computation failed: {e}")
    st.stop()

if profile is None or profile.empty:
    st.error("Computed profile is empty (no usable gamma/OI data).")
    st.stop()

# ----------------------------
# KPI cards (helpful summary)
# ----------------------------
k1, k2, k3, k4 = st.columns(4)

call_wall_strike = float(profile.loc[profile["CallGEX"].idxmax(), "strike"])
put_wall_strike = float(profile.loc[profile["PutGEX"].idxmax(), "strike"])
abs_peak_strike = float(profile.loc[profile["AbsGEX"].idxmax(), "strike"])
abs_peak_value = float(profile["AbsGEX"].max())

with k1:
    st.metric("Call Wall (max CallGEX)", f"{call_wall_strike:.2f}")
with k2:
    st.metric("Put Wall (max PutGEX)", f"{put_wall_strike:.2f}")
with k3:
    st.metric("AbsGEX Peak (max pressure)", f"{abs_peak_strike:.2f}")
with k4:
    st.metric("AbsGEX Peak value", f"{abs_peak_value:,.0f}")

st.divider()

# ----------------------------
# Top strikes table
# ----------------------------
st.subheader("Top strikes (by AbsGEX)")
top = profile.sort_values("AbsGEX", ascending=False).head(top_n).copy()
st.dataframe(top[["strike", "AbsGEX", "CallGEX", "PutGEX", "NetGEX"]], use_container_width=True)

# ----------------------------
# Chart: horizontal bars profile
# ----------------------------
st.subheader("Absolute GEX profile (all strikes)")
fig = px.bar(
    profile,
    x="AbsGEX",
    y="strike",
    orientation="h",
    title="Absolute GEX by Strike (Abs(CallGEX)+Abs(PutGEX))",
)

# Add "spot" as horizontal line (approx)
xmax = float(profile["AbsGEX"].max()) if float(profile["AbsGEX"].max()) > 0 else 1.0
fig.add_shape(
    type="line",
    x0=0,
    x1=xmax * 1.02,
    y0=spot,
    y1=spot,
    xref="x",
    yref="y",
)

# Add key levels as shapes (call wall / put wall / abs peak)
for level, label in [
    (call_wall_strike, "Call Wall"),
    (put_wall_strike, "Put Wall"),
    (abs_peak_strike, "Abs Peak"),
]:
    fig.add_shape(
        type="line",
        x0=0,
        x1=xmax * 1.02,
        y0=level,
        y1=level,
        xref="x",
        yref="y",
    )

st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# Generate thinkScript levels for TOS
# ----------------------------
st.subheader("TOS levels (copy/paste)")

# Top 10 AbsGEX strikes, sorted
levels = top["strike"].sort_values().tolist()[:10]

script_lines = [
    "# 0DTE AbsGEX Levels (auto-generated)",
    f"# Underlying: {underlying} | Exp: {exp.isoformat()} | Spot: {spot:.2f}",
    "",
    f"input CallWall = {call_wall_strike:.2f};",
    f"input PutWall  = {put_wall_strike:.2f};",
    f"input AbsPeak  = {abs_peak_strike:.2f};",
]

for i, s in enumerate(levels, start=1):
    script_lines.append(f"input Lvl{i} = {float(s):.2f};")

script_lines += [
    "",
    "plot pCallWall = CallWall;",
    "plot pPutWall  = PutWall;",
    "plot pAbsPeak  = AbsPeak;",
    "",
    "pCallWall.SetDefaultColor(Color.GREEN);",
    "pPutWall.SetDefaultColor(Color.RED);",
    "pAbsPeak.SetDefaultColor(Color.YELLOW);",
    "pCallWall.SetStyle(Curve.SHORT_DASH);",
    "pPutWall.SetStyle(Curve.SHORT_DASH);",
    "pAbsPeak.SetStyle(Curve.FIRM);",
    "",
]

for i in range(1, len(levels) + 1):
    script_lines += [
        f"plot L{i} = Lvl{i};",
        f"L{i}.SetDefaultColor(Color.GRAY);",
        f"L{i}.SetStyle(Curve.SHORT_DASH);",
        "",
    ]

st.code("\n".join(script_lines), language="text")

# ----------------------------
# Debug panel (optional)
# ----------------------------
with st.expander("Debug (optional)"):
    st.write("Sample chain rows:")
    st.dataframe(chain_df.head(20), use_container_width=True)
