import os
import datetime as dt
import pandas as pd
import streamlit as st
import plotly.express as px

from polygon_data import get_spot, get_chain_0dte
from gex import compute_abs_gex_by_strike

st.set_page_config(page_title="0DTE GEX Dashboard", layout="wide")

st.title("0DTE Absolute GEX by Strike (variant 2)")

api_key = os.getenv("POLYGON_API_KEY", "")
if not api_key:
    st.error("Set POLYGON_API_KEY in Railway environment variables.")
    st.stop()

colA, colB, colC = st.columns([1,1,2])
with colA:
    underlying = st.text_input("Underlying", value="SPY")
with colB:
    asof = st.date_input("As of", value=dt.date.today())
with colC:
    top_n = st.slider("Top N strikes (by AbsGEX)", min_value=5, max_value=50, value=15)

with st.spinner("Fetching data from Polygon..."):
    spot = get_spot(underlying, api_key)
    chain_df, exp = get_chain_0dte(underlying, api_key, asof)
except Exception as e:
    st.error(str(e))
    st.stop()    

st.caption(f"Spot: {spot:.2f} | Expiration used: {exp.isoformat()} | Contracts: {len(chain_df)}")

# Compute
profile = compute_abs_gex_by_strike(chain_df, spot=spot, r=0.05)

# Top strikes table
top = profile.sort_values("AbsGEX", ascending=False).head(top_n).copy()

st.subheader("Top strikes")
st.dataframe(top[["strike", "AbsGEX", "CallGEX", "PutGEX", "NetGEX"]], use_container_width=True)

# Chart: horizontal bars like terminal
st.subheader("Absolute GEX profile (all strikes)")
chart_df = profile.copy()
chart_df["SpotLine"] = spot

fig = px.bar(
    chart_df,
    x="AbsGEX",
    y="strike",
    orientation="h",
    title="Absolute GEX by Strike (Abs(CallGEX)+Abs(PutGEX))",
)
# Add spot line (approx visual via shape)
fig.add_shape(
    type="line",
    x0=0, x1=chart_df["AbsGEX"].max() * 1.02 if chart_df["AbsGEX"].max() > 0 else 1,
    y0=spot, y1=spot,
    xref="x", yref="y",
)

st.plotly_chart(fig, use_container_width=True)

# Generate thinkScript levels for top strikes
st.subheader("Generate TOS levels (manual lines)")
levels = top["strike"].sort_values().tolist()
script_lines = ["# Auto-generated top AbsGEX strikes (0DTE)"]
for i, s in enumerate(levels[:10], start=1):
    script_lines.append(f"input lvl{i} = {float(s):.2f};")
script_lines.append("")
for i in range(1, min(10, len(levels)) + 1):
    script_lines.append(f"plot L{i} = lvl{i};")
    script_lines.append(f"L{i}.SetDefaultColor(Color.YELLOW);")
    script_lines.append(f"L{i}.SetStyle(Curve.SHORT_DASH);")
    script_lines.append("")

st.code("\n".join(script_lines), language="text")
