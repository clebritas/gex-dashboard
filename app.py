import datetime as dt
import streamlit as st
import pandas as pd

from polygon_data import fetch_0dte_abs_gex_by_strike, PolygonAuthError, PolygonRequestError


st.set_page_config(page_title="0DTE Abs GEX by Strike", layout="wide")

st.title("0DTE Absolute GEX by Strike (Abs(CallGEX)+Abs(PutGEX))")

with st.sidebar:
    st.subheader("Inputs")
    underlying = st.text_input("Underlying", value="SPY").upper().strip()
    as_of = st.date_input("As of", value=dt.date.today())
    top_n = st.slider("Top N strikes (by AbsGEX)", min_value=5, max_value=50, value=15, step=1)
    ttl = st.number_input("Cache TTL (sec)", min_value=5, max_value=600, value=60, step=5)


@st.cache_data(ttl=60, show_spinner=False)
def _load(underlying: str, as_of_str: str) -> pd.DataFrame:
    return fetch_0dte_abs_gex_by_strike(underlying=underlying, as_of=as_of_str)


as_of_str = as_of.strftime("%Y-%m-%d")

try:
    with st.spinner("Fetching Polygon option chain snapshot..."):
        df = _load(underlying, as_of_str)

    # обновим TTL на лету (хак: пересоздаём кеш через параметр — проще: просто выставь ttl руками в коде)
    # Если хочешь именно runtime TTL — скажи, сделаем без cache_data.

    if df.empty:
        st.warning("No rows returned. Possible reasons: market closed, no contracts, or greeks/OI missing.")
        st.stop()

    # Top N по AbsGEX
    top = df.sort_values("abs_gex", ascending=False).head(int(top_n)).copy()
    top = top.sort_values("strike").reset_index(drop=True)

    c1, c2 = st.columns([2, 1])

    with c1:
        st.subheader("Chart (Top strikes by AbsGEX)")
        chart_df = top.set_index("strike")[["abs_gex"]]
        st.bar_chart(chart_df)

    with c2:
        st.subheader("Top table")
        st.dataframe(
            top[["strike", "call_gex", "put_gex", "abs_gex"]],
            use_container_width=True,
            hide_index=True,
        )

except PolygonAuthError as e:
    st.error(
        "Data fetch failed: Polygon auth/entitlement error.\n\n"
        f"{e}\n\n"
        "Что проверить:\n"
        "• план реально Options Starter (не Stocks)\n"
        "• в Polygon Dashboard у ключа включён продукт Options\n"
        "• ключ именно от того аккаунта/воркспейса, где куплен Options Starter\n"
        "• для Starter данные будут 15-minute delayed — это ок\n"
    )
except PolygonRequestError as e:
    st.error(f"Polygon request error: {e}")
except Exception as e:
    st.exception(e)
