import os
import requests
import datetime as dt
import pandas as pd

POLY = "https://api.polygon.io"

def _get(url, params):
    r = requests.get(url, params=params, timeout=10)
    if r.status_code in (401, 403):
        raise RuntimeError(f"Polygon auth error ({r.status_code}). Check POLYGON_API_KEY and plan.")
    r.raise_for_status()
    return r.json()
    
if r.status_code in (401, 403):
    raise RuntimeError(f"{r.status_code} {r.text}")

def get_spot(ticker: str, api_key: str) -> float:
    # Last trade (works widely)
    data = _get(f"{POLY}/v2/last/trade/{ticker}", {"apiKey": api_key})
    return float(data["last"]["price"])

def get_nearest_expiration_0dte(underlying: str, api_key: str, asof: dt.date) -> dt.date:
    """
    Finds nearest expiration >= asof. For 0DTE, we prefer same day if exists.
    """
    # Options contracts list (paginate if needed later; MVP keeps it simple)
    url = f"{POLY}/v3/reference/options/contracts"
    params = {
        "apiKey": api_key,
        "underlying_ticker": underlying,
        "limit": 1000,
        "sort": "expiration_date",
        "order": "asc",
        "as_of": asof.isoformat(),
    }
    data = _get(url, params)
    exps = []
    for c in data.get("results", []):
        ed = c.get("expiration_date")
        if ed:
            exps.append(dt.date.fromisoformat(ed))
    if not exps:
        raise RuntimeError("Polygon did not return any expirations for this underlying.")
    exps = sorted(set(exps))
    # Prefer same-day (0DTE) if present
    if asof in exps:
        return asof
    # else nearest future
    for e in exps:
        if e >= asof:
            return e
    return exps[-1]

def get_chain_0dte(underlying: str, api_key: str, asof: dt.date) -> pd.DataFrame:
    """
    Returns a DataFrame of option contracts for nearest expiration (0DTE if available).
    Columns: strike, type(call/put), open_interest, iv(optional), gamma(optional), tte_years
    """
    exp = get_nearest_expiration_0dte(underlying, api_key, asof)

    url = f"{POLY}/v3/reference/options/contracts"
    params = {
        "apiKey": api_key,
        "underlying_ticker": underlying,
        "expiration_date": exp.isoformat(),
        "limit": 1000,
        "sort": "strike_price",
        "order": "asc",
        "as_of": asof.isoformat(),
    }
    data = _get(url, params)
    rows = []
    for c in data.get("results", []):
        strike = c.get("strike_price")
        opt_type = c.get("contract_type")  # "call"/"put"
        ticker = c.get("ticker")
        if strike is None or opt_type not in ("call", "put") or not ticker:
            continue
        rows.append({"option_ticker": ticker, "strike": float(strike), "type": opt_type})

    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError("No option contracts returned for that expiration.")

    # Pull snapshots (open interest, iv, greeks) per option ticker.
    # Endpoint availability may vary by plan; we’ll attempt and fill missing gracefully.
    snap_rows = []
    for t in df["option_ticker"].tolist():
        try:
            snap = _get(f"{POLY}/v3/snapshot/options/{underlying}/{t}", {"apiKey": api_key})
            # Schema can vary; extract defensively:
            res = snap.get("results", {}) or {}
            details = res.get("details", {}) or {}
            greeks = res.get("greeks", {}) or {}
            iv = res.get("implied_volatility")
            oi = res.get("open_interest")
            gamma = greeks.get("gamma")

            snap_rows.append({
                "option_ticker": t,
                "open_interest": oi,
                "iv": iv,
                "gamma": gamma,
            })
        except Exception:
            snap_rows.append({"option_ticker": t, "open_interest": None, "iv": None, "gamma": None})

    snap_df = pd.DataFrame(snap_rows)
    out = df.merge(snap_df, on="option_ticker", how="left")

    # time to expiration in years (approx). For 0DTE, set tiny positive to avoid divide-by-zero
    tte_days = max((exp - asof).days, 0)
    out["tte_years"] = max(tte_days, 0) / 365.0
    if out["tte_years"].iloc[0] == 0.0:
        out["tte_years"] = 1.0 / 365.0  # treat 0DTE as 1 day for stability

    return out, exp
