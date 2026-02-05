import os
import time
import requests
import pandas as pd


POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "").strip()
BASE = "https://api.polygon.io"


class PolygonAuthError(Exception):
    pass


class PolygonRequestError(Exception):
    pass


def _get_json(url: str, params: dict | None = None, timeout: int = 30) -> dict:
    if not POLYGON_API_KEY:
        raise PolygonAuthError("POLYGON_API_KEY is empty. Add it to Railway Variables.")

    params = dict(params or {})
    params["apiKey"] = POLYGON_API_KEY

    r = requests.get(url, params=params, timeout=timeout)

    # Явно обрабатываем ошибки доступа/плана
    if r.status_code in (401, 403):
        try:
            payload = r.json()
        except Exception:
            payload = {"message": r.text}
        raise PolygonAuthError(f"Polygon auth error ({r.status_code}): {payload}")

    if r.status_code >= 400:
        raise PolygonRequestError(f"Polygon request failed ({r.status_code}): {r.text}")

    return r.json()


def fetch_0dte_abs_gex_by_strike(
    underlying: str,
    as_of: str,
    contract_type: str | None = None,  # "call" | "put" | None
    limit: int = 250,
    sleep_s: float = 0.0,
) -> pd.DataFrame:
    """
    Pull 0DTE option chain snapshot for `underlying` and expiration_date=`as_of`,
    compute per-strike:
      CallGEX = sum(gamma * open_interest) for calls
      PutGEX  = sum(gamma * open_interest) for puts  (kept negative sign for convenience)
      AbsGEX  = abs(CallGEX) + abs(PutGEX_signed)

    No spot used. No multiplier applied (если захочешь *100 — скажешь).
    """
    underlying = underlying.upper().strip()

    url = f"{BASE}/v3/snapshot/options/{underlying}"
    params = {
        "expiration_date": as_of,   # 0DTE
        "limit": int(limit),
    }
    if contract_type in ("call", "put"):
        params["contract_type"] = contract_type

    rows: list[dict] = []
    next_url: str | None = None

    while True:
        if next_url:
            data = _get_json(next_url, params={})  # next_url уже содержит apiKey? нет -> мы добавим в _get_json
        else:
            data = _get_json(url, params=params)

        results = data.get("results") or []
        for item in results:
            details = item.get("details") or {}
            greeks = item.get("greeks") or {}

            strike = details.get("strike_price")
            ctype = details.get("contract_type")  # "call" / "put"
            gamma = greeks.get("gamma")
            oi = item.get("open_interest")

            if strike is None or ctype not in ("call", "put"):
                continue
            if gamma is None or oi is None:
                # у некоторых контрактов greeks могут быть пустыми — пропускаем
                continue

            rows.append(
                {
                    "strike": float(strike),
                    "type": ctype,
                    "gamma": float(gamma),
                    "open_interest": float(oi),
                }
            )

        next_url = data.get("next_url")
        if not next_url:
            break

        # Massive/Polygon next_url обычно без apiKey — добавим его
        if "apiKey=" not in next_url and POLYGON_API_KEY:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={POLYGON_API_KEY}"

        if sleep_s and sleep_s > 0:
            time.sleep(sleep_s)

    if not rows:
        return pd.DataFrame(columns=["strike", "call_gex", "put_gex", "abs_gex"])

    df = pd.DataFrame(rows)
    df["gex"] = df["gamma"] * df["open_interest"]

    # calls positive, puts negative (удобно для Net, но Abs считаем отдельно)
    df.loc[df["type"] == "put", "gex"] *= -1.0

    # агрегируем по strike
    pivot = df.pivot_table(index="strike", columns="type", values="gex", aggfunc="sum").fillna(0.0)
    # гарантируем колонки
    if "call" not in pivot.columns:
        pivot["call"] = 0.0
    if "put" not in pivot.columns:
        pivot["put"] = 0.0

    out = (
        pivot.rename(columns={"call": "call_gex", "put": "put_gex"})
        .reset_index()
        .sort_values("strike")
        .reset_index(drop=True)
    )

    out["abs_gex"] = out["call_gex"].abs() + out["put_gex"].abs()
    return out
