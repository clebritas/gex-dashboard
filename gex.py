import math
import numpy as np
import pandas as pd
from scipy.stats import norm

CONTRACT_MULTIPLIER = 100

def bs_gamma(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """
    Black-Scholes gamma for equity options.
    S spot, K strike, T years, r rate, sigma IV (decimal).
    Returns gamma per $1 move in underlying.
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return np.nan
    d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
    return norm.pdf(d1) / (S * sigma * math.sqrt(T))

def compute_abs_gex_by_strike(df: pd.DataFrame, spot: float, r: float = 0.05) -> pd.DataFrame:
    """
    df columns expected:
      - strike (float)
      - type ('call' or 'put')
      - open_interest (int)
      - iv (float, decimal like 0.22)  [optional if gamma is present]
      - gamma (float)                 [optional]
      - tte_years (float)
    Returns aggregated by strike with:
      CallGEX, PutGEX, AbsGEX (variant 2), NetGEX, plus helpers.
    """
    work = df.copy()

    # gamma: use provided if exists, else compute from IV
    if "gamma" not in work.columns or work["gamma"].isna().all():
        work["gamma"] = work.apply(
            lambda row: bs_gamma(spot, row["strike"], row["tte_years"], r, row.get("iv", np.nan)),
            axis=1
        )

    work["open_interest"] = pd.to_numeric(work["open_interest"], errors="coerce").fillna(0.0)
    work["gamma"] = pd.to_numeric(work["gamma"], errors="coerce")

    # GEX per contract line: gamma * OI * 100
    work["gex"] = work["gamma"] * work["open_interest"] * CONTRACT_MULTIPLIER

    calls = work[work["type"] == "call"].groupby("strike")["gex"].sum().rename("CallGEX")
    puts  = work[work["type"] == "put"].groupby("strike")["gex"].sum().rename("PutGEX")

    out = pd.concat([calls, puts], axis=1).fillna(0.0).reset_index()

    out["AbsGEX"] = out["CallGEX"].abs() + out["PutGEX"].abs()          # ✅ вариант 2
    out["NetGEX"] = out["CallGEX"] - out["PutGEX"]                      # полезно для направления
    out = out.sort_values("strike").reset_index(drop=True)
    return out
