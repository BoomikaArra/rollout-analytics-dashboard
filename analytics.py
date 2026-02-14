
from __future__ import annotations

from typing import Dict, Any
import pandas as pd
import numpy as np

STEPS = ["impression", "click", "apply", "approve"]

def load_events_csv(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"user_id": "string"})
    required = {"date", "user_id", "variant", "channel", "segment", "step"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["variant"] = df["variant"].astype(str).str.lower().str.strip()
    df["channel"] = df["channel"].astype(str).str.lower().str.strip()
    df["segment"] = df["segment"].astype(str).str.lower().str.strip()
    df["step"] = df["step"].astype(str).str.lower().str.strip()

    return df[df["step"].isin(STEPS)].copy()

def _unique_users(df: pd.DataFrame, step: str) -> int:
    return int(df.loc[df["step"] == step, "user_id"].nunique())

def compute_funnel_summary(df: pd.DataFrame) -> Dict[str, Any]:
    counts = {s: _unique_users(df, s) for s in STEPS}

    def safe_rate(n, d):
        return float(n / d) if d and d > 0 else 0.0

    rates = {
        "click_over_impression": safe_rate(counts["click"], counts["impression"]),
        "apply_over_click": safe_rate(counts["apply"], counts["click"]),
        "approve_over_apply": safe_rate(counts["approve"], counts["apply"]),
        "approve_over_impression": safe_rate(counts["approve"], counts["impression"]),
    }
    return {"counts": counts, "rates": rates}

def compute_lift_table(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["variant"].isin(["control", "test"])].copy()
    if sub.empty:
        return pd.DataFrame(columns=["metric", "control", "test", "lift_pct"])

    def rates_for(v):
        return compute_funnel_summary(sub[sub["variant"] == v])["rates"]

    c = rates_for("control")
    t = rates_for("test")

    rows = []
    for metric in ["click_over_impression", "apply_over_click", "approve_over_apply", "approve_over_impression"]:
        cv = float(c.get(metric, 0.0))
        tv = float(t.get(metric, 0.0))
        lift = ((tv - cv) / cv) if cv > 0 else 0.0
        rows.append({"metric": metric, "control": cv, "test": tv, "lift_pct": lift})
    return pd.DataFrame(rows)

def compute_daily_series(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["date","impressions","clicks","applies","approves","approve_rate_over_impression"])

    g = (
        df.groupby(["date", "step"])["user_id"]
        .nunique()
        .reset_index(name="users")
        .pivot(index="date", columns="step", values="users")
        .fillna(0)
        .reset_index()
        .sort_values("date")
    )

    for s in STEPS:
        if s not in g.columns:
            g[s] = 0

    g.rename(columns={"impression":"impressions","click":"clicks","apply":"applies","approve":"approves"}, inplace=True)
    g["approve_rate_over_impression"] = np.where(g["impressions"] > 0, g["approves"] / g["impressions"], 0.0)
    return g[["date","impressions","clicks","applies","approves","approve_rate_over_impression"]]

def detect_anomalies(daily: pd.DataFrame, metric: str = "approve_rate_over_impression", z_threshold: float = 3.0) -> pd.DataFrame:
    if daily.empty or metric not in daily.columns:
        return pd.DataFrame(columns=["date", "metric", "value", "z", "flag"])

    s = daily[metric].astype(float)
    mu = float(s.mean())
    sd = float(s.std(ddof=0)) if float(s.std(ddof=0)) > 0 else 0.0
    z = np.zeros(len(s)) if sd == 0.0 else (s - mu) / sd

    out = daily[["date"]].copy()
    out["metric"] = metric
    out["value"] = s.values
    out["z"] = z
    out["flag"] = np.where(np.abs(z) >= z_threshold, "ANOMALY", "")

    flagged = out[out["flag"] == "ANOMALY"].copy()
    return flagged if not flagged.empty else out.tail(14).copy()
