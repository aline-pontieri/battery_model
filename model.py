"""Battery discharge risk scoring — ported from battery_discharge_model.ipynb."""

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline

HORIZON = 7
SOC_THRESHOLD = 60
ALERT_THRESHOLD = 0.8

NUM_FEATURES = [
    "volt_mean", "volt_slope_3d", "volt_slope_7d", "volt_vol_7d",
    "soc_slope_3d", "soc_slope_7d", "soc_vol_7d",
    "temp_mean", "rents_sum",
]
CAT_FEATURES = ["fw_version_type"]
FEATURES_ALL = NUM_FEATURES + CAT_FEATURES


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df.get("hour_bucket", df.get("timestamp")), errors="coerce")
    df = df.rename(columns={"hour_bucket": "timestamp"}) if "hour_bucket" in df.columns else df

    sort_cols = ["bloqs_id", "timestamp"]
    if "battery_name" in df.columns:
        sort_cols.insert(1, "battery_name")
    df = df.sort_values(sort_cols).reset_index(drop=True)

    if "battery_serial_number" in df.columns:
        serial = df["battery_serial_number"].astype(str).str.strip()
        df["battery_serial_number"] = serial.mask(serial.isin(["", "N/A", "nan", "None"]), np.nan)

    df = df[df["timestamp"].notna()].copy()
    df = df[(df["soc"].notna()) & (df["soc"] >= 0) & (df["voltage"].notna()) & (df["voltage"] > 0)].copy()
    print(f"Cleaned: {len(df):,} rows | {df['bloqs_id'].nunique()} bloqs")
    return df


def add_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    group_keys = ["bloqs_id"] + (["battery_name"] if "battery_name" in df.columns else [])
    df = df.sort_values(group_keys + ["timestamp"]).reset_index(drop=True)

    if "battery_serial_number" in df.columns:
        df["battery_serial_number"] = df["battery_serial_number"].astype(str).str.strip()
        df.loc[df["battery_serial_number"].isin(["", "N/A", "nan", "None"]), "battery_serial_number"] = np.nan
        df["_serial_filled"] = df.groupby(group_keys, observed=True)["battery_serial_number"].ffill()
        df["_prev_serial"] = df.groupby(group_keys, observed=True)["_serial_filled"].shift(1)
        df["is_new_session"] = (
            df["_serial_filled"].notna() & df["_prev_serial"].notna() &
            (df["_serial_filled"] != df["_prev_serial"])
        ).astype(int)
        df.loc[df.groupby(group_keys, observed=True).head(1).index, "is_new_session"] = 0
        df["session_id"] = df.groupby(group_keys, observed=True)["is_new_session"].cumsum()
        df["battery_serial_number"] = df["_serial_filled"]
        df = df.drop(columns=["_serial_filled", "_prev_serial", "is_new_session"])
    else:
        df["battery_serial_number"] = np.nan
        df["session_id"] = 0

    grp = group_keys + ["session_id"]
    global_end = df["timestamp"].max()
    df["battery_start_time"] = df.groupby(grp, observed=True)["timestamp"].transform("min")
    df["_end_raw"] = df.groupby(grp, observed=True)["timestamp"].transform("max")
    df["is_censored"] = (df["_end_raw"] >= (global_end - pd.Timedelta("1h"))).astype(int)
    df["battery_end_time"] = df["_end_raw"].where(df["is_censored"] == 0, pd.NaT)

    mask = df["battery_end_time"].notna()
    df["days_left"] = np.nan
    df.loc[mask, "days_left"] = (
        (df.loc[mask, "battery_end_time"] - df.loc[mask, "timestamp"])
        .dt.total_seconds() / 86400
    ).round(1)

    for col in ["country", "address", "point_code"]:
        if col not in df.columns:
            df[col] = np.nan

    out_cols = [
        "timestamp", "partner", "bloq_name", "fw_version_type", "bloqs_id",
        "battery_name", "battery_serial_number", "soc", "voltage", "temperature",
        "rents", "days_left", "battery_start_time", "battery_end_time",
        "session_id", "is_censored", "country", "address", "point_code",
    ]
    return df[[c for c in out_cols if c in df.columns]].copy()


def _slope(y: pd.Series) -> float:
    if len(y) < 2:
        return 0.0
    return float(np.polyfit(np.arange(len(y)), y, 1)[0])


def build_features(df_hourly: pd.DataFrame) -> pd.DataFrame:
    df = df_hourly.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["date"] = df["timestamp"].dt.floor("D")
    df["session_id"] = pd.to_numeric(df["session_id"], errors="coerce").fillna(0).astype(int)

    for col in ["soc", "voltage", "temperature", "rents"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["fw_version_type"] = df["fw_version_type"].astype("string")

    daily = (
        df.groupby(["bloqs_id", "battery_name", "session_id", "date"], observed=True, sort=False)
        .agg(
            country=("country", "first"),
            address=("address", "first"),
            point_code=("point_code", "first"),
            fw_version_type=("fw_version_type", "first"),
            soc_mean=("soc", "mean"),
            soc_min=("soc", "min"),
            volt_mean=("voltage", "mean"),
            temp_mean=("temperature", "mean"),
            rents_sum=("rents", "sum"),
        )
        .reset_index()
        .sort_values(["bloqs_id", "battery_name", "session_id", "date"])
        .reset_index(drop=True)
    )

    g = daily.groupby(["bloqs_id", "battery_name", "session_id"], observed=True, sort=False)
    daily["volt_slope_3d"] = g["volt_mean"].transform(lambda s: s.rolling(3, min_periods=2).apply(_slope, raw=False))
    daily["volt_slope_7d"] = g["volt_mean"].transform(lambda s: s.rolling(7, min_periods=3).apply(_slope, raw=False))
    daily["volt_vol_7d"] = g["volt_mean"].transform(lambda s: s.rolling(7, min_periods=3).std()).fillna(0)
    daily["soc_slope_3d"] = g["soc_mean"].transform(lambda s: s.rolling(3, min_periods=2).apply(_slope, raw=False))
    daily["soc_slope_7d"] = g["soc_mean"].transform(lambda s: s.rolling(7, min_periods=3).apply(_slope, raw=False))
    daily["soc_vol_7d"] = g["soc_mean"].transform(lambda s: s.rolling(7, min_periods=3).std()).fillna(0)
    return daily


def score(df_hourly: pd.DataFrame, pipe: Pipeline) -> pd.DataFrame:
    df_clean = clean(df_hourly)
    df_sessions = add_sessions(df_clean)
    daily = build_features(df_sessions)

    risk_today = (
        daily.sort_values("date")
        .groupby(["bloqs_id", "battery_name"], observed=True, sort=False)
        .tail(1)
        .copy()
    )

    scorable = risk_today.dropna(subset=FEATURES_ALL)
    risk_today["risk_prob"] = np.nan
    risk_today.loc[scorable.index, "risk_prob"] = pipe.predict_proba(scorable[FEATURES_ALL])[:, 1]
    risk_today["model_alert"] = (risk_today["risk_prob"] >= ALERT_THRESHOLD).astype(int)

    risk_today["risk_score_voltage"] = np.select(
        [risk_today["volt_mean"] < 12.7, risk_today["volt_mean"].between(12.7, 12.9, inclusive="both")],
        ["Warning", "Early Warning"],
        default="OK",
    )

    export_cols = [
        "date", "bloqs_id", "battery_name", "country", "fw_version_type",
        "soc_mean", "volt_mean", "temp_mean", "rents_sum",
        "risk_score_voltage", "risk_prob", "model_alert", "address", "point_code",
    ]
    return risk_today[[c for c in export_cols if c in risk_today.columns]].sort_values(
        ["model_alert", "risk_prob"], ascending=[False, False]
    )
