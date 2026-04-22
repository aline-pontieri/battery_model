"""Retrain the battery risk model on the local training CSV and save to Drive."""

import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

import gdrive
import model as m

ROOT = Path(__file__).parent
INPUT_DIR = ROOT / "input"
MODEL_LOCAL = ROOT / "battery_risk_model.joblib"

RANDOM_STATE = 42
ALERT_THRESHOLD = m.ALERT_THRESHOLD


def find_train_csv() -> Path:
    if len(sys.argv) >= 2:
        return Path(sys.argv[1])
    csvs = [f for f in INPUT_DIR.glob("*.csv") if "train" in f.name.lower()]
    if not csvs:
        print("No training CSV found in input/. Filename must contain 'train'.")
        sys.exit(1)
    return sorted(csvs)[-1]


def add_label(daily: pd.DataFrame) -> pd.DataFrame:
    out = daily.copy()
    g = out.groupby(["bloqs_id", "battery_name", "session_id"], observed=True, sort=False)
    out["future_min_soc"] = g["soc_min"].transform(
        lambda s: s.shift(-1).rolling(m.HORIZON, min_periods=1).min()
    )
    out["y_risk"] = (out["future_min_soc"] < m.SOC_THRESHOLD).astype(int)
    return out


def session_split(df_model: pd.DataFrame):
    sessions = (
        df_model[["bloqs_id", "battery_name", "session_id"]]
        .drop_duplicates()
        .sample(frac=1, random_state=RANDOM_STATE)
        .reset_index(drop=True)
    )
    cut = int(0.8 * len(sessions))
    train = df_model.merge(sessions.iloc[:cut], on=["bloqs_id", "battery_name", "session_id"])
    test = df_model.merge(sessions.iloc[cut:], on=["bloqs_id", "battery_name", "session_id"])
    return train, test


def build_pipeline() -> Pipeline:
    numeric = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])
    prep = ColumnTransformer([
        ("num", numeric, m.NUM_FEATURES),
        ("cat", categorical, m.CAT_FEATURES),
    ], remainder="drop")
    return Pipeline([
        ("prep", prep),
        ("clf", LogisticRegression(class_weight="balanced", max_iter=2000, random_state=RANDOM_STATE)),
    ])


def print_metrics(pipe, X_test, y_test):
    p = pipe.predict_proba(X_test)[:, 1]
    yhat = (p >= ALERT_THRESHOLD).astype(int)
    print(f"  ROC AUC:           {roc_auc_score(y_test, p):.3f}")
    print(f"  PR  AUC:           {average_precision_score(y_test, p):.3f}")
    print(f"  Recall:            {recall_score(y_test, yhat, zero_division=0):.3f}")
    print(f"  Precision:         {precision_score(y_test, yhat, zero_division=0):.3f}")
    print(f"  Balanced accuracy: {balanced_accuracy_score(y_test, yhat):.3f}")


def main():
    csv_path = find_train_csv()
    print(f"Training on: {csv_path.name}")

    df = pd.read_csv(csv_path, engine="python", on_bad_lines="skip")
    df_clean = m.clean(df)
    df_sessions = m.add_sessions(df_clean)
    daily = m.build_features(df_sessions)
    daily = add_label(daily)

    df_model = daily.dropna(subset=m.FEATURES_ALL + ["y_risk"]).copy()
    train, test = session_split(df_model)

    print(f"Train rows: {len(train):,} | Test rows: {len(test):,}")
    print(f"Positive rate: {df_model['y_risk'].mean():.3f}")

    pipe = build_pipeline()
    pipe.fit(train[m.FEATURES_ALL], train["y_risk"].astype(int))

    print("\nTest metrics:")
    print_metrics(pipe, test[m.FEATURES_ALL], test["y_risk"].astype(int))

    joblib.dump(pipe, MODEL_LOCAL)
    print(f"\nModel saved: {MODEL_LOCAL}")

    print("Uploading model to Google Drive...")
    service = gdrive.get_service()
    folder_id = gdrive.get_battery_models_folder_id(service)
    gdrive.upload_file(service, MODEL_LOCAL, folder_id)
    print("Done.")


if __name__ == "__main__":
    main()
