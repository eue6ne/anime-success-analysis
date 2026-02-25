from __future__ import annotations

import warnings
warnings.filterwarnings('ignore')

from pathlib import Path
import os

import numpy as np
import pandas as pd
import mysql.connector
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv


# -------------------------
# Environment
# -------------------------
load_dotenv()

DB = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
}


# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
PROCESSED_DIR.mkdir(parents = True, exist_ok = True)


# -------------------------
# Load from DB
# -------------------------
def load_feature_anime() -> pd.DataFrame:
    conn = mysql.connector.connect(**DB)
    df = pd.read_sql("SELECT * FROM feature_anime;", conn)
    conn.close()
    return df


# -------------------------
# Preprocess
# -------------------------
def build_model_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    df = df.copy()

    # 1) start_date → year
    df["start_date"] = pd.to_datetime(df["start_date"], errors = "coerce")
    df["year"] = df["start_date"].dt.year

    # 2) target: log_members
    df["log_members"] = np.log1p(df["members"])

    # 3) 결측 제거
    df = df.dropna(subset = ["log_members", "score", "year", "type"])

    # 4) 더미화 (핵심: dtype = int로 고정)
    df = pd.get_dummies(df, columns = ["type"], drop_first = True, dtype = int)

    # 5) ipynb 최종 모델과 동일하게 type_TV Special 제거
    if "type_TV Special" in df.columns:
        df = df.drop(columns = ["type_TV Special"])

    # 6) Features
    base_features = [
        "score",
        "genre_count",
        "producer_count",
        "studio_count",
        "voice_actor_count",
        "year",
    ]
    type_features = [c for c in df.columns if c.startswith("type_")]

    features = base_features + type_features

    X = df[features].copy()
    y = df["log_members"].copy()

    # 7) statsmodels OLS 안전장치: object 방지
    X = X.apply(pd.to_numeric, errors = "coerce").fillna(0).astype(float)
    y = pd.to_numeric(y, errors = "coerce").astype(float)

    return X, y


# -------------------------
# Save processed
# -------------------------
def save_splits(
    X: pd.DataFrame,
    y: pd.Series,
    test_size: float = 0.2,
    random_state: int = 42,
) -> None:

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size = test_size,
        random_state = random_state,
    )

    X_train.to_csv(PROCESSED_DIR / "X_train.csv", index = False)
    X_test.to_csv(PROCESSED_DIR / "X_test.csv", index = False)
    y_train.to_csv(PROCESSED_DIR / "y_train.csv", index = False)
    y_test.to_csv(PROCESSED_DIR / "y_test.csv", index = False)

    # 컬럼 목록 저장
    (PROCESSED_DIR / "feature_columns.txt").write_text(
        "\n".join(X.columns.tolist()),
        encoding = "utf-8",
    )


def main():
    df = load_feature_anime()
    X, y = build_model_frame(df)
    save_splits(X, y)

    print("Dataset saved to data/processed/")
    print("X shape:", X.shape)
    print("y shape:", y.shape)


if __name__ == "__main__":
    main()