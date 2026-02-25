from __future__ import annotations

import warnings
warnings.filterwarnings('ignore')

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error


# -------------------------
# Paths
# -------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data" / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(parents = True, exist_ok = True)


# -------------------------
# Load data
# -------------------------
def load_data():
    X_train = pd.read_csv(DATA_DIR / "X_train.csv")
    X_test = pd.read_csv(DATA_DIR / "X_test.csv")
    y_train = pd.read_csv(DATA_DIR / "y_train.csv").squeeze()
    y_test = pd.read_csv(DATA_DIR / "y_test.csv").squeeze()

    # 전부 numeric화
    X_train = X_train.apply(pd.to_numeric, errors = "coerce").fillna(0).astype(float)
    X_test = X_test.apply(pd.to_numeric, errors = "coerce").fillna(0).astype(float)
    y_train = pd.to_numeric(y_train, errors = "coerce").astype(float)
    y_test = pd.to_numeric(y_test, errors = "coerce").astype(float)

    return X_train, X_test, y_train, y_test


def evaluate(y_true, y_pred) -> tuple[float, float]:
    r2 = r2_score(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared = False)
    return r2, rmse


# -------------------------
# OLS (statsmodels, ipynb 방식)
# -------------------------
def run_ols(X_train, X_test, y_train, y_test):
    X_train_const = sm.add_constant(X_train, has_constant = "add")
    X_test_const = sm.add_constant(X_test, has_constant = "add")

    model = sm.OLS(y_train, X_train_const).fit()
    pred = model.predict(X_test_const)

    r2, rmse = evaluate(y_test, pred)

    print("\n=== OLS (statsmodels) ===")
    print(f"OLS Test R2: {r2:.4f}")
    print(f"OLS Test RMSE: {rmse:.4f}")

    # summary 저장
    (REPORTS_DIR / "ols_summary.txt").write_text(model.summary().as_text(), encoding = "utf-8")

    return model, pred, r2, rmse


# -------------------------
# Ridge (ipynb에서 best alpha = 10 기준)
# -------------------------
def run_ridge(X_train, X_test, y_train, y_test):
    model = Ridge(alpha = 10.0)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    r2, rmse = evaluate(y_test, pred)

    print("\n=== Ridge ===")
    print("Best alpha:", 10.0)
    print(f"Ridge Test R2: {r2:.4f}")
    print(f"Ridge Test RMSE: {rmse:.4f}")

    return model, pred, r2, rmse


# -------------------------
# Random Forest (ipynb 유사 설정)
# -------------------------
def run_rf(X_train, X_test, y_train, y_test):
    model = RandomForestRegressor(n_estimators = 200, random_state = 42, n_jobs = -1, )
    model.fit(X_train, y_train)
    pred = model.predict(X_test)

    r2, rmse = evaluate(y_test, pred)

    print("\n=== Random Forest ===")
    print(f"RF Test R2: {r2:.4f}")
    print(f"RF Test RMSE: {rmse:.4f}")

    importance = pd.DataFrame({"feature": X_train.columns,
                               "importance": model.feature_importances_,
                              }).sort_values(by = "importance", ascending = False)

    print("\nTop 10 Feature Importance:")
    print(importance.head(10))

    importance.to_csv(REPORTS_DIR / "rf_feature_importance.csv", index = False)

    return model, pred, r2, rmse


# -------------------------
# Main
# -------------------------
def main():
    X_train, X_test, y_train, y_test = load_data()

    run_ols(X_train, X_test, y_train, y_test)
    run_ridge(X_train, X_test, y_train, y_test)
    run_rf(X_train, X_test, y_train, y_test)

    print("\nSaved:")
    print("- reports/ols_summary.txt")
    print("- reports/rf_feature_importance.csv")


if __name__ == "__main__":
    main()