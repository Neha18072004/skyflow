"""Quick baseline delay model on features_flight_delay - proves the table is ML-ready.

Split is by time, never random: a random split would train on flights from after the test
flights, which is leakage of a different kind than the feature-level one we guard against.
  * first 28 days are dropped from training: their 28-day history windows are incomplete
  * train: 2024-01-29 .. 2024-03-10, test: 2024-03-11 .. 2024-03-31
"""

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score

from skyflow.db import get_engine

TRAIN_START, TEST_START = "2024-01-29", "2024-03-11"
NUMERIC = ["dep_hour", "day_of_week", "month", "distance_miles", "route_flights_28d",
           "route_delay_rate_28d", "airline_flights_28d", "airline_delay_rate_28d"]
CATEGORICAL = ["airline_code", "origin", "dest"]


def main() -> None:
    df = pd.read_sql("SELECT * FROM features_flight_delay", get_engine())
    df = df[df["flight_date"] >= pd.Timestamp(TRAIN_START).date()]

    # Integer-code the categoricals. Airline (15 values) is passed as a native categorical;
    # airports (300+) exceed HistGradientBoosting's 255-bin categorical limit, so they're plain
    # ordinal codes - the route/airline delay-rate features carry most of that signal anyway.
    for col in CATEGORICAL:
        df[col] = df[col].astype("category").cat.codes
    features = NUMERIC + CATEGORICAL

    is_test = df["flight_date"] >= pd.Timestamp(TEST_START).date()
    train, test = df[~is_test], df[is_test]
    y_train, y_test = train["is_arr_delayed_15"].astype(int), test["is_arr_delayed_15"].astype(int)

    model = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.1, categorical_features=[features.index("airline_code")],
        random_state=42,
    )
    model.fit(train[features], y_train)
    auc = roc_auc_score(y_test, model.predict_proba(test[features])[:, 1])

    # Reference point: how well does the single historical route delay rate rank flights?
    ref = roc_auc_score(y_test, test["route_delay_rate_28d"].fillna(train["is_arr_delayed_15"].mean()))

    print(f"train rows: {len(train):,}  ({train['flight_date'].min()} .. {train['flight_date'].max()})")
    print(f"test rows:  {len(test):,}  ({test['flight_date'].min()} .. {test['flight_date'].max()})")
    print(f"test positive rate: {y_test.mean():.4f}")
    print(f"AUC route_delay_rate_28d alone: {ref:.4f}")
    print(f"AUC gradient-boosted baseline:  {auc:.4f}")


if __name__ == "__main__":
    main()
