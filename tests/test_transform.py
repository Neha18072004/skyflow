from datetime import date, datetime

import pandas as pd

from skyflow.transform import arrival_day_offset, hhmm_to_minutes, hhmm_to_time_key, transform_frame

from .conftest import raw_frame, raw_row


def test_hhmm_conversions_handle_midnight_2400():
    s = pd.Series([0, 1435, 2359, 2400])
    assert hhmm_to_minutes(s).tolist() == [0, 875, 1439, 1440]
    assert hhmm_to_time_key(s).tolist() == [0, 1435, 2359, 0]


def test_arrival_day_offset_red_eye_and_short_westbound_hop():
    # LAX 22:00 -> JFK 06:24 next day (324 min block, +3h zone shift) => +1 day
    # ORD 07:00 ET-ish -> 06:55 local, 55 min block, -1h zone shift   => same day
    dep = pd.Series([22 * 60, 7 * 60])
    arr = pd.Series([6 * 60 + 24, 6 * 60 + 55])
    elapsed = pd.Series([324, 55])
    assert arrival_day_offset(dep, arr, elapsed).tolist() == [1, 0]


def test_transform_types_timestamps_and_label():
    out = transform_frame(raw_frame([raw_row()]))
    row = out.iloc[0]
    assert row["flight_date"] == date(2024, 1, 1)
    assert row["airline_name"] == "American Airlines"
    assert row["sched_dep_at"] == datetime(2024, 1, 1, 8, 0)
    assert row["actual_dep_at"] == datetime(2024, 1, 1, 8, 5)
    assert row["sched_arr_at"] == datetime(2024, 1, 1, 11, 30)
    assert row["actual_arr_at"] == datetime(2024, 1, 1, 11, 50)
    assert row["distance_miles"] == 2475
    assert bool(row["is_arr_delayed_15"]) is True


def test_actual_departure_rolls_over_midnight():
    out = transform_frame(raw_frame([raw_row(
        crs_dep_time="2350", dep_time="20", dep_delay_min="30.00",
        crs_arr_time="745", sched_elapsed_min="295.00",
    )]))
    assert out.loc[0, "actual_dep_at"] == datetime(2024, 1, 2, 0, 20)
    assert out.loc[0, "sched_arr_at"] == datetime(2024, 1, 2, 7, 45)


def test_label_boundary_and_missing_outcomes():
    out = transform_frame(raw_frame([
        raw_row(flight_number="1", arr_delay_min="14.00"),
        raw_row(flight_number="2", arr_delay_min="15.00"),   # DOT definition: >= 15 is late
        raw_row(flight_number="3", is_cancelled="1.00", cancellation_code="B",
                dep_time=None, dep_delay_min=None, arr_time=None, arr_delay_min=None,
                actual_elapsed_min=None),
        raw_row(flight_number="4", is_diverted="1.00", arr_time=None, arr_delay_min=None,
                actual_elapsed_min=None),
    ]))
    labels = out["is_arr_delayed_15"].tolist()
    assert labels[0] is False and labels[1] is True
    assert pd.isna(labels[2]) and pd.isna(labels[3])
    assert pd.isna(out.loc[2, "actual_dep_at"])
