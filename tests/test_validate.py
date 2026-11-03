from skyflow.validate import validate_frame

from .conftest import DAY1, raw_frame, raw_row


def results_by_name(report):
    return {r.check_name: r for r in report.results}


def test_clean_partition_passes_every_check(valid_day):
    report = validate_frame(valid_day, DAY1)
    assert all(r.passed for r in report.results), [r for r in report.results if not r.passed]
    assert report.rejected_rows == 0


def test_missing_column_is_critical_and_stops_further_checks(valid_day):
    report = validate_frame(valid_day.drop(columns=["distance_miles"]), DAY1)
    assert [r.check_name for r in report.critical_failures] == ["required_columns"]
    assert len(report.results) == 1


def test_empty_partition_is_critical(valid_day):
    report = validate_frame(valid_day.iloc[0:0], DAY1)
    assert "row_count_nonzero" in {r.check_name for r in report.critical_failures}


def test_duplicate_natural_key_is_critical(valid_day):
    df = raw_frame(valid_day.to_dict("records") + [raw_row(flight_number="1", dest="SFO")])
    check = results_by_name(validate_frame(df, DAY1))["duplicate_natural_key"]
    assert not check.passed and check.failed_rows == 2


def test_null_in_critical_field_is_critical(valid_day):
    valid_day.loc[0, "origin"] = None
    check = results_by_name(validate_frame(valid_day, DAY1))["null_critical_fields"]
    assert not check.passed and check.severity == "critical"


def test_bad_rows_are_rejected_with_a_reason_but_day_still_passes(valid_day):
    valid_day.loc[0, "distance_miles"] = "-5"
    valid_day.loc[1, "arr_delay_min"] = "5000"       # > 2 days late
    report = validate_frame(valid_day, DAY1)

    assert report.critical_failures == []
    assert report.reject_reasons[0] == "distance_positive"
    assert report.reject_reasons[1] == "delay_bounds"
    assert report.rejected_rows == 2                  # 2 / 200 = 1%, right at the cap
    assert results_by_name(report)["reject_rate"].passed


def test_reject_rate_above_threshold_fails_the_day(valid_day):
    valid_day.loc[0:9, "distance_miles"] = "0"       # 10 / 200 = 5% > 1%
    check = results_by_name(validate_frame(valid_day, DAY1))["reject_rate"]
    assert not check.passed and check.failed_rows == 10


def test_non_numeric_and_bad_time_values_are_caught(valid_day):
    valid_day.loc[0, "dep_delay_min"] = "abc"
    valid_day.loc[1, "crs_dep_time"] = "1275"         # 75 minutes is not a time
    report = validate_frame(valid_day, DAY1)
    assert report.reject_reasons[0] == "numeric_types"
    assert report.reject_reasons[1] == "time_format"


def test_flown_flight_without_arrival_is_inconsistent(valid_day):
    valid_day.loc[0, "arr_delay_min"] = None          # not cancelled, not diverted, no arrival
    valid_day.loc[1, "is_cancelled"] = "1.00"         # cancelled but has an arrival delay
    report = validate_frame(valid_day, DAY1)
    assert report.reject_reasons[0] == "status_consistency"
    assert report.reject_reasons[1] == "status_consistency"


def test_cancelled_flight_with_null_times_is_valid(valid_day):
    valid_day.loc[0, ["is_cancelled", "cancellation_code"]] = ["1.00", "B"]
    valid_day.loc[0, ["dep_time", "dep_delay_min", "arr_time", "arr_delay_min",
                      "actual_elapsed_min"]] = None
    assert validate_frame(valid_day, DAY1).rejected_rows == 0
