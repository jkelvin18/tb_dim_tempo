import pytest
from lambda_function import parse_event_date, generate_month_data
from datetime import datetime
from dateutil.relativedelta import relativedelta


def test_parse_event_date_with_date():
    event = {"data_ref": "202306"}
    result = parse_event_date(event)
    expected = datetime(2023, 6, 1)
    assert result == expected, f"Expected {expected}, but got {result}"


def test_parse_event_date_without_date():
    event = {}
    result = parse_event_date(event)
    # As it returns the current date plus one month, we just need to verify
    # it's close enough to now plus one month
    expected = datetime.now() + relativedelta(months=1)
    assert (
        abs((result - expected).total_seconds()) < 1
    ), f"Expected around {expected}, but got {result}"


def test_parse_event_date_with_invalid_date():
    event = {"data_ref": "invalid_date"}
    with pytest.raises(ValueError):
        parse_event_date(event)


def test_parse_event_date_with_empty_date():
    event = {"data_ref": ""}
    # It should return the current date plus one month when the date is empty
    result = parse_event_date(event)
    expected = datetime.now() + relativedelta(months=1)
    assert (
        abs((result - expected).total_seconds()) < 1
    ), f"Expected around {expected}, but got {result}"


def test_generate_month_data():
    # Test a month with 30 days
    data = generate_month_data(2023, 4)
    assert (
        len(data) == 30 * 24
    ), "Data length for April should be 30 days * 24 hours"
    assert data[0]["dat_temp"] == datetime(
        2023, 4, 1, 0
    ), "First timestamp should be midnight on April 1"
    assert data[-1]["dat_temp"] == datetime(
        2023, 4, 30, 23
    ), "Last timestamp should be 23:00 on April 30"

    # Test a month with 31 days
    data = generate_month_data(2023, 5)
    assert (
        len(data) == 31 * 24
    ), "Data length for May should be 31 days * 24 hours"
    assert data[0]["dat_temp"] == datetime(
        2023, 5, 1, 0
    ), "First timestamp should be midnight on May 1"
    assert data[-1]["dat_temp"] == datetime(
        2023, 5, 31, 23
    ), "Last timestamp should be 23:00 on May 31"

    # Test a February in a leap year
    data = generate_month_data(2024, 2)
    assert (
        len(data) == 29 * 24
    ), "Data length for February in a leap year should be 29 days * 24 hours"
    assert data[0]["dat_temp"] == datetime(
        2024, 2, 1, 0
    ), "First timestamp should be midnight on February 1"
    assert data[-1]["dat_temp"] == datetime(
        2024, 2, 29, 23
    ), "Last timestamp should be 23:00 on February 29"

    # Test a February in a non-leap year
    data = generate_month_data(2023, 2)
    assert (
        len(data) == 28 * 24
    ), "Data length for February in a non-leap year should be 28 days * 24 hours"
    assert data[0]["dat_temp"] == datetime(
        2023, 2, 1, 0
    ), "First timestamp should be midnight on February 1"
    assert data[-1]["dat_temp"] == datetime(
        2023, 2, 28, 23
    ), "Last timestamp should be 23:00 on February 28"
