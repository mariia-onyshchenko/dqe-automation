"""
Description: Data Quality checks for facility_name_min_time_spent_per_visit_date dataset.
Requirement: TICKET-1234
Author: Mariia Onyshchenko
"""

import pytest

pytestmark = [
    pytest.mark.parquet_data,
    pytest.mark.facility_name_min_time_spent_per_visit_date
]

@pytest.fixture(scope="module")
def facility_name_min_time_spent_db_data(db_connection):
    query = """
        SELECT f.facility_name,
               v.visit_timestamp::date AS visit_date,
               MIN(v.duration_minutes) AS min_time_spent
        FROM visits v
        JOIN facilities f ON f.id = v.facility_id
        GROUP BY f.facility_name, visit_date
    """
    return db_connection.get_data_sql(query)

def test_dataset_is_not_empty(
    facility_name_min_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_dataset_is_not_empty(
        facility_name_min_time_spent_per_visit_date_data
    )


def test_row_count_matches(
    facility_name_min_time_spent_db_data,
    facility_name_min_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_count(
        facility_name_min_time_spent_db_data,
        facility_name_min_time_spent_per_visit_date_data
    )


def test_full_dataset_match(
    facility_name_min_time_spent_db_data,
    facility_name_min_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_data_full_data_set(
        facility_name_min_time_spent_db_data,
        facility_name_min_time_spent_per_visit_date_data
    )


def test_no_duplicates(
    facility_name_min_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_duplicates(
        facility_name_min_time_spent_per_visit_date_data,
        column_names=["facility_name", "visit_date"]
    )


def test_no_null_values(
    facility_name_min_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_not_null_values(
        facility_name_min_time_spent_per_visit_date_data,
        ["facility_name", "visit_date", "min_time_spent"]
    )
