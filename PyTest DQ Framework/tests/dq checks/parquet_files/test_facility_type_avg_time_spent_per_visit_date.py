"""
Description: Data Quality checks for facility_type_avg_time_spent_per_visit_date dataset.
Requirement: TICKET-1234
Author: Mariia Onyshchenko
"""

import pytest

pytestmark = [
    pytest.mark.parquet_data,
    pytest.mark.facility_type_avg_time_spent_per_visit_date
]

@pytest.fixture(scope="module")
def facility_type_avg_time_spent_db_data(db_connection):
    query = """
        SELECT f.facility_type,
               v.visit_timestamp::date AS visit_date,
               AVG(v.duration_minutes)::int AS avg_time_spent
        FROM visits v
        JOIN facilities f ON f.id = v.facility_id
        GROUP BY f.facility_type, visit_date
    """
    return db_connection.get_data_sql(query)

def test_dataset_is_not_empty(
    facility_type_avg_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_dataset_is_not_empty(
        facility_type_avg_time_spent_per_visit_date_data
    )


def test_row_count_matches(
    facility_type_avg_time_spent_db_data,
    facility_type_avg_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_count(
        facility_type_avg_time_spent_db_data,
        facility_type_avg_time_spent_per_visit_date_data
    )


def test_full_dataset_match(
    facility_type_avg_time_spent_db_data,
    facility_type_avg_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_data_full_data_set(
        facility_type_avg_time_spent_db_data,
        facility_type_avg_time_spent_per_visit_date_data
    )


def test_no_duplicates(
    facility_type_avg_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_duplicates(
        facility_type_avg_time_spent_per_visit_date_data,
        column_names=["facility_type", "visit_date"]
    )


def test_no_null_values(
    facility_type_avg_time_spent_per_visit_date_data,
    data_quality_library
):
    data_quality_library.check_not_null_values(
        facility_type_avg_time_spent_per_visit_date_data,
        ["facility_type", "visit_date", "avg_time_spent"]
    )
