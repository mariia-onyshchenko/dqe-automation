"""
Description: Data Quality checks for patient_sum_treatment_cost_per_facility_type dataset.
Requirement: TICKET-1234
Author: Mariia Onyshchenko
"""

import pytest

pytestmark = [
    pytest.mark.parquet_data,
    pytest.mark.patient_sum_treatment_cost_per_facility_type
]

# Correct SQL based on actual dataset structure
@pytest.fixture(scope="module")
def patient_sum_treatment_cost_db_data(db_connection):
    query = """
        SELECT
            f.facility_type,
            CONCAT(p.first_name, ' ', p.last_name) AS full_name,
            SUM(v.treatment_cost) AS sum_treatment_cost
        FROM visits v
        JOIN facilities f ON f.id = v.facility_id
        JOIN patients p ON p.id = v.patient_id
        GROUP BY f.facility_type, full_name
    """
    return db_connection.get_data_sql(query)


def test_dataset_is_not_empty(
    patient_sum_treatment_cost_per_facility_type_data,
    data_quality_library
):
    data_quality_library.check_dataset_is_not_empty(
        patient_sum_treatment_cost_per_facility_type_data
    )


def test_row_count_matches(
    patient_sum_treatment_cost_db_data,
    patient_sum_treatment_cost_per_facility_type_data,
    data_quality_library
):
    data_quality_library.check_count(
        patient_sum_treatment_cost_db_data,
        patient_sum_treatment_cost_per_facility_type_data
    )


def test_full_dataset_match(
    patient_sum_treatment_cost_db_data,
    patient_sum_treatment_cost_per_facility_type_data,
    data_quality_library
):
    data_quality_library.check_data_full_data_set(
        patient_sum_treatment_cost_db_data,
        patient_sum_treatment_cost_per_facility_type_data
    )


def test_no_duplicates(
    patient_sum_treatment_cost_per_facility_type_data,
    data_quality_library
):
    data_quality_library.check_duplicates(
        patient_sum_treatment_cost_per_facility_type_data,
        column_names=["facility_type", "full_name"]
    )


def test_no_null_values(
    patient_sum_treatment_cost_per_facility_type_data,
    data_quality_library
):
    data_quality_library.check_not_null_values(
        patient_sum_treatment_cost_per_facility_type_data,
        ["facility_type", "full_name", "sum_treatment_cost"]
    )
