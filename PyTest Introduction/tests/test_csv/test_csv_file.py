import pytest
import re

def test_file_not_empty(read_csv):
    assert not read_csv.empty, "csv file is empty"

@pytest.mark.validate_csv
def test_validate_schema(validate_schema):
    assert validate_schema, "schema does not match expected columns."

@pytest.mark.validate_csv
@pytest.mark.skip(reason="age validation disabled")
def test_age_column_valid(read_csv):
    invalid = read_csv[(read_csv["age"] < 0) | (read_csv["age"] > 100)]
    assert invalid.empty, f"invalid ages: {invalid}"

@pytest.mark.validate_csv
def test_email_column_valid(read_csv):
    pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")
    invalid_rows = []
    for _, row in read_csv.iterrows():
        email = row["email"]
        if not pattern.match(email):
            invalid_rows.append({"id": row["id"], "email": email})
    assert not invalid_rows, (
        "invalid emails:\n" +
        "\n".join([f"id={item['id']}, email={item['email']}" for item in invalid_rows])
    )

@pytest.mark.validate_csv
@pytest.mark.xfail(reason="duplicate rows are expected")
def test_duplicates(read_csv):
    duplicates = read_csv[read_csv.duplicated()]
    assert duplicates.empty, f"duplicates: {duplicates}"

@pytest.mark.parametrize("id,expected_is_active", [(1, False), (2, True)])
def test_active_param(read_csv, id, expected_is_active):
    record = read_csv[read_csv["id"] == id]
    assert not record.empty, f"no record found for id={id}"
    actual = record["is_active"].astype(str).values[0] == "True"
    assert actual == expected_is_active, (
        f"wrong is_active for id={id}. "
        f"expected={expected_is_active}, got={actual}")

def test_active(read_csv):
    record = read_csv[read_csv["id"] == 2]
    assert not record.empty, "no record found for id=2"
    actual = record["is_active"].astype(str).values[0] == "True"
    assert actual, "is_active should be True for id=2"
