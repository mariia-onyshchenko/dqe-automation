import pytest
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  
CSV_PATH = BASE_DIR / "src" / "data" / "data.csv"

@pytest.fixture(scope="session")
def read_csv():
    """A fixture to read CSV file and return its content."""
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found at: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    return df


@pytest.fixture(scope="session")
def validate_schema(read_csv):
    """A fixture to validate the schema."""
    exp_schema = ["id", "name", "age", "email", "is_active"]
    act_schema = list(read_csv.columns)
    return act_schema == exp_schema


def pytest_collection_modifyitems(session, config, items):
    """A hook to dynamically mark tests that do not have explicit marks."""
    for item in items:
        if not item.own_markers:
            item.add_marker("unmarked")
