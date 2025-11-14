import pytest
import pandas as pd
import os


@pytest.fixture(scope="session")
def read_csv():
    """a fixture to read csv file and return its content"""
    path = r'PyTest Introduction\src\data\data.csv'
    df = pd.read_csv(path)
    return df

@pytest.fixture(scope="session")
def validate_schema(read_csv):
    """a fixture to validate the schema"""
    exp_schema = ["id", "name", "age", "email", "is_active"]
    act_schema = list(read_csv.columns)
    return act_schema == exp_schema

def pytest_collection_modifyitems(session, config, items):
    """a hook to dynamically mark tests that do not have explicit marks"""
    for item in items:
        if not item.own_markers:
            item.add_marker("unmarked")

