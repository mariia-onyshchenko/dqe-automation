import pytest
from src.connectors.postgres.postgres_connector import PostgresConnectorContextManager
from src.connectors.file_system.parquet_reader import ParquetReader
from src.data_quality.data_quality_validation_library import DataQualityLibrary

def pytest_addoption(parser):
    parser.addoption("--db_host", action="store", default="localhost", help="Database host")
    parser.addoption("--db_port", action="store", default="5434", help="Database port")
    parser.addoption("--db_name", action="store", default="mydatabase", help="Database name")
    parser.addoption("--db_user", action="store", help="Database user")
    parser.addoption("--db_password", action="store", help="Database password")
    parser.addoption("--parquet_root", action="store", default="parquet_data", help="Root folder for parquet files")

@pytest.fixture(scope="session")
def parquet_root(request):
    return request.config.getoption("--parquet_root")

@pytest.fixture(scope="session")
def db_connection(request):
    db_host = request.config.getoption("--db_host")
    db_port = request.config.getoption("--db_port")
    db_name = request.config.getoption("--db_name")
    db_user = request.config.getoption("--db_user")
    db_password = request.config.getoption("--db_password")

    try:
        with PostgresConnectorContextManager(
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_name=db_name,
            db_port=db_port
        ) as db_connector:
            yield db_connector
    except Exception as e:
        pytest.fail(f"failed to initialize PostgresConnectorContextManager: {e}")

@pytest.fixture(scope="session")
def parquet_reader():
    try:
        reader = ParquetReader()
        yield reader
    except Exception as e:
        pytest.fail(f"failed to initialize ParquetReader: {e}")

@pytest.fixture(scope="session")
def data_quality_library():
    try:
        dq_lib = DataQualityLibrary()
        yield dq_lib
    except Exception as e:
        pytest.fail(f"failed to initialize DataQualityLibrary: {e}")

# Example: Use parquet_root in fixtures
@pytest.fixture(scope="module")
def facility_name_min_time_spent_per_visit_date_data(parquet_reader, parquet_root):
    path = f"{parquet_root}/facility_name_min_time_spent_per_visit_date"
    df = parquet_reader.process(path, include_subfolders=True)
    return df

@pytest.fixture(scope="module")
def facility_type_avg_time_spent_per_visit_date_data(parquet_reader, parquet_root):
    path = f"{parquet_root}/facility_type_avg_time_spent_per_visit_date"
    df = parquet_reader.process(path, include_subfolders=True)
    return df

@pytest.fixture(scope="module")
def patient_sum_treatment_cost_per_facility_type_data(parquet_reader, parquet_root):
    path = f"{parquet_root}/patient_sum_treatment_cost_per_facility_type"
    df = parquet_reader.process(path, include_subfolders=True)
    return df
