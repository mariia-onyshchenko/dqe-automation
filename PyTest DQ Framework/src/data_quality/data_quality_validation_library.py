import pandas as pd


class DataQualityLibrary:
    """
    A library of static methods for performing data quality checks on pandas DataFrames.

    This class is intended to be used in a PyTest-based testing framework to validate
    the quality of data in DataFrames. Each method performs a specific data quality
    check and uses assertions to ensure that the data meets the expected conditions.
    """

    @staticmethod
    def check_duplicates(df, column_names=None):
        if column_names:
            duplicates = df[df.duplicated(subset=column_names, keep=False)]
            assert duplicates.empty, (
                f"duplicate rows found in columns {column_names}:\n{duplicates}"
            )
        else:
            duplicates = df[df.duplicated(keep=False)]
            assert duplicates.empty, (
                f"duplicate rows found in dataset:\n{duplicates}"
            )

    @staticmethod
    def check_count(df1, df2):
        assert len(df1) == len(df2), (
            f"row count mismatch: df1={len(df1)}, df2={len(df2)}"
        )

    @staticmethod
    def check_data_full_data_set(df1, df2):
        merged = df1.merge(df2, how="outer", indicator=True)
        diffs = merged[merged["_merge"] != "both"]

        assert diffs.empty, (
            "data mismatch detected between dataframes.\n"
            f"differences:\n{diffs}"
        )

    @staticmethod
    def check_dataset_is_not_empty(df):
        assert not df.empty, "dataset is empty"

    @staticmethod
    def check_not_null_values(df, column_names=None):
        if column_names is None:
            column_names = df.columns

        for col in column_names:
            null_rows = df[df[col].isna()]
            assert null_rows.empty, (
                f"null values found in column '{col}':\n{null_rows}"
            )
