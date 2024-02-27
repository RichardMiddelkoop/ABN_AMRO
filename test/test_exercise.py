import pytest
from chispa.dataframe_comparer import *
from src.ABN_AMRO_exercise.exercise import *
from pyspark.sql import SparkSession


def test_filter_column_by_string():
    """
    Test the filter_column_by_string function.

    This function sets up a SparkSession, creates a DataFrame with sample data,
    calls the filter_column_by_string function with specific parameters,
    and compares the actual result with the expected result using assert_df_equality.

    """
    spark = (SparkSession.builder
            .master("local")
            .appName("chispa")
            .getOrCreate())
    source_data = [
        ("United Kingdom",),
        ("UnitedKingdom",),
        ("Netherlands",),
        ("!#@__UK&&",),
        (None,)
    ]
    source_df = spark.createDataFrame(source_data, ["country"])
    actual_df = filter_column_by_string(source_df, "country", "Netherlands, United Kingdom")
    expected_data = [
        ("United Kingdom",),
        ("Netherlands",)
    ]
    expected_df = spark.createDataFrame(expected_data, ["country"])
    assert_df_equality(actual_df, expected_df)

def test_select_columns_from_df():
    """
    Test the select_columns_from_df function.

    This function sets up a SparkSession, creates a DataFrame with sample data,
    calls the select_columns_from_df function with specific parameters,
    and compares the actual result with the expected result using assert_df_equality.

    """
    spark = (SparkSession.builder
            .master("local")
            .appName("chispa")
            .getOrCreate())
    source_data = [
        ("ABN", " ", "AMRO")
    ]
    source_df = spark.createDataFrame(source_data, ["1","2","3"])
    actual_df = select_columns_from_df(source_df, "1", "3")
    expected_data = [
        ("ABN", "AMRO")
    ]
    expected_df = spark.createDataFrame(expected_data, ["1", "3"])
    assert_df_equality(actual_df, expected_df)

def test_rename_column_from_dict():
    """
    Test the rename_column_from_dict function.

    This function sets up a SparkSession, creates a DataFrame with sample data,
    calls the rename_column_from_dict function with specific parameters,
    and compares the actual result with the expected result using assert_df_equality.

    """
    spark = (SparkSession.builder
            .master("local")
            .appName("chispa")
            .getOrCreate())
    source_data = [
        ("ABN", " ", "AMRO")
    ]
    source_df = spark.createDataFrame(source_data, ["1","2","3"])
    actual_df = rename_column_from_dict(source_df, {"1": "First", "2": "Second", "3":"Third"})
    expected_data = [
        ("ABN", " ", "AMRO")
    ]
    expected_df = spark.createDataFrame(expected_data, ["First", "Second", "Third"])
    assert_df_equality(actual_df, expected_df)