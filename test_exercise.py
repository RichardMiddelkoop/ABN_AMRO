import pytest
from chispa.dataframe_comparer import *
import pyspark.sql.functions as F
from exercise import *
from pyspark.sql import SparkSession


def test_filter_column_by_string():
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
    actual_df = source_df.withColumn(
        "clean_country",
        filter_column_by_string(source_df, F.col("country"), "Netherlands, United Kingdom")
    )
    expected_data = [
        ("United Kingdom",),
        ("Netherlands",)
    ]
    expected_df = spark.createDataFrame(expected_data, ["country", "clean_country"])
    assert_df_equality(actual_df, expected_df)