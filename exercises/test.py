import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from exercises.cleaner import generate_and_merge_dataframes, initialize_spark
from exercises.checker import validate_data

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a Spark session."""
    return initialize_spark()

def create_initial_dataframe(spark):
    """Helper function to create a DataFrame mimicking the data to be cleaned, now including device2 and energy import.
       Includes invalid data instances for testing the validation function."""
    schema = StructType([
        StructField("t_measurement", StringType()),  # Using string to simulate needing cast to timestamp
        StructField("t_report", StringType()),
        StructField("device_id", StringType()),
        StructField("signal_name", StringType()),
        StructField("value", DoubleType())
    ])
    data = [
        ("1717232343.2512069", "1717232333.2512006", "device1", "energy export", 61.0),  # Valid
        ("1717232323.0000000", "1717232303.0000000", "device1", "energy export", 150.0), # Invalid value above range
        ("1717232323.2512069", "1717232323.2512006", "device1", "energy import", 8.0),  # Invalid value below range
        ("1717232323.2512069", "1717232323.2512006", "device2", "energy export", 120.0),  # Invalid value above range
        ("1717232324.0000000", "1717232323.0000000", "device2", "energy import",11.0),   # Invalid value above range
        ("1717232323.2512069", "1717232323.2512006", "device2", "energy export", 85.0),  # Valid
        ("1717232324.0000000", "1717232323.0000000", "device2", "energy import", 5.0),   # Valid
        ("1717232323.2512069", "1717232323.4512076", "device1", "energy export", 65.0)  # Invalid consistency
    ]
    return spark.createDataFrame(data, schema)

def test_generate_and_merge_dataframes(spark):
    """Test the data cleaning function for proper timestamp generation and merging with extended device and signal cases."""
    df = create_initial_dataframe(spark)
    df = (df.withColumn("temp_measurement", to_timestamp(col("t_measurement").cast("double")))
           .withColumn("temp_report", to_timestamp(col("t_report").cast("double"))))
    cleaned_df = generate_and_merge_dataframes(df)

    assert cleaned_df is not None, "The cleaned DataFrame should not be None"
    assert cleaned_df.count() > df.count(), "Cleaned DataFrame should have more rows due to generated timestamps"
    assert cleaned_df.filter(col("t_measurement").isNull() | col("t_report").isNull()).count() == 0, "There should be no null timestamps"
    assert sorted(df.select("device_id", "signal_name").distinct().collect()) == sorted(cleaned_df.select("device_id", "signal_name").distinct().collect()), "All device and signal combinations should be present in the cleaned data"

def test_validate_data(spark):
    """Test the data validation logic with mixed valid and invalid data."""
    df = create_initial_dataframe(spark)
    df = (df.withColumn("t_measurement", to_timestamp(col("t_measurement").cast("double")))
           .withColumn("t_report", to_timestamp(col("t_report").cast("double")))
           .withColumn("value", col("value").cast("double")))

    validated_df = validate_data(df)

    assert validated_df is not None, "The validated DataFrame should not be None"
    # Expected count for invalid entries might need adjustment based on actual validation rules and data.
    expected_invalid_count = 5
    actual_invalid_count = validated_df.filter(col("all_tests_passed") == False).count()
    assert actual_invalid_count == expected_invalid_count, f"Expected {expected_invalid_count} invalid entries, but got {actual_invalid_count}"
