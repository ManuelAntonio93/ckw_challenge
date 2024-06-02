from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, format_string

def initialize_spark():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
        .appName("Data Validation with Detailed Checks")
        .master("local[*]")
        .getOrCreate())

def load_data(spark):
    """Load data for processing."""
    return spark.read.parquet("./datastore/cleaned_data")

def validate_data(df):
    """Perform data validation checks on the dataframe."""
    # Data type checks and null checks
    df = df.withColumn("t_measurement", col("t_measurement").cast("double"))
    df = df.withColumn("t_report", col("t_report").cast("double"))
    df = df.withColumn("value", col("value").cast("float"))

    type_and_null_checks = (col("t_measurement").isNotNull() & col("t_report").isNotNull() & col("value").isNotNull())

    # Range checks based on the provided ranges
    range_checks = (
        ((col("device_id") == "device1") & (col("signal_name") == "energy export") & (col("value").between(50, 70))) |
        ((col("device_id") == "device1") & (col("signal_name") == "energy import") & (col("value").between(10, 20))) |
        ((col("device_id") == "device2") & (col("signal_name") == "energy export") & (col("value").between(80, 90))) |
        ((col("device_id") == "device2") & (col("signal_name") == "energy import") & (col("value").between(0, 10)))
    )

    # Consistency check for timestamps
    consistency_check = col("t_report") < col("t_measurement")

    # Combine all checks into a single logical expression
    all_checks = type_and_null_checks & range_checks & consistency_check

    # Apply all checks and create columns to show results
    df = df.withColumn("all_tests_passed", all_checks)
    df = df.withColumn("test_output", 
                       when(col("all_tests_passed"), "All checks passed")
                       .otherwise(concat_ws("-", 
                                            when(~type_and_null_checks, "Data type or Null value error"),
                                            when(~range_checks, "Range check failed"),
                                            when(~consistency_check, "Consistency check failed"))))
    
    # Get "t_measurement" and "t_report" with all decimals 
    df = (df.withColumn("t_measurement", format_string("%.7f", "t_measurement"))
           .withColumn("t_report", format_string("%.7f", "t_report")))
    
    return df

# Main execution block to ensure this script runs as a standalone program
if __name__ == "__main__":
    
    spark = initialize_spark()
    
    # Load data from a predefined location
    df = load_data(spark)
    
    # Apply validation checks to the data and return the validated DataFrame
    df_validated = validate_data(df)
    
    # Write the validated data to a Parquet file, overwriting any existing file
    df_validated.write.parquet("./datastore/checked_data", mode="overwrite")
    
    df_validated.show(truncate=False)
    
    spark.stop()


