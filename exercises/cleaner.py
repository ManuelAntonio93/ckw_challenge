from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, to_timestamp, expr, lead, explode, sequence, last, first, when, format_string
from pyspark.sql.window import Window

def initialize_spark():
    """Initialize and return a Spark session."""
    return (SparkSession.builder
            .appName("Data Cleaning and Interpolation")
            .master("local[*]").getOrCreate())

def load_and_prepare_data(spark, file_path):
    """Load data from a Parquet file and add temporary timestamp columns for calculations."""
    df = spark.read.parquet(file_path)
    df = (df.withColumn("temp_measurement", to_timestamp(col("t_measurement").cast("double")))
           .withColumn("temp_report", to_timestamp(col("t_report").cast("double"))))
    return df

def generate_and_merge_dataframes(df):
    """Generate rows for missing timestamps and merge with original dataframe."""
    window_spec = Window.partitionBy("device_id", "signal_name").orderBy("temp_measurement")
    df = df.withColumn("next_measurement_temp", lead("temp_measurement", 1).over(window_spec))
    time_df = (df.filter(col("next_measurement_temp").isNotNull())
                .withColumn("time_diff", unix_timestamp("next_measurement_temp") - unix_timestamp("temp_measurement"))
                .filter(col("time_diff") > 10)
                .withColumn("timestamp", explode(sequence("temp_measurement", "next_measurement_temp", expr("interval 10 seconds"))))
                .withColumn("t_measurement", format_string("%.7f", unix_timestamp("timestamp").cast("double")))
                .withColumn("t_report", format_string("%.7f", unix_timestamp("timestamp").cast("double")-10))
                .select("device_id", "signal_name", "t_measurement", "t_report","temp_measurement"))
    original_df = df.select("device_id", "signal_name", "t_measurement", "t_report", "value","temp_measurement")
    return original_df.unionByName(time_df, allowMissingColumns=True)

def interpolate_and_finalize(df):
    """Interpolate missing values and prepare for output."""
    window_spec = Window.partitionBy("device_id", "signal_name").orderBy("temp_measurement")
    window_spec_rows_between = window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    window_spec_rows_between_reverse = window_spec.rowsBetween(Window.currentRow, Window.unboundedFollowing)
    df = df.withColumn("prev_value", last("value", True).over(window_spec_rows_between))
    df = df.withColumn("next_value", first("value", True).over(window_spec_rows_between_reverse))
    df = df.withColumn("interpolated_value", col("value").isNull())
    return df.withColumn("value", when(col("interpolated_value"), (col("prev_value") + col("next_value")) / 2).otherwise(col("value")))

if __name__ == "__main__":
    
    spark = initialize_spark()
    
    # Load and prepare data
    df = load_and_prepare_data(spark, "./datastore/processed_data")
    
    # Generate missing timestamps and merge them with the original data
    full_df = generate_and_merge_dataframes(df)
    
    # Apply interpolation logic for missing values and finalize the dataframe
    final_df = interpolate_and_finalize(full_df)
    
    # Select necessary columns and write the final DataFrame to a Parquet overwriting existing data
    (final_df.select("t_measurement", "t_report", "device_id", "signal_name", "value")
             .write.parquet("./datastore/cleaned_data", mode="overwrite"))
    
    spark.stop()


