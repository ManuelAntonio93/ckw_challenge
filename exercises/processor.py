from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import from_json, col, current_timestamp, lit, format_string
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, TimestampType

def initialize_spark_session():
    """ Initialize Spark session for processing. """
    return SparkSession.builder \
        .appName("Kafka Data Processing") \
        .master("local[*]") \
        .getOrCreate()
        
def define_log_schema():
    """Defines and returns the schema used for logging."""
    return StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("level", StringType(), True),
        StructField("message", StringType(), True)
    ])

def log_to_parquet(spark, log_parquet_path, level, message):
    """Creates a log entry and writes it to a Parquet file.
    This function dynamically creates a DataFrame for each log message, which includes the current timestamp,
    log level, and message, and then appends it to the specified Parquet file."""
    try:
        # Create a DataFrame with one row and no columns
        log_df = spark.range(1).select(
            current_timestamp().alias("timestamp"),
            lit(level).alias("level"),
            lit(message).alias("message")
        )
        
        # Write the log DataFrame to Parquet in append mode
        log_df.write.mode("append").parquet(log_parquet_path)
    except Exception as e:
        print(f"Failed to log message: {str(e)}")

def read_and_process_data(spark, data_path, schema):
    """Read and process Parquet files into a DataFrame. 
       It flattens the 'value' struct column that contains the json data"""
    try:
        df = spark.read.parquet(data_path)
        df_processed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp", "timestampType")
        df_final = (df_processed.withColumn("values_json", from_json(col("value"), schema)).select("values_json.*")
                    .withColumn("t_measurement", format_string("%.7f", "t_measurement"))
                    .withColumn("t_report", format_string("%.7f", "t_report")))
        return df_final
    except Exception as e:
        log_to_parquet(spark, "./datastore/process_logs", "ERROR", f"Error processing data: {str(e)}")
        raise

def write_data(df, output_path):
    """Write DataFrame to a Parquet file."""
    try:
        df.write.parquet(output_path, mode='overwrite')
        log_to_parquet(spark, "./datastore/process_logs", "INFO", "Data written successfully to Parquet.")
    except Exception as e:
        log_to_parquet(spark, "./datastore/process_logs", "ERROR", f"Failed to write data: {str(e)}")

# Main execution block to ensure this script runs as a standalone program
if __name__ == "__main__":
    
    spark = initialize_spark_session()
    
    # Define paths for source and output data.
    consumer_data_folder_path = "./datastore/consumer_data"
    processed_data_path = "./datastore/processed_data"
    
    # JSON schema for embedded JSON within Parquet files.
    json_schema = StructType([
        StructField('t_measurement', DoubleType(), True),
        StructField('t_report', DoubleType(), True),
        StructField('device_id', StringType(), True),
        StructField('signal_name', StringType(), True),
        StructField('value', FloatType(), True)
    ])
    
    # Process and load data based on defined schema
    df_processed = read_and_process_data(spark, consumer_data_folder_path, json_schema)
    
    # Check for non-empty DataFrame, display schema, data sample, and write to Parquet
    if df_processed is not None:
        df_processed.printSchema()
        df_processed.show()          
        write_data(df_processed, processed_data_path)
    
    spark.stop()