import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Initialize "PYSPARK_SUBMIT_ARGS" environment variable to import the Kafka jars
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

def initialize_spark_session():
    """Initialize and return a Spark session configured for Kafka consumption."""
    spark = SparkSession.builder \
        .appName("KafkaConsumer") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.sql.shuffle.partitions", 50) \
        .master("local[*]") \
        .getOrCreate()
    return spark

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

def read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    """Sets up a readStream from Kafka.
    This function configures the read stream with the specified Kafka brokers and topic,
    starting from the earliest available messages and handling any potential data loss situations."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp", "timestampType")

def process_batch(df, epoch_id, log_parquet_path):
    """Processes each microbatch of data received from the Kafka stream.
    Logs the processing of the batch to a Parquet file and writes the data to a consumer data directory."""
    log_to_parquet(spark, log_parquet_path, "INFO", f"Processing batch {epoch_id}")
    df.write.mode("append").parquet("./datastore/consumer_data")

# Main execution block to ensure this script runs as a standalone program
if __name__ == "__main__":
    
    spark = initialize_spark_session()
    log_schema = define_log_schema()
    log_parquet_path = "./datastore/process_logs/"
    kafka_bootstrap_servers = "192.168.1.72:9093"
    kafka_topic = "mytopic"

    # Reading data from Kafka
    df_raw = read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic)
    # Displaying the schema of the DataFrame to the console for verification
    df_raw.printSchema()  

    # Setting up the streaming query to process each batch using a process_batch function
    query = (df_raw.writeStream
        .foreachBatch(lambda df, epoch_id: process_batch(df, epoch_id, log_parquet_path))
        .trigger(availableNow=True)
        .option("checkpointLocation", "./datastore/checkpoint_consumer")
        .start())

    query.awaitTermination()


