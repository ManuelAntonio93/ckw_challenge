from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json,col
import time

# Initialize SparkSession with necessary configurations for Kafka
# Create a Spark session with Kafka support
spark = (
    SparkSession
    .builder
    .appName("Streaming from Kafka")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.shuffle.partitions", 50)
    .master("local[*]")
    .getOrCreate()
)

# Define Kafka parameters
kafka_bootstrap_servers = "192.168.1.72:9093"  # Use your Kafka server IP and port
kafka_topic = "mytopic"

# Attempt to read from Kafka
df_raw = (spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp", "timestampType"))

# Define the schema for the JSON data
json_schema = StructType([
    StructField('t_measurement', StringType(), True),
    StructField('t_report', StringType(), True),
    StructField('device_id', StringType(), True),
    StructField('signal_name', StringType(), True),
    StructField('value', FloatType(), True)
])

df_raw = df_raw.withColumn("values_json", from_json(col("value"), json_schema))

# Show the schema of the DataFrame to confirm successful data load
df_raw.printSchema()

# Display few rows to see if data is coming through correctly
df_raw.show(truncate=False)

# Stop the Spark session
spark.stop()
