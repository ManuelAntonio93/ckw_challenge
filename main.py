from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

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

# Kafka settings and Kafka topic to read from
kafka_bootstrap_servers = "192.168.1.72:9093"
kafka_topic = "mytopic"
kafka_group_id = "mygroup"

# Reading from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()


# Selecting the required columns from the dataframe and converting the 'value' from binary to string
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp", "timestampType")

# Display the data to console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the stream to finish
query.awaitTermination()
