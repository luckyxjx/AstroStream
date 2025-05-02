# spark-processor/process_flares.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, when
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, StructField, ArrayType, MapType

# --- Configuration ---
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092') # Kafka broker address inside Docker
KAFKA_TOPIC = 'nasa_flares' # Topic to read from
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://mongo:27017/') # MongoDB connection URI
MONGO_DB = 'astrostream' # MongoDB database name
MONGO_COLLECTION = 'solar_flares' # MongoDB collection name
SPARK_MASTER_URL = os.environ.get('SPARK_MASTER_URL', 'spark://spark-master:7077') # Spark master URL

# --- Define Schema for NASA Flare Data ---
# Updated schema based on typical DONKI FLR structure
instrument_schema = StructType([
    StructField("displayName", StringType(), True)
])

linked_event_schema = StructType([
    StructField("activityID", StringType(), True)
])

flare_schema = StructType([
    StructField("flrID", StringType(), True),
    # Instruments can be an array of objects
    StructField("instruments", ArrayType(instrument_schema), True),
    StructField("beginTime", StringType(), True), # Keep as string for parsing flexibility
    StructField("peakTime", StringType(), True),
    StructField("endTime", StringType(), True),
    StructField("classType", StringType(), True),
    StructField("sourceLocation", StringType(), True),
    StructField("activeRegionNum", DoubleType(), True), # API often returns number, handle potential nulls
    # LinkedEvents can be an array of objects
    StructField("linkedEvents", ArrayType(linked_event_schema), True),
    StructField("note", StringType(), True),
    StructField("catalog", StringType(), True),
    StructField("link", StringType(), True),
    # Add submissionTime if available in your data source
    # StructField("submissionTime", StringType(), True)
])

# --- Spark Session Initialization ---
print("Initializing Spark Session...")
try:
    spark = SparkSession \
        .builder \
        .appName("AstroStreamFlareProcessor") \
        .master(SPARK_MASTER_URL) \
        .config("spark.mongodb.output.uri", f"{MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints/mongo_flare_checkpoint") \
        .getOrCreate()

    # Set log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session initialized successfully.")
except Exception as e:
    print(f"Error initializing Spark Session: {e}")
    exit(1)


# --- Read from Kafka ---
print(f"Reading from Kafka topic: {KAFKA_TOPIC} at broker: {KAFKA_BROKER}")
try:
    kafka_stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    print("Kafka stream DataFrame created.")
except Exception as e:
    print(f"Error creating Kafka stream DataFrame: {e}")
    exit(1)


# --- Process Data ---
# Select the value field (which contains the JSON message), cast it to STRING
# Then parse the JSON string using the defined schema
# Handle potential null values or missing fields gracefully
processed_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), flare_schema).alias("data")) \
    .select("data.*") # Flatten the struct fields into columns

# Add a timestamp for when the event was processed by Spark
processed_df = processed_df.withColumn("processingTimestamp", current_timestamp())

# Clean up potential nulls or empty strings if needed before writing
# Example: Replace null activeRegionNum with 0 or a specific indicator
processed_df = processed_df.withColumn(
    "activeRegionNum",
     when(col("activeRegionNum").isNull(), 0.0).otherwise(col("activeRegionNum"))
)

# Example: Convert date strings to Timestamps (adjust format string as needed)
# Ensure the format matches the API output exactly (e.g., 'yyyy-MM-dd'T'HH:mm' or 'yyyy-MM-dd'T'HH:mm:ss'Z'')
# Note: Timestamp conversion can fail if format is inconsistent; handle errors if necessary
# processed_df = processed_df.withColumn("beginTime", F.to_timestamp(F.col("beginTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
# processed_df = processed_df.withColumn("peakTime", F.to_timestamp(F.col("peakTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
# processed_df = processed_df.withColumn("endTime", F.to_timestamp(F.col("endTime"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))


print("Stream processing logic defined.")

# --- Write to MongoDB ---
# Using foreachBatch for more control, e.g., handling potential write errors or complex logic
print(f"Setting up stream write to MongoDB: {MONGO_URI}{MONGO_DB}.{MONGO_COLLECTION}")

def write_to_mongo(df, epoch_id):
    print(f"Writing batch {epoch_id} to MongoDB...")
    try:
        df.write \
          .format("mongodb") \
          .mode("append") \
          .option("database", MONGO_DB) \
          .option("collection", MONGO_COLLECTION) \
          .save()
        print(f"Successfully wrote batch {epoch_id}.")
    except Exception as e:
        print(f"Error writing batch {epoch_id} to MongoDB: {e}")
        # Consider adding more robust error handling/logging here

try:
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/spark_checkpoints/mongo_flare_checkpoint") \
        .foreachBatch(write_to_mongo) \
        .start()

    print("MongoDB write stream started using foreachBatch. Waiting for termination...")
    query.awaitTermination() # Keep the script running to process the stream

except Exception as e:
    print(f"Error starting or running MongoDB write stream: {e}")
finally:
    print("Stopping Spark Session.")
    spark.stop()
