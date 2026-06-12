# import the necessary libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into columns using the defined schema
parsed_df = raw_df.select(
    from_json(col("value"), schema).alias("ride")
).select("ride.*")

# Convert timestamp column to TimestampType and add a watermark
df_typed = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

df_typed = df_typed.withWatermark("event_time", "1 minute")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_df = df_typed.groupBy(window(col("event_time"), "5 minutes", "1 minute")).agg(
    sum("fare_amount").alias("total_fare")
)

# Extract window start and end times as separate columns
aggregated_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Define a function to write each batch to a CSV file with column names
def write_batch(batch_df, batch_id):

    print(f"\n===== Batch {batch_id} =====")
    batch_df.show(truncate=False)

    # Save the batch DataFrame as a CSV file with headers included
    batch_df.coalesce(1).write \
    .mode("append") \
    .option("header", True) \
    .csv("outputs/task_3")

# Use foreachBatch to apply the function to each micro-batch
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
