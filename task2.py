from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

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

# Compute aggregations: total fare and average distance grouped by driver_id
aggregated_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("average_distance")
)

# Define a function to write each batch to a CSV file
def write_batch(batch_df, batch_id):

    print(f"\n===== Batch {batch_id} =====")
    batch_df.show(truncate=False)

    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", True) \
        .csv("outputs/task_2")


# Use foreachBatch to apply the function to each micro-batch
query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
