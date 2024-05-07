from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, TimestampType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .getOrCreate()

# Define schema for ad impressions JSON data
impressions_schema = StructType() \
    .add("ad_creative_id", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("website", StringType())

# Read ad impressions data from Kafka topic
impressions_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad_impressions_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), impressions_schema).alias("data")) \
    .select("data.*")

# Define schema for clicks/conversions CSV data
clicks_schema = StructType() \
    .add("event_timestamp", TimestampType()) \
    .add("user_id", StringType()) \
    .add("ad_campaign_id", StringType()) \
    .add("conversion_type", StringType())

# Read clicks/conversions data from Kafka topic
clicks_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks_conversions_topic") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), clicks_schema).alias("data")) \
    .select("data.*")

# Perform join operation to correlate ad impressions with clicks/conversions
correlated_df = impressions_df.join(clicks_df, "user_id")

# Write correlated data to Parquet files in Amazon S3
query = correlated_df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://advertiseX/data/") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
