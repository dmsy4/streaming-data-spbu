from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType
import os


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64/"

parquet_path = "parquets/"

schema = StructType([
    StructField("Time", TimestampType(), True),
    StructField("Source", StringType(), True),
    StructField("Text", StringType(), True),
    StructField("Has_Media", BooleanType(), True),
    StructField("Message_ID", LongType(), True)
])

spark = (
    SparkSession
    .builder
    .appName("AnomalyDetection")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

streaming_df = (
    spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)
    .parquet(parquet_path)
)

window_spec = window(col("Time"), "30 seconds")

message_counts = (
    streaming_df
    .withWatermark("Time", "30 seconds")
    .groupBy(window_spec, "Source")
    .agg(count("Message_ID").alias("MessageCount"))
)

query = (
    message_counts
    .writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
