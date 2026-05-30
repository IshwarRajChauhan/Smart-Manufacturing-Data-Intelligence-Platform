from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import year, month, dayofmonth, to_timestamp

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.json(
    "s3://manufacturing-data-lake-ishwar/raw/"
)

df = df.withColumn(
    "event_time",
    to_timestamp(df.timestamp)
)

df = df.withColumn(
    "year",
    year("event_time")
)

df = df.withColumn(
    "month",
    month("event_time")
)

df = df.withColumn(
    "day",
    dayofmonth("event_time")
)

df.write \
    .mode("append") \
    .partitionBy("year", "month", "day") \
    .parquet(
        "s3://manufacturing-data-lake-ishwar/processed/"
    )