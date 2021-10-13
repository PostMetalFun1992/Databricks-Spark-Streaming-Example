# Databricks notebook source
import configparser

secret = dbutils.secrets.get(scope="abfs-access", key="storage-creds")

config = configparser.ConfigParser()
config.read_string(secret)

class AZStorage:
    IN_STORAGE_ACCOUNT = config["INPUT"]["AZ_STORAGE_ACCOUNT"]
    IN_CONTAINER = config["INPUT"]["AZ_CONTAINER"]
    IN_CLIENT_ID = config["INPUT"]["AZ_CLIENT_ID"]
    IN_CLIENT_SECRET = config["INPUT"]["AZ_CLIENT_SECRET"]
    IN_CLIENT_ENDPOINT = config["INPUT"]["AZ_CLIENT_ENDPOINT"]

    OUT_STORAGE_ACCOUNT = config["OUTPUT"]["AZ_STORAGE_ACCOUNT"]
    OUT_CONTAINER = config["OUTPUT"]["AZ_CONTAINER"]
    OUT_CLIENT_ID = config["OUTPUT"]["AZ_CLIENT_ID"]
    OUT_CLIENT_SECRET = config["OUTPUT"]["AZ_CLIENT_SECRET"]
    OUT_CLIENT_ENDPOINT = config["OUTPUT"]["AZ_CLIENT_ENDPOINT"]

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_ID}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_SECRET}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.IN_CLIENT_ENDPOINT}")

spark.conf.set(f"fs.azure.account.auth.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_ID}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_SECRET}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net", f"{AZStorage.OUT_CLIENT_ENDPOINT}")

# COMMAND ----------

IN_STORAGE_URI = f"abfss://{AZStorage.IN_CONTAINER}@{AZStorage.IN_STORAGE_ACCOUNT}.dfs.core.windows.net"
OUT_STORAGE_URI = f"abfss://{AZStorage.OUT_CONTAINER}@{AZStorage.OUT_STORAGE_ACCOUNT}.dfs.core.windows.net"

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import StructType as S, StructField as F
from pyspark.sql.types import IntegerType, DoubleType, StringType

hotel_weather_schema = S([
    F("address", StringType(), True),
    F("avg_tmpr_c", DoubleType(), True),
    F("avg_tmpr_f", DoubleType(), True),
    F("city", StringType(), True),
    F("country", StringType(), True),
    F("geoHash", StringType(), True),
    F("id", StringType(), True),
    F("latitude", DoubleType(), True),
    F("longitude", DoubleType(), True),
    F("name", StringType(), True),
    F("wthr_date", StringType(), True),
    F("year", IntegerType(), True),
    F("month", IntegerType(), True),
    F("day", IntegerType(), True),
])

# COMMAND ----------

hotel_weather_raw_stream = (
    spark.readStream
    .format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .schema(hotel_weather_schema)
    .load(f"{IN_STORAGE_URI}/hotel-weather")
)

# COMMAND ----------

hotel_weather_cleaned_stream = (
    hotel_weather_raw_stream
    .select(col("id"), col("address"), col("country"), col("city"), f.to_timestamp(col("wthr_date")).alias("wthr_timestamp"), col("avg_tmpr_c"))
    .withColumnRenamed("id", "hotel_id") \
    .withColumnRenamed("avg_tmpr_c", "tmpr_c") \
    .withColumnRenamed("address", "hotel_name")
)

# COMMAND ----------

hotels_count_by_city_stream = (
    hotel_weather_cleaned_stream
    .select("wthr_timestamp", "country", "city", "hotel_id")
    .distinct()
    .groupBy(f.window("wthr_timestamp", "1 day"), col("country"), col("city"))
    .agg(f.count("hotel_id").alias("hotels_count"))
)

# COMMAND ----------

(
    hotels_count_by_city_stream
    .writeStream
    .outputMode("complete")
    .format("delta")
    .option("checkpointLocation", "dbfs:/checkpoints/hotels_count_by_city_stream")
    .start(f"{OUT_STORAGE_URI}/hotels_count_by_city")
)

# COMMAND ----------

hotels_count_by_city_table = (
    spark
    .read
    .format("delta")
    .load(f"{OUT_STORAGE_URI}/hotels_count_by_city")
    .select("*")
    .orderBy(col("hotels_count").desc())
)

display(hotels_count_by_city_table)

# COMMAND ----------

weather_count_by_city_stream = (
    hotel_weather_cleaned_stream
    .groupBy(f.window("wthr_timestamp", "1 day"), "country", "city")
    .agg(f.max("tmpr_c").alias("max_tmpr_c"), f.min("tmpr_c").alias("min_tmpr_c"), f.avg("tmpr_c").alias("avg_tmpr_c"))
)

# COMMAND ----------

(
    weather_count_by_city_stream
    .writeStream
    .outputMode("complete")
    .format("delta")
    .option("checkpointLocation", "dbfs:/checkpoints/weather_count_by_city_stream")
    .start(f"{OUT_STORAGE_URI}/weather_count_by_city")
)

# COMMAND ----------

weather_count_by_city_table = (
    spark
    .read
    .format("delta")
    .load(f"{OUT_STORAGE_URI}/weather_count_by_city")
    .select("*")
)

display(weather_count_by_city_table)
