import pandas as pd
import geopandas as gpd
import folium
from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
import pyspark
import numpy as np
from matplotlib.ticker import FixedLocator
import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import year
import os
import datetime
import pandas as pd




# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("curating_data")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config('spark.driver.memory', '4g')
    .config('spark.executor.memory', '2g')
    .getOrCreate()
)


output_relative_dir = 'data/curated/'

# Check if it exists as it makedir will raise an error if it does exist.
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)


target_dir = 'yellow_data'
if not os.path.exists(output_relative_dir + target_dir):
    os.makedirs(output_relative_dir + target_dir)

sdf = spark.read.parquet("data/raw/yellow_data/*")

# Creating new column measuring trip time in seconds
sdf = sdf.withColumn("trip_time_sec",                   
            ((F.col("tpep_dropoff_datetime") - F.col("tpep_pickup_datetime"))).cast("int"))

sdf = sdf.where(
        ((F.col('trip_time_sec') > 0)                       # No negative trip times
        & (F.col('trip_time_sec') < 5*60*60)                # No trip times longer than 5 hours
        & (year(F.col('tpep_pickup_datetime')) == 2022)     # No trips outside of 2022
        & (F.col('PULocationID') < 263)                     # No trips outside our location ids
        & (F.col('PULocationID') > 0)                       # No trips outside our location ids
        & (F.col('DOLocationID') < 263)                     # No trips outside our location ids
        & (F.col('DOLocationID') > 0)                       # No trips outside our location ids
        )
    )

sdf \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .parquet('data/curated/yellow_data')



df = pd.read_csv("data/raw/weather.csv")

# Dropping null values
df = df.dropna()
# Dropping values outside of 2022
df['time'] = pd.to_datetime(df['time'])
df = df[df['time'].dt.year == 2022]

# Dropping negative rain
df = df[df['rainmm'] >= 0]
df.to_csv("data/curated/weather.csv", index=False)


