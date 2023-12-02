from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import year
import os
import datetime
import pandas as pd

# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("raw_preprocessing")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

output_relative_dir = 'data/raw/'

# Check if it exists as it makedir will raise an error if it does exist.
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)

target_dir = 'yellow_data'
if not os.path.exists(output_relative_dir + target_dir):
    os.makedirs(output_relative_dir + target_dir)


# Ensuring that the parquet files within our landing folder are aligned and compatible with one another.
sdf = spark.read.parquet('data/landing/yellow_data')

# Columns which can be converted to integers.
int_cast_cols = ('PULocationID', 'DOLocationID')


# Converting data types and dropping unwanted columns.
for month in range(1, 13):
    input_path = f'data/landing/yellow_data/2022-{str(month).zfill(2)}.parquet'
    output_path = f'data/raw/yellow_data/2022-{str(month).zfill(2)}.parquet'

    print(output_path)

    sdf = spark.read.parquet(input_path)

    columns_to_drop = ('VendorID', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag',
                        'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                        'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee')
    
    sdf = sdf.drop(*columns_to_drop)
    
    # Converting column data types to integers.
    for col in int_cast_cols:
        sdf = sdf.withColumn(
            col,
            F.col(col).cast('integer')
    )

    # Ensuring columns are consistently lower cased.
    consistent_col_casing = [F.col(col_name).alias(col_name.lower()) for col_name in sdf.columns]
    sdf = sdf.select(*consistent_col_casing)

    sdf \
    .coalesce(1) \
    .write \
    .mode('overwrite') \
    .parquet(output_path)



# Reading in weather data.
df = pd.read_csv("data/landing/weather.csv", header=2)

# Getting rid of non alphanumeric characters from the column names for consistency.
df.columns = df.columns.str.replace(r'[^a-zA-Z0-9]', '', regex=True)
df.to_csv("data/raw/weather.csv", index=False)







