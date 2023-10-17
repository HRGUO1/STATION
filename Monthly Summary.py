# Databricks notebook source
# MAGIC %pip install geopy

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import explode, col, lit, when
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id
from geopy.geocoders import Nominatim
from pyspark.sql.functions import udf
from geopy.extra.rate_limiter import RateLimiter
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import math

# COMMAND ----------

base_s3_station_path = "s3://monthly-summary/raw/station.txt"
base_s3_ncdc_path = "s3://monthly-summary/raw/"

# COMMAND ----------

## 1. Data Handling

# COMMAND ----------

#### 1.1 Read Station Dataset 

# COMMAND ----------

station = spark.sparkContext.textFile(base_s3_station_path)

# COMMAND ----------


station_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("elevation", FloatType(), True),
    StructField("station_name", StringType(), True),
    StructField("GSN_flag", StringType(), True),
    StructField("HCN_CRN_flag", StringType(), True)
])


# COMMAND ----------

def parse_line(line):
    fields = line.split()
    station_id = fields[0]
    latitude = float(fields[1])
    longitude = float(fields[2])
    elevation = float(fields[3])

    # Initiate optional fields as None
    GSN_flag = None
    HCN_CRN_flag = None

    # Check the last field to see if it's a digit (HCN_CRN_flag)
    if fields[-1].isdigit():
        HCN_CRN_flag = fields.pop(-1)

    # Check the new last field to see if it's "GSN" (GSN_flag)
    if fields[-1] == 'GSN':
        GSN_flag = fields.pop(-1)

    # The remaining fields make up the station_name
    station_name = ' '.join(fields[4:])

    return (station_id, latitude, longitude, elevation, station_name, GSN_flag, HCN_CRN_flag)


# COMMAND ----------

df_station = spark.createDataFrame(station.map(parse_line), station_schema)

# COMMAND ----------

station_ids = [row.station_id for row in df_station.select('station_id').collect()]
s3_file_paths_1 = [f"s3://monthly-summary/raw/{station_id}.csv" for station_id in station_ids][0:32000]
s3_file_paths_2 = [f"s3://monthly-summary/raw/{station_id}.csv" for station_id in station_ids][32000:64000]
s3_file_paths_3 = [f"s3://monthly-summary/raw/{station_id}.csv" for station_id in station_ids][64000:96000]
s3_file_paths_4 = [f"s3://monthly-summary/raw/{station_id}.csv" for station_id in station_ids][96000:]

# COMMAND ----------

#### 1.2 Read Data Schema for Monthly Summary

# COMMAND ----------

weather_schema = StructType([
    # Core Columns
    StructField("STATION", StringType(), True),
    StructField("DATE", DateType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("TAVG", DoubleType(), True),
    
    # Bonus: Climate Change Metric
    StructField("PRCP", DoubleType(), True)
])

# COMMAND ----------

##### Read Dta by chunck and stored in partitioned dataset

# COMMAND ----------

import boto3
from pyspark.sql import functions as F
s3 = boto3.client('s3')
chunk_size = 500

# COMMAND ----------

missing_files_1 = []
file_chunks_1 = [s3_file_paths_1[i:i + chunk_size] for i in range(0, len(s3_file_paths_1), chunk_size)]
final_df_1 = spark.createDataFrame([], schema=weather_schema)

target_columns = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'TAVG', 'PRCP']

for index, chunk in enumerate(file_chunks_1):
    print(f"Processing chunk {index+1} of {len(file_chunks_1)}")
    
    existing_files = []
    
    for file_path in chunk:
        bucket_name, key = file_path.replace("s3://", "").split("/", 1)
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            existing_files.append(file_path)
        except:
            print(f"File does not exist: {file_path}")
            missing_station_id = key.split('/')[-1].replace('.csv', '')
            missing_files_1.append(missing_station_id)
    
    if existing_files:
        # Read the CSV without specifying the schema
        temp_df = spark.read.option("header", "true").csv(existing_files)
        
        # Add missing columns and fill with null
        for col in target_columns:
            if col not in temp_df.columns:
                temp_df = temp_df.withColumn(col, F.lit(None).cast("string"))
        
        # Use select to specify the columns in the order you need
        temp_df = temp_df.select(*target_columns)
        
        # Union the temporary DataFrame with the final DataFrame
        final_df_1 = final_df_1.unionByName(temp_df)
        
        print(f"Appended chunk {index+1} to final DataFrame")

print(f"List of missing files: {missing_files_1}")



# COMMAND ----------

display(final_df_1)

# COMMAND ----------

final_df_1.write.mode('overwrite').parquet("s3://monthly-summary/final_df_1.parquet")

# COMMAND ----------

missing_files_2 = []
file_chunks_2 = [s3_file_paths_2[i:i + chunk_size] for i in range(0, len(s3_file_paths_2), chunk_size)]
final_df_2 = spark.createDataFrame([], schema=weather_schema)

target_columns = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE','TAVG', 'PRCP']

for index, chunk in enumerate(file_chunks_2):
    print(f"Processing chunk {index+1} of {len(file_chunks_2)}")
    
    existing_files = []
    
    for file_path in chunk:
        bucket_name, key = file_path.replace("s3://", "").split("/", 1)
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            existing_files.append(file_path)
        except:
            print(f"File does not exist: {file_path}")
            missing_station_id = key.split('/')[-1].replace('.csv', '')
            missing_files_2.append(missing_station_id)
    
    if existing_files:
        # Read the CSV without specifying the schema
        temp_df = spark.read.option("header", "true").csv(existing_files)
        
        # Add missing columns and fill with null
        for col in target_columns:
            if col not in temp_df.columns:
                temp_df = temp_df.withColumn(col, F.lit(None).cast("string"))
        
        # Use select to specify the columns in the order you need
        temp_df = temp_df.select(*target_columns)
        
        # Union the temporary DataFrame with the final DataFrame
        final_df_2 = final_df_2.unionByName(temp_df)
        
        print(f"Appended chunk {index+1} to final DataFrame")

print(f"List of missing files: {missing_files_2}")

# COMMAND ----------

final_df_2.write.mode('overwrite').parquet("s3://monthly-summary/final_df_2.parquet")

# COMMAND ----------

missing_files_3 = []
file_chunks_3 = [s3_file_paths_3[i:i + chunk_size] for i in range(0, len(s3_file_paths_3), chunk_size)]
final_df_3 = spark.createDataFrame([], schema=weather_schema)

target_columns = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'TAVG', 'PRCP']

for index, chunk in enumerate(file_chunks_3):
    print(f"Processing chunk {index+1} of {len(file_chunks_3)}")
    
    existing_files = []
    
    for file_path in chunk:
        bucket_name, key = file_path.replace("s3://", "").split("/", 1)
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            existing_files.append(file_path)
        except:
            print(f"File does not exist: {file_path}")
            missing_station_id = key.split('/')[-1].replace('.csv', '')
            missing_files_3.append(missing_station_id)
    
    if existing_files:
        # Read the CSV without specifying the schema
        temp_df = spark.read.option("header", "true").csv(existing_files)
        
        # Add missing columns and fill with null
        for col in target_columns:
            if col not in temp_df.columns:
                temp_df = temp_df.withColumn(col, F.lit(None).cast("string"))
        
        # Use select to specify the columns in the order you need
        temp_df = temp_df.select(*target_columns)
        
        # Union the temporary DataFrame with the final DataFrame
        final_df_3 = final_df_3.unionByName(temp_df)
        
        print(f"Appended chunk {index+1} to final DataFrame")

print(f"List of missing files: {missing_files_3}")

# COMMAND ----------

final_df_3.write.mode('overwrite').parquet("s3://monthly-summary/final_df_3.parquet")

# COMMAND ----------

missing_files_4 = []
file_chunks_4 = [s3_file_paths_4[i:i + chunk_size] for i in range(0, len(s3_file_paths_4), chunk_size)]
final_df_4 = spark.createDataFrame([], schema=weather_schema)

target_columns = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'TAVG', 'PRCP']

for index, chunk in enumerate(file_chunks_4):
    print(f"Processing chunk {index+1} of {len(file_chunks_4)}")
    
    existing_files = []
    
    for file_path in chunk:
        bucket_name, key = file_path.replace("s3://", "").split("/", 1)
        try:
            s3.head_object(Bucket=bucket_name, Key=key)
            existing_files.append(file_path)
        except:
            print(f"File does not exist: {file_path}")
            missing_station_id = key.split('/')[-1].replace('.csv', '')
            missing_files_4.append(missing_station_id)
    
    if existing_files:
        # Read the CSV without specifying the schema
        temp_df = spark.read.option("header", "true").csv(existing_files)
        
        # Add missing columns and fill with null
        for col in target_columns:
            if col not in temp_df.columns:
                temp_df = temp_df.withColumn(col, F.lit(None).cast("string"))
        
        # Use select to specify the columns in the order you need
        temp_df = temp_df.select(*target_columns)
        
        # Union the temporary DataFrame with the final DataFrame
        final_df_4 = final_df_4.unionByName(temp_df)
        
        print(f"Appended chunk {index+1} to final DataFrame")

print(f"List of missing files: {missing_files_4}")

# COMMAND ----------

final_df_4.write.mode('overwrite').parquet("s3://monthly-summary/final_df_4.parquet")

# COMMAND ----------

##### Union dataframes

# COMMAND ----------

assert list(final_df_1.columns) == list(final_df_2.columns), "DataFrames have different columns"
df_12 = final_df_1.union(final_df_2)

# COMMAND ----------

assert list(df_12.columns) == list(final_df_3.columns), "DataFrames have different columns"
df_123 = df_12.union(final_df_3)

# COMMAND ----------

assert list(df_123.columns) == list(final_df_4.columns), "DataFrames have different columns"
combined_df = df_123.union(final_df_4)

# COMMAND ----------

combined_df.display(100)

# COMMAND ----------

### 1.2 Data Transformation

# COMMAND ----------

#### Data Cleaning

# COMMAND ----------

# Data Cleaning
# Those are basic cleaning steps based on assessment goal, but more detailed steps can be adopted for any specific country 
def filter_missing_data(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("TAVG").isNotNull())
    )

def filter_outliers(df: DataFrame) -> DataFrame:
    # Remove records where 'TAVG' exceeds 122 or is below -20
    return df.filter((F.col("TAVG") <= 122) & (F.col("TAVG") >= -20))

def convert_temperatures(df: DataFrame) -> DataFrame:
    # Convert temperatures based on the 'TAVG' column.
    # If 'TAVG' exceeds 50, assume it's in Fahrenheit and convert to Celsius.
    df = df.withColumn(
        "TAVG",
        F.when((F.col("TAVG") > 50), (F.col("TAVG") - 32) * 5 / 9)
        .otherwise(F.col("TAVG"))
    )
    return df


def add_time_and_season_columns(df: DataFrame) -> DataFrame:
    return df.withColumn("YEAR", F.substring("DATE", 1, 4)) \
            .withColumn("MONTH", F.substring("DATE", 6, 2)) \
            .withColumn("SEASON", 
                F.when((F.col("MONTH").between(3, 5)), "Spring")
                .when((F.col("MONTH").between(6, 8)), "Summer")
                .when((F.col("MONTH").between(9, 11)), "Autumn")
                .otherwise("Winter")
            )

def round_off_data(df: DataFrame) -> DataFrame:
    return df.withColumn("TAVG", F.round("TAVG", 2)) \
            .withColumn("PRCP", F.round("PRCP", 2)) 



# COMMAND ----------

processed_df = filter_missing_data(combined_df)
processed_df = filter_outliers(processed_df)
processed_df = convert_temperatures(processed_df)
processed_df = add_time_and_season_columns(processed_df)
processed_df = round_off_data(processed_df)

# COMMAND ----------

processed_df = processed_df.withColumn("YEAR", F.substring("DATE", 1, 4))
processed_df = processed_df.withColumn("MONTH", F.substring("DATE", 6, 2))
processed_df = processed_df.withColumn("SEASON", 
                   F.when((F.col("MONTH").between(3, 5)), "Spring")
                   .when((F.col("MONTH").between(6, 8)), "Summer")
                   .when((F.col("MONTH").between(9, 11)), "Autumn")
                   .otherwise("Winter"))


# COMMAND ----------

processed_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/Full_df.parquet")

# COMMAND ----------

##### Q1: Average seasonal temperature for each season and year where data is available

# COMMAND ----------

seasonal_avg_df = (processed_df.groupBy("YEAR", "SEASON")
                     .agg(F.round(F.avg("TAVG"), 2).alias("AVG_SEASONAL_TEMP"))
                     .orderBy("YEAR", "SEASON"))


# COMMAND ----------

seasonal_avg_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/seasonal_avg.parquet")


# COMMAND ----------

##### Q2: List of weather stations and number of available datapoints

# COMMAND ----------

Ava_df = processed_df.groupBy("YEAR", "SEASON", "STATION").agg(
    round(F.avg("TAVG"),2).alias("average_temperature"),
    F.count("TAVG").alias("datapoints")
)

# COMMAND ----------

Ava_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/Available_dtapoint.parquet")

# COMMAND ----------

##### Q3: Accepts 2 sets of coordinates -prepare partition files

# COMMAND ----------

partition_column = "YEAR"
partition_ranges = [
    (2006, 2023),
    (1763, 1800),
    (1801, 1850),
    (1851, 1900),
    (1901, 1927),
    (1928, 1955),
    (1956, 1975),
    (1976, 1985),
    (1986, 1995),
    (1996, 2005)
]

for start_year, end_year in partition_ranges:
    print(f"Starting partition for years {start_year} to {end_year}...")
    
    # Filter the DataFrame based on the year range
    partitioned_df = processed_df.filter((processed_df[partition_column] >= start_year) & (processed_df[partition_column] <= end_year))
    
    # Define the partition name and S3 path
    partition_name = f"{start_year}_{end_year}"
    partition_path = f's3://monthly-summary/Processed/{partition_name}'
    
    # Write the partition to a Parquet file
    partitioned_df.write.parquet(partition_path, mode='overwrite')
    
    print(f"Successfully completed partition for years {start_year} to {end_year}. Data written to {partition_path}")


# COMMAND ----------

##### Bonus: 1. Identify Country of each station

# COMMAND ----------

geolocator = Nominatim(user_agent="geoapiExercises")
def get_country(latitude, longitude):
    from geopy.geocoders import Nominatim  # Import here
    geolocator = Nominatim(user_agent="kryhunnnn@gmail.com")
    location = geolocator.reverse((latitude, longitude), language='en')
    return location.raw['address']['country']

get_country_udf = udf(get_country)

# COMMAND ----------

# Add a new column for the country
df_with_country = df_station.withColumn("country", get_country_udf("latitude", "longitude"))
df_country = df_with_country

# COMMAND ----------

df_country.write.mode('overwrite').parquet("s3://monthly-summary/Processed/weather_station_ctry.parquet")

# COMMAND ----------

##### Bonus: 2. Change in global temperature/ precipitation

# COMMAND ----------

agg_df = processed_df.groupBy("STATION", "YEAR").agg(
    F.avg("TAVG").alias("avg_temp"),
    F.avg("PRCP").alias("avg_precip")
)

# COMMAND ----------

global_agg_df = agg_df.groupBy("YEAR").agg(
    round(F.avg("avg_temp")).alias("rounded_global_avg_temp"),
    F.avg("avg_precip").alias("global_avg_precip")
).orderBy("YEAR")
global_agg_pandas_df = global_agg_df.toPandas()

# COMMAND ----------

windowSpec = Window().partitionBy().orderBy("YEAR")
global_change_df = global_agg_df.withColumn("prev_rounded_global_avg_temp", F.lag("rounded_global_avg_temp").over(windowSpec))
global_change_df = global_change_df.withColumn("prev_global_avg_precip", F.lag("global_avg_precip").over(windowSpec))
global_change_df = global_change_df.withColumn("temp_change", (global_change_df["rounded_global_avg_temp"] - global_change_df["prev_rounded_global_avg_temp"]))
global_change_df = global_change_df.withColumn("precip_change", (global_change_df["global_avg_precip"] - global_change_df["prev_global_avg_precip"]))

# COMMAND ----------

# Create a heatmap for Year-over-Year Temperature Change
plt.figure(figsize=(15, 8))
sns.heatmap(global_change_pandas_df.pivot("YEAR", "rounded_global_avg_temp", "temp_change"), annot=True, cmap="coolwarm", fmt=".1f")
plt.title('Heatmap of Year-over-Year Change in Global Average Temperature')
plt.xlabel('Rounded Global Average Temperature')
plt.ylabel('Year')
plt.show()
plt.savefig("heatmap_temp_change.png")

# COMMAND ----------

# Create a heatmap for Year-over-Year Precipitation Change
plt.figure(figsize=(15, 8))
sns.heatmap(global_change_pandas_df.pivot("YEAR", "rounded_global_avg_temp", "precip_change"), annot=True, cmap="coolwarm", fmt=".1f")
plt.title('Heatmap of Year-over-Year Change in Global Average Precipitation')
plt.xlabel('Rounded Global Average Temperature')
plt.ylabel('Year')
plt.show()
plt.savefig("heatmap_precip_change.png")

# COMMAND ----------

files = ["heatmap_temp_change.png", "heatmap_precip_change.png"]
for file in files:
    s3.upload_file(file, 'monthly-summary', file)


# COMMAND ----------

##### Q3: Identify Outliers

# COMMAND ----------

##### I already try to deal with abnormal values, trade-offs between data integrity & data accuracy

# COMMAND ----------

def filter_outliers(df: DataFrame) -> DataFrame:
    # Remove records where 'TAVG' exceeds 122 or is below -20
    return df.filter((F.col("TAVG") <= 122) & (F.col("TAVG") >= -20))

def convert_temperatures(df: DataFrame) -> DataFrame:
    # Convert temperatures based on the 'TAVG' column.
    # If 'TAVG' exceeds 50, assume it's in Fahrenheit and convert to Celsius.
    df = df.withColumn(
        "TAVG",
        F.when((F.col("TAVG") > 50), (F.col("TAVG") - 32) * 5 / 9)
        .otherwise(F.col("TAVG"))
    )
    return df

# COMMAND ----------

# Also, if i can have more time to figure out the geopy api problem,i would definitely do the analysis via continents, which are more suitable to do a weather analysis than using a global data.
