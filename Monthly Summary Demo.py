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

import random

def generate_random_ranges(num_ranges, lower_bound, upper_bound, max_length):
    random_ranges = []
    for _ in range(num_ranges):
        start = random.randint(lower_bound, upper_bound - max_length)
        end = start + random.randint(1, max_length)
        if end > upper_bound:
            end = upper_bound
        random_ranges.append((start, end))
    return random_ranges

# Parameters
num_ranges = 30  
lower_bound = 1  
upper_bound = 121000  
max_length = 30  

# Generate random ranges
random_ranges = generate_random_ranges(num_ranges, lower_bound, upper_bound, max_length)

print("Generated random ranges:", random_ranges)


# COMMAND ----------

ranges_to_extract = [(76579, 76584), (51065, 51067), (118788, 118799), (71990, 72011), (7014, 7028), (103608, 103620), (109198, 109199), (82627, 82642), (8559, 8567), (54851, 54859), (16663, 16688), (70099, 70104), (112875, 112883), (41699, 41706), (86602, 86626), (82372, 82380), (16901, 16914), (56659, 56686), (73009, 73030), (83371, 83393), (114401, 114418), (51951, 51972), (3286, 3289), (22476, 22498), (7512, 7533), (3310, 3327), (55663, 55686), (80549, 80576), (120917, 120929), (22618, 22645)]

# COMMAND ----------

station_ids = [row.station_id for row in df_station.select('station_id').collect()]
s3_file_paths = [f"s3://monthly-summary/raw/{station_id}.csv" for station_id in station_ids]

demo_paths = []
for start, end in ranges_to_extract:
    demo_paths.extend(s3_file_paths[start:end])

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
chunk_size = 100  #  adjust this value as needed
file_chunks_1 = [demo_paths[i:i + chunk_size] for i in range(0, len(demo_paths), chunk_size)]
final_df_1 = spark.createDataFrame([], schema=weather_schema)

# Define target columns and column aliases
# If i got more time to deeply consider the dataset, i will include TMAX and TIME to do data quality check of TAVG, instead of only choose TAVG. 

# The reason to keep latitude and longitude column (maybe redunant related to the station_id dataset which also contains lat & lont) here beacause i need to do avg temperature by coordinated locations, instead of run joinning query to match two large dataset

target_columns = ['STATION', 'DATE', 'LATITUDE', 'LONGITUDE','TAVG', 'PRCP']


# Process each chunk
for index, chunk in enumerate(file_chunks_1):
    print(f"Processing chunk {index + 1} of {len(file_chunks_1)}")
    
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
        temp_df = spark.read.option("header", "true").csv(existing_files)
        
        # Handle column aliasing
        for original, alias in column_aliases.items():
            if original in temp_df.columns:
                temp_df = temp_df.withColumnRenamed(original, alias)
        
        # Add missing columns and fill with null
        for col in target_columns:
            if col not in temp_df.columns:
                temp_df = temp_df.withColumn(col, F.lit(None).cast("string"))
        
        temp_df = temp_df.select(*target_columns)
        final_df_1 = final_df_1.unionByName(temp_df)
        
        print(f"Appended chunk {index + 1} to final DataFrame")

print(f"List of missing files: {missing_files_1}")



# COMMAND ----------

display(final_df_1)

# COMMAND ----------

final_df_1.write.mode('overwrite').parquet("s3://monthly-summary/final_df_demo.parquet")

# COMMAND ----------

##### Union dataframes if needed for large dataset

# COMMAND ----------

#assert list(final_df_1.columns) == list(final_df_2.columns), "DataFrames have different columns"
#combined_df = final_df_1.union(final_df_2)
combined_df = final_df_1

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

# If i have TMAX and TMIN colum, i will also do:

def filter_temperature_data(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("TMAX") >= F.col("TAVG")) &
        (F.col("TAVG") >= F.col("TMIN"))
    ).filter(
        (F.col("TMAX") > F.col("TAVG")) & (F.col("TAVG") > F.col("TMIN")) |
        (F.col("TMAX") == F.col("TAVG")) & (F.col("TAVG") == F.col("TMIN"))
    )

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

processed_df.show(100)

# COMMAND ----------

print("Total rows in DataFrame:", processed_df.count())

# COMMAND ----------



# COMMAND ----------

processed_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/Full_df_demo.parquet")

# COMMAND ----------

##### Q1: Average seasonal temperature for each season and year where data is available

# COMMAND ----------

seasonal_avg_df = (processed_df.groupBy("YEAR", "SEASON")
                     .agg(F.round(F.avg("TAVG"), 2).alias("AVG_SEASONAL_TEMP"))
                     .orderBy("YEAR", "SEASON"))


# COMMAND ----------

seasonal_avg_df.display()

# COMMAND ----------

seasonal_avg_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/seasonal_avg_demo.parquet")


# COMMAND ----------

##### Q2: List of weather stations and number of available datapoints

# COMMAND ----------

Ava_df = processed_df.groupBy("YEAR", "SEASON", "STATION").agg(
    round(F.avg("TAVG"),2).alias("average_temperature"),
    F.count("TAVG").alias("datapoints")
)

# COMMAND ----------

Ava_df.display()

# COMMAND ----------

Ava_df.write.mode('overwrite').parquet("s3://monthly-summary/Processed/Available_dtapoint_demo.parquet")

# COMMAND ----------

##### Q3: Accepts 2 sets of coordinates -prepare partition files

# COMMAND ----------

# For large dataset only
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

reverse = RateLimiter(geolocator.reverse, min_delay_seconds=5)
get_country_udf = udf(get_country)

# COMMAND ----------

# Add a new column for the country
df_with_country = df_station.withColumn("country", get_country_udf("latitude", "longitude"))
df_with_country.show()
# Due to the rate limit of public API, can't do filter

# COMMAND ----------

# It has rate limit via public geopy api , if i got time i will try to use the other, but geopy can already satisfied bonu goal #1.
# df_filtered = df_with_country.filter(df_with_country.country != "Canada")
# df_filtered.show()

# COMMAND ----------

##### Bonus: 2. Change in global temperature/ precipitation

# COMMAND ----------

from pyspark.sql.window import Window

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

global_change_pandas_df = global_change_df.toPandas()

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
