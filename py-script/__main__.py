#!/usr/bin/env python
# coding: utf-8

import subprocess
from ctxmanager import StdOutToFile
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Zoomcamp-Homework") \
        .getOrCreate()

print("\n\nQuestion 1. Spark Version:\n")
print(spark.version)


print("Inferring schema...\n")
get_schema = spark.read.csv("./data/raw/infer_fhv_schema.csv", inferSchema=True, header=True)

get_schema.printSchema()

# The **fhv_tripdata** file has some weird spaces in some rows
# in the columns **dispatching_base_num** and **Affiliated_base_number**.
# I will clean them using **withColumn** and a user-defined function, for practice.

print("Reading fhv_tripdata_2019-10.csv.gz... ", end='')
df = spark.read.csv("./data/raw/fhv_tripdata_2019-10.csv.gz", schema=get_schema.schema, header=True)
print("done\n")

@udf(StringType())
def strip_spaces(col):
    try:
        col = col.strip()
    # if your col is None you get an AttributeError
    except AttributeError:
        pass
    return col

print("Cleaning columns... ", end= '')
df_udf = df \
        .withColumn("dispatching_base_num", strip_spaces("dispatching_base_num")) \
        .withColumn("Affiliated_base_number", strip_spaces("Affiliated_base_number"))

print("done\n")
print("Repartitioning... ", end='')
df_udf = df_udf.repartition(6)
print("done\n")

print("Verifying number of partitions... ")
# Check that it worked
rdd = df_udf.rdd
partitions = rdd.mapPartitions(lambda iterator: [1])

print("Partition count:", partitions.count())
print()

print("Writing partitions to './data/parquet/'... ", end='')
# Write as parquet:
df_udf.write.parquet("./data/parquet/", mode='overwrite')
print("done\n")


print("Question 2. Average size of written partitions:")

with StdOutToFile("temp.txt"):
    result = subprocess.run(["ls", "-l", "./data/parquet/"], capture_output=True, text=True)
    print(result.stdout)

command = 'BEGIN {CONVFMT="%.2f"}; NR > 2 { total_size+=$5 }; END { print total_size/(NR -3)/1024/1024 "MB" }'
result = subprocess.run(["awk", f"{command}", "temp.txt"], capture_output=True, text=True)
print(result.stdout)

subprocess.run(["rm", "temp.txt"])

# Create view in order to write some queries
df_udf.createOrReplaceTempView("taxi_view")

oct_15th_count = spark.sql(
"""
    SELECT
        COUNT(1) AS october_15th_trips
    FROM
        taxi_view
    WHERE
        CAST(pickup_datetime AS DATE) = CAST("2019-10-15" AS DATE)
"""
)
print("Question 3. Count of records for 2019-10-15:")
oct_15th_count.show()

longest_trip = spark.sql(
"""
    SELECT
        TIMESTAMPDIFF(HOUR, pickup_datetime, dropOff_datetime) AS trip_length
    FROM
        taxi_view
    ORDER BY
        trip_length DESC
    LIMIT 1
"""
)

print("Question 4. Longest Trip in Hours:")
longest_trip.show()
print()

print("Question 5. Spark User Interface runs on:")
print("Port 4040\n")

# Read zones:
df_zones = spark.read.csv("./data/raw/taxi_zone_lookup.csv", inferSchema=True, header=True)


# Create view in order to write some queries
df_zones.createOrReplaceTempView("zones")


least_frequent_zone = spark.sql(
"""
    SELECT
        COUNT(1) AS num_trips_per_zone,
        zones.Zone
    FROM
        taxi_view
        INNER JOIN
        zones
        ON
        taxi_view.PUlocationID = zones.LocationID
    GROUP BY
        zones.Zone
    ORDER BY
        1
    LIMIT 1
"""
)
print("Question 6. Least Frequent Pickup Zone:")
least_frequent_zone.show()
print("Success")