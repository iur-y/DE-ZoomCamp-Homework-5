# What is this repository?
* My answers for the questions of the fifth module of a Data Engineering course, which you can find [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main)

* For this homework we will be using the FHV 2019-10 data found here. [FHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz)

* And the Zones data found here. [Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)

# How can I run this repository?
#### You need to have PySpark installed. After that:
1. Go to a directory with no Git repository initialized
2. Clone this repository: `git clone https://github.com/iur-y/DE-ZoomCamp-Homework-5.git`
3. Enter the just created directory
4. Check if you have execute permissions on the file `setup.sh`. If you don't, type `chmod 700 setup.sh`
5. Now type `./setup.sh`

# Questions
### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

### Answer: `3.5.0`

### Question 2: 

**FHV October 2019**

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 1MB
- 6MB
- 25MB
- 87MB

### Answer: `6MB`

### Question 3: 

**Count records** 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 108,164
- 12,856
- 452,470
- 62,610

### Answer: `62,610`

### Question 4: 

**Longest trip for each day** 

What is the length of the longest trip in the dataset in hours?

- 631,152.50 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours

### Answer: `631,152.50`

### Question 5: 

**User Interface**

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

### Answer: `4040`

### Question 6: 

**Least frequent pickup location zone**

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?</br>

- East Chelsea
- Jamaica Bay
- Union Sq
- Crown Heights North

### Answer: `Jamaica Bay`
