#!/bin/bash

echo -e "Creating directories 'data/raw'\n"
mkdir -p ./data/raw

echo -e "Downloading file fhv_tripdata_2019-10.csv.gz into 'data/raw'\n"
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz -P ./data/raw

echo -e "Downloading file taxi_zone_lookup.csv into 'data/raw'...\n"
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv -P ./data/raw

echo -n "Creating infer_fhv_schema.csv in 'data/raw'... "
zcat ./data/raw/fhv_tripdata_2019-10.csv.gz | head -n 1000 | sed 's/\(B[0-9]\+\) */\1/g' > ./data/raw/infer_fhv_schema.csv
echo -e "done\n"

echo -e "Starting python script\n"
python3 py-script
