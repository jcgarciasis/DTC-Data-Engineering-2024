#Create an external table using the Green Taxi Trip Records Data for 2022. 

CREATE OR REPLACE EXTERNAL TABLE `iconic-indexer-412617.yellow_cab_data.homework_week3`
OPTIONS (
  format = 'parquet',
  uris = ['gs://iconic-indexer-412617-terra-bucket/green_taxi_2022_.parquet']
)
;

#check green_taxi

Select * from iconic-indexer-412617.yellow_cab_data.homework_week3

#Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table)

CREATE OR REPLACE TABLE `iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned` 
AS SELECT * FROM `iconic-indexer-412617.yellow_cab_data.homework_week3`;

#debug

Select *
From iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned;

#Question 1: What is count of records for the 2022 Green Taxi Data??

Select Count(*)
from iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned;

#Question 2:
#Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
#What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Select COUNT(DISTINCT pulocation_id) from iconic-indexer-412617.yellow_cab_data.homework_week3;

Select COUNT(DISTINCT pulocation_id) from iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned;


#Question 3:
#How many records have a fare_amount of 0?

Select count(*) from iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned
Where fare_amount = 0;

#Question4 - What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
#partition by lpep_pickup_datetime and cluster by pulocation_id

CREATE OR REPLACE TABLE iconic-indexer-412617.yellow_cab_data.yellow_tripdata_partitoned_clustered
PARTITION BY date_column
CLUSTER BY pulocation_id AS 
SELECT date(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', lpep_pickup_datetime)) as date_column, *
 FROM iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned

#debug

Select * from iconic-indexer-412617.yellow_cab_data.yellow_tripdata_partitoned_clustered


#Question5- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
#Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to #the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 

Select DISTINCT(pulocation_id) from iconic-indexer-412617.yellow_cab_data.green_tripdata_non_partitoned
Where lpep_pickup_datetime between '2022-06-01' and '2022-06-30'

#23.24 mb

Select DISTINCT(pulocation_id) from iconic-indexer-412617.yellow_cab_data.yellow_tripdata_partitoned_clustered
Where date_column between '2022-06-01' and '2022-06-30'

##1.12 mb
