--Homework
SELECT *
FROM `dtc-de-250122.trips_data_all.fhv_all_partitoned`
WHERE PULocationID IS NOT NULL
LIMIT 10;

--Question 1:
--What is count for fhv vehicles data for year 2019 *
SELECT COUNT(1)
FROM `dtc-de-250122.trips_data_all.fhv_all_partitoned`;
-- 42084899

--Question 2:
--How many distinct dispatching_base_num we have in fhv for 2019 *
SELECT count(total) FROM (
SELECT DISTINCT dispatching_base_num total
FROM `dtc-de-250122.trips_data_all.fhv_all_partitoned`
);
--792

--Question 3:
--Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num *
-- I think filter is partition and order clustering, ill check it manually

-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_non_partitoned`
WHERE (dropoff_datetime >= '2019-03-03' AND dropoff_datetime < '2019-03-04')
ORDER BY dispatching_base_num;
--This query will process 1.6 GiB when run.        Query complete (2.1 sec elapsed, 1.6 GB processed)

-- partitoned_by_dropoff
CREATE OR REPLACE TABLE `dtc-de-250122.trips_data_all.fhv_all_partitoned_by_dropoff`
PARTITION BY DATE(dropoff_datetime)
AS
    SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_non_partitoned`;
-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_partitoned_by_dropoff`
WHERE (dropoff_datetime >= '2019-03-03' AND dropoff_datetime < '2019-03-04')
ORDER BY dispatching_base_num;
--This query will process 1.3 MiB when run   Query complete (0.7 sec elapsed, 1.3 MB processed)

--partitoned by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-250122.trips_data_all.fhv_all_partitoned_by_dispatching`
PARTITION BY dispatching_base_num
AS
    SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_non_partitoned`;
-- ERROR: PARTITION BY expression must be DATE(<timestamp_column>), DATE(<datetime_column>),
--DATETIME_TRUNC(<datetime_column>, DAY/HOUR/MONTH/YEAR), a DATE column,
--TIMESTAMP_TRUNC(<timestamp_column>, DAY/HOUR/MONTH/YEAR), DATE_TRUNC(<date_column>, MONTH/YEAR),
--or RANGE_BUCKET(<int64_column>, GENERATE_ARRAY(<int64_value>, <int64_value>[, <int64_value>]))

--Partition by dropoff_datetime and cluster by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-250122.trips_data_all.fhv_all_part_by_dropoff_clust_by_disp`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num
AS
    SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_non_partitoned`;

-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_part_by_dropoff_clust_by_disp`
WHERE (dropoff_datetime >= '2019-03-03' AND dropoff_datetime < '2019-03-04')
ORDER BY dispatching_base_num;
-- This query will process 1.3 MiB when run. Query complete (0.6 sec elapsed, 1.3 MB processed)

--QUESTION 4
-- What is the count, estimated and actual data processed for query which counts
--trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279
SELECT count(1) FROM `dtc-de-250122.trips_data_all.fhv_all_part_by_dropoff_clust_by_disp`
WHERE (dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31')
   AND (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279');
--This query will process 400.1 MiB when run. Query complete (0.9 sec elapsed, 140.5 MB processed)

SELECT count(1) FROM `dtc-de-250122.trips_data_all.fhv_all`
WHERE (dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31')
   AND (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279');
--This query will process 0 B when run. Query complete (6.9 sec elapsed, 400.1 MB processed)

--QUESTION 5
--What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag
--1. Nothing
-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all`
WHERE (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279') AND SR_Flag IS NULL;
-- Query complete (10.1 sec elapsed, 1.6 GB processed)

--2. Partition by SR_Flag and cluster by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-250122.trips_data_all.fhv_all_part_flag_clust_disp`
PARTITION BY RANGE_BUCKET(SR_Flag, GENERATE_ARRAY(1,43))
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all`;
-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_part_flag_clust_disp`
WHERE (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279') AND SR_Flag IS NULL;
--This query will process 1.3 GiB when run. Query complete (1.0 sec elapsed, 57.5 MB processed)

--1. Nothing
-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all`
WHERE (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279') AND SR_Flag IS NULL;
-- Query complete (10.1 sec elapsed, 1.6 GB processed)

--2. Partition by SR_Flag and cluster by dispatching_base_num
CREATE OR REPLACE TABLE `dtc-de-250122.trips_data_all.fhv_all_part_flag_clust_disp`
PARTITION BY RANGE_BUCKET(SR_Flag, GENERATE_ARRAY(1,43))
CLUSTER BY dispatching_base_num AS
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all`;
-- Check query
SELECT * FROM `dtc-de-250122.trips_data_all.fhv_all_part_flag_clust_disp`
WHERE SR_Flag IS NULL AND (dispatching_base_num = 'B00987' OR dispatching_base_num ='B02060' OR dispatching_base_num = 'B02279') ;
--This query will process 1.3 GiB when run. Query complete (1.0 sec elapsed, 57.5 MB processed)

