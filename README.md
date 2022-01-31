# data_engineering_course
Open source data engineering course from Data Talks Club

**Week 1: Introduction & Prerequisites

Introduction to GCP
Docker and docker-compose
Running Postgres locally with Docker
Setting up infrastructure on GCP with Terraform
Preparing the environment for the course
Homework week 1 psql scripts

**Week 2: Data ingestion

Data Lake
Workflow orchestration
Setting up Airflow locally
Ingesting data to GCP with Airflow
Ingesting data to local Postgres with Airflow
Moving data from AWS to GCP (Transfer service)
Homework

**Week 3: Data Warehouse

Goal: Structuring data into a Data Warehouse

Instructor: Ankush

Data warehouse (BigQuery) (25 minutes)
What is a data warehouse solution
What is big query, why is it so fast, Cost of BQ, (5 min)
Partitoning and clustering, Automatic re-clustering (10 min)
Pointing to a location in google storage (5 min)
Loading data to big query & PG (10 min) -- using Airflow operator?
BQ best practices
Misc: BQ Geo location, BQ ML
Alternatives (Snowflake/Redshift)

**Week 4: Analytics engineering

Goal: Transforming Data in DWH to Analytical Views

Basics (15 mins)
What is DBT?
ETL vs ELT
Data modeling
DBT fit of the tool in the tech stack
Usage (Combination of coding + theory) (1:30-1:45 mins)
Anatomy of a dbt model: written code vs compiled Sources
Materialisations: table, view, incremental, ephemeral
Seeds
Sources and ref
Jinja and Macros
Tests
Documentation
Packages
Deployment: local development vs production
DBT cloud: scheduler, sources and data catalog (Airflow)
Google data studio -> Dashboard
Extra knowledge:
DBT cli (local)

**Week 5: Batch processing

Distributed processing (Spark) (40 + ? minutes)
What is Spark, spark cluster (5 mins)
Explaining potential of Spark (10 mins)
What is broadcast variables, partitioning, shuffle (10 mins)
Pre-joining data (10 mins)
use-case
What else is out there (Flink) (5 mins)
Extending Orchestration env (airflow) (30 minutes)
Big query on airflow (10 mins)
Spark on airflow (10 mins)

**Week 6: Streaming

Basics
What is Kafka
Internals of Kafka, broker
Partitoning of Kafka topic
Replication of Kafka topic
Consumer-producer
Schemas (avro)
Streaming
Kafka streams
Kafka connect
Alternatives (PubSub/Pulsar)

**Week 7, 8 & 9: Project

Putting everything we learned to practice
Duration: 2-3 weeks

Upcoming buzzwords
Delta Lake/Lakehouse
Databricks
Apache iceberg
Apache hudi
Data mesh
KSQLDB
Streaming analytics
Mlops
