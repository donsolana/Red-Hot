# Music Streaming Data Warehouse with AWS Redshift

## Introduction

In this project, I created a Cloud Datawarehouse for a music streaming platform, by transforming `.json` into a Star Schema for analytical purposes. This project is built for scale and data is ingested directly from cloud storage and spread across multiple nodes. 

`Json` files are used by any modern applications so this projects begins by ingesting these files containing both event data and data attributed to songs on the platform. I used an ELT workflow, where data was first extracted and loaded into Redshift before transformation. 



### Workflow Breakdown

I have broken down this stage into two sections, Namely:

#### 1. Create Tables

1. Defined tables with create statements in `sql_queries.py`.
2. Set up Redshift Clusters and connect.
3. Create tables in Redshift using `create_tables.py` to call table creation statements from `sql_queries.py`.
4. The above process will set up 7 tables, two of which will hold untransformed data. These are named staging tables


### 2. ELT

1. The COPY statement is used to ingest data from S3 buckets into the staging tables.
2. Then the staging tables are transformed and inserted into each of the five tables by loading the result of bespoke queries using the `INSERT INTO` statements.
3. As redshift is a distributed database I also declare a strategy for partioning(dist key) the tables across nodes and sorting(sort key) them within each node. 




### How to run Project

1. Ensure `create_tables.py` ,`etl.py`, `sql_queries.py` and a config file are present. 
2. All files should be under the same folder location, and the config file should contain the endpoint to the S3 bucket where the raw files are kept.
3. The `create_tables.py` file should be ran first. As it creates the database connection and creates tables.
5. etl.py can then be ran. The database connection will be already made and so the `INSERT` statements can run smoothly.



### Schema

As mentioned earlier a star schema was used with five tables Redshift. Namely:

1. Fact Table

**songplays** - records associated with song plays.

***songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent***

2. Dimension Tables

**users** - users in the app
***user_id, first_name, last_name, gender, level***

**songs** - songs in database
***song_id, title, artist_id, year, duration***

**artists** - artists in database
***artist_id, name, location, latitude, longitude***

**time** - time measurement of observations in songplays
***start_time, hour, day, week, month, year, weekday***


### Dataset
The original(raw files) datasets are in the json data format, stored in `AWS S3` buckets, they include

1. ***log data*** : event level information on individual songplays.

2. ***Song data*** : information about the song such as Artist name, song length etc.


### To-do

Add data quality checks.

### Libraries

1. `Boto3` - Python SDK for AWS
2. `psycopg2` - Connecting with data warehouse (Redshift uses standard database drivers)
3. `configparser` - Handings Configuration files

### References

1. https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

2. Milliseconds to timestamp sql
https://stackoverflow.com/questions/7872720/convert-date-from-long-time-postgres
