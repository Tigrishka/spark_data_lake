# Project 4: Data Lake with Apache Spark

## Project Description
This project is aimed to help a startup called *Sparkify* move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
The challenge is in building an ETL pipeline that extracts data from **AWS S3**, processes the data into analytics tables using **Spark**, and loads them back into S3 as a set of dimensional tables. This will allow *Sparkify* analytics team to continue finding insights in what songs their users are listening to.

## Database Schema
The song data dataset resides in S3 link `s3://udacity-dend/song_data` and populates *songs* and *artists* database tables.
The log data dataset resides in S3 link `s3://udacity-dend/log_data` and is loaded into *users* and *time* tables.<br>
The fact table *songplays* uses information from the *songs* table, *artists* table, and original log files located in the json path `s3://udacity-dend/log_json_path.json`.

###Schema for Song Play Analysis
The star schema optimized for queries on song play analysis includes 
**fact table** - *songplays* (records in log data associated with song plays i.e. records with page)<br>
**dimention tables**
   1. *users* - users in the app,
   2. *songs* - songs in music database,
   3. *artists* - artists in music database,
   4. *time* - timestamps of records in *songplays* broken down into specific units<br>

## Files

**`etl.py`** reads data from S3, processes that data using Spark, and writes them back to S3
**`requirements.txt`** contains libraries used in the pipeline
