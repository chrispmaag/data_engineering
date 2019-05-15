# Music Streaming: Data Modeling with Postgres and ETL Pipeline Using Python

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Packages

This project primarily uses pandas and psycopg2 for working with Postgres.

## Motivation

Sparkify would like to create a Postgres database with tables designed to optimize queries on song play analysis.

## Credit

This was part of Udacity's Data Engineering Nanodegree.

## Files

The three main files are:

- **sql_queries.py**, which contains the SQL queries responsible for dropping, creating, and inserting the JSON data into Postgres
- **etl.ipynb**, which walks through the initial steps of the ETL process in a visual manner
- **etl.py**, which wraps everything up into functions for the full ETL process

To run the ETL pipeline, you'll first want to execute `python create_tables.py` in the terminal. This drops tables if they exist and then creates new tables with the appropriate columns. After that, you'll run `python etl.py` to process and insert the log data into the tables. 

## Results

1. Purpose of this database:
    - In order for the analytics team to be able to understand what songs users are listening to, we need to extract the JSON data into Postgres tables. This will allow the analytics team to aggregate and analyze the data using SQL queries.
2. Database schema design and ETL pipeline:
    - We used the star schema with one fact table (songplays) and several dimension tables (songs, artists, time and users) in order to provide the anlytics team with an efficient relational database setup for their queries. With the star schema, users will be able to use simplified queries and have fast aggregations. The ETL pipeline reads in JSON data, filters the log data to records associated with song plays, and then inserts the data into Postgres tables.
