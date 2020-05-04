# Data Modeling with Cassandra


## Project Introduction

A startup called Sparkify wants to analyze the data they have gathered about songs and user activity in their new music streaming app. The analytical goal of this project is to create a database that will help them analyze user activity. In particular, what songs users are listenng to.

## Dataset used in this project
The dataset of this project is a directory of CSV files partitioned by date.

### Here is an example of what a even file looks like.
> event_data/2018-11-08-events.csv


## Project files

- **event_data** folder stores all required data.
- **Project_1B_ Project_Template.ipynb** the code itself.
- **event_datafile_new.csv** An event data CSV file used to insert data into the Apache Cassandra tables.
- **Images** a screenshot of what the denormalized data in the event_datafile_new.csv should look like.
- **README.md** offers discussion of this project.


## Build ETL pipeline
- Iterate through each event file in event_data to process and create a new CSV file in Python
- Create relevant tables as requested, load records into tables
- Use SELECT statements to run queries against the database
- Drop the tables and shut down the cluster

## Start project
Use Project_1B_ Project_Template.ipynb to run example queries.


