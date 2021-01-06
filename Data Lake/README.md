# Data Lake


## Project Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to Build an ETL pipeline that extracts its data from S3, processes it with Spark, and reloads the data into S3 as a set of dimension tables. In this way, the analysis team can continue to gain insight into the songs that users are listening to.

## Project Dataset
The data set of this project consists of files in JSON format, which are in S3.
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

### Here is an example of what a song file, TRAABJL12903CDCF1A.json, looks like.
```json
{
    "num_songs": 1, 
    "artist_id": "ARJIE2Y1187B994AB7", 
    "artist_latitude": null, 
    "artist_longitude": null, 
    "artist_location": "", 
    "artist_name": "Line Renaud", 
    "song_id": "SOUPIRU12A6D4FA1E1", 
    "title": "Der Kleine Dompfaff", 
    "duration": 152.92036, "year": 0
}
```
### Here is an example of what a log file, 2018-11-12-events.json, looks like.
```json
{
    "artist":"Pavement",
    "auth":"Logged In",
    "firstName":"Sylvie",
    "gender":"F",
    "itemInSession":0,
    "lastName":"Cruz",
    "length":99.16036,
    "level":"free",
    "location":"Washington-Arlington-Alexandria, DC-VA-MD-WV",
    "method":"PUT",
    "page":"NextSong",
    "registration":1540266185796.0,
    "sessionId":345,
    "song":"Mercy:The Laundromat",
    "status":200,"ts":1541990258796,
    "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.77.4 (KHTML, like Gecko) Version\/7.0.5 Safari\/537.77.4\"",
    "userId":"10"}
```

## Schema for Song Play Analysis

## Fact Table
### **songplays** - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
## Dimension Tables
### **users** - users in the app
- user_id, first_name, last_name, gender, level
### **songs** - songs in music database
- song_id, title, artist_id, year, duration
### **artists** - artists in music database
- artist_id, name, location, lattitude, longitude
### **time** - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday

## Project files

- **etl.py** reads data from S3, processes that data using Spark and writes them back to S3
- **dl.cfg** contains AWS Credentials
- **README.md** offers discussion of this project.


## Build ETL pipeline

- Load the credentials from dl.cfg
- Load the Data which are in JSON Files(Song Data and Log Data)
- After loading the JSON Files from S3, use Spark process this JSON files and then generate a set of Fact and Dimension Tables
- Load back these dimensional process to S3


## Start project
1. Updated dl.cfg file with AWS Credentials
2. Create AWS S3 bucket and update python etl.py with the correct output path
3. Run "python etl.py" in the terminal