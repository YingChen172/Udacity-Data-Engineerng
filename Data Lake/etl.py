import configparser
from datetime import datetime
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load and extract song and artist data from source data and save them back to S3

    param spark       : the Spark Session
    param input_data  : the source location of song_data
    param output_data : The destination where the results are saved
            
    """
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs_table"), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists_table"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Load and extract song and artist data from source data and save them back to S3
        
    param spark       : the Spark Session
    param input_data  : the source location of song_data
    param output_data : The destination where the results are saved
            
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong').dropDuplicates()
    
    # created log view 
    df = df.withColumn('songplay_id', monotonically_increasing_id())
    df.createOrReplaceTempView("log_data_table")
    

    # extract columns for users table    
    users_table = spark.sql("""
    SELECT DISTINCT userT.userId   AS user_id, 
           userT.firstName         AS first_name,
           userT.lastName          AS last_name,
           userT.gender            AS gender,
           userT.level             AS level
    FROM log_data_table userT
    WHERE userT.userId IS NOT NULL
    """)
    
    # write users table to parquet files
    #users_table.write.mode('overwrite').parquet(output_data + 'users_table/')
    users_table.write.parquet(os.path.join(output_data,"users_table"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x) / 1000), TimestampType())
    df = df.withColumn("hour", hour(get_timestamp(df.ts))) \
            .withColumn("day", dayofmonth(get_timestamp(df.ts))) \
            .withColumn("week", weekofyear(get_timestamp(df.ts))) \
            .withColumn("month", month(get_timestamp(df.ts))) \
            .withColumn("year", year(get_timestamp(df.ts))) \
            .withColumn("weekday", dayofweek(get_timestamp(df.ts))) \
    
    # extract columns to create time table
    time_table = df.select(["ts", "hour", "day", "week", "month", "year", "weekday"]).withColumnRenamed("ts", "start_time")
    
    # write time table to parquet files partitioned by year and month
    #time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'time_table/')
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"time_table"), mode="overwrite")
    
    # created song view
    song_data_view = input_data + "song_data/*/*/*/*.json"
    Song_df = spark.read.json(song_data_view).dropDuplicates()
    Song_df.createOrReplaceTempView("song_data_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
    SELECT logT.songplay_id                     AS songplay_id,
           to_timestamp(logT.ts/1000)           AS start_time,
           month(to_timestamp(logT.ts/1000))    AS month,
           year(to_timestamp(logT.ts/1000))     AS year,
           logT.userId                          AS user_id,
           logT.level                           AS level,
           songT.song_id                        AS song_id,
           songT.artist_id                      AS artist_id,
           logT.sessionId                       AS session_id,
           logT.location                        AS location,
           logT.userAgent                       AS user_agent
    FROM log_data_table logT
    INNER JOIN song_data_table songT ON logT.artist = songT.artist_name \
    AND logT.song = songT.title \
    AND logT.length =songT.duration
    """)

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays_table/')
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays_table"), mode="overwrite")


def main():
    """
    Load input data (song_data and log_data) from input_data path,
    process the data to extract songs_table, artists_table, users_table, time_table, songplays_table,
    and store the queried data to parquet files to output_data path
    """
    # Create Spark session
    spark = create_spark_session()
    
    # Define input and output paths
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake-bucket/"
    
    # Process the song data
    process_song_data(spark, input_data, output_data)
    
    # Process the log data
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()