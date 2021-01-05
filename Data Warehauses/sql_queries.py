import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# GLOBAL VARIABLES
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist          VARCHAR,
    auth            VARCHAR, 
    firstName       VARCHAR,
    gender          VARCHAR,   
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           CHAR, 
    location        VARCHAR,
    method          VARCHAR ,
    page            VARCHAR,
    registration    FLOAT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              TIMESTAMP,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    song_id            VARCHAR,
    num_songs          INTEGER,
    title              VARCHAR,
    artist_name        VARCHAR,
    artist_latitude    FLOAT,
    year               INTEGER,
    duration           FLOAT,
    artist_id          VARCHAR,
    artist_longitude   FLOAT,
    artist_location    VARCHAR
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplays
(
    songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time           TIMESTAMP NOT NULL sortkey,
    user_id              INTEGER NOT NULL,
    level                VARCHAR,
    song_id              VARCHAR NOT NULL distkey,
    artist_id            VARCHAR NOT NULL,
    session_id           INTEGER,
    location             VARCHAR,
    user_agent           VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_users
(
    user_id INTEGER PRIMARY KEY distkey,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_songs
(
    song_id     VARCHAR PRIMARY KEY,
    title       VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL sortkey distkey,
    year        INTEGER,
    duration    FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artists
(
    artist_id          VARCHAR PRIMARY KEY distkey,
    name               VARCHAR,
    location           VARCHAR,
    latitude           FLOAT,
    longitude          FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time
(
    start_time    TIMESTAMP PRIMARY KEY sortkey distkey,
    hour          INTEGER,
    day           INTEGER,
    week          INTEGER,
    month         INTEGER,
    year          INTEGER,
    weekday       INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES
# fact Table fact_songplays

songplay_table_insert = ("""
    INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
    se.userId      AS user_id,
    se.level       AS level,
    ss.song_id     AS song_id,
    ss.artist_id   AS artist_id,
    se.sessionId   AS session_id,
    se.location    AS location,
    se.userAgent   AS user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

# dimension table dim_users

user_table_insert = ("""
    INSERT INTO dim_users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
    firstName     AS first_name,
    lastName      AS last_name,
    gender        AS gender,
    level         AS level
    FROM staging_events
    where userId IS NOT NULL
    AND page = 'NextSong';
""")

# dimension table dim_songs 

song_table_insert = ("""
    INSERT INTO dim_songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id as song_id,
    title        AS title,
    artist_id    AS artist_id,
    year         AS year,
    duration     AS duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

# dimension table dim_artists 

artist_table_insert = ("""
    INSERT INTO dim_artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id   AS artist_id,
    artist_name                 AS name,
    artist_location             AS location,
    artist_latitude             AS latitude,
    artist_longitude            AS longitude
    FROM staging_songs
    where artist_id IS NOT NULL;
""")

# dimension table dim_time 

time_table_insert = ("""
    INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
    EXTRACT(hour from ts),
    EXTRACT(day from ts),
    EXTRACT(week from ts),
    EXTRACT(month from ts),
    EXTRACT(year from ts),
    EXTRACT(weekday from ts)
    FROM staging_events
    WHERE page ='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
