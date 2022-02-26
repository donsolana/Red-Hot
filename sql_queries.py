import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
SONG_DATA = config.get("S3", "SONG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
artist varchar,
auth varchar,
first_name varchar,
gender varchar,
item_in_sesssion int,
last_name varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
session_id int,
song varchar,
status varchar,
ts bigint,
user_agent varchar,
user_id int)
diststyle auto;
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
numsongs int,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year int
)
diststyle auto;
""")

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(songplay_id bigint IDENTITY(1,1) PRIMARY KEY, 
 start_time timestamp sortkey,
 user_id int,
 level varchar,
 song_id varchar,
 artist_id varchar,
 session_id int,
 location varchar distkey,
 user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(user_id int PRIMARY KEY sortkey,
firstname varchar,
lastname varchar,
gender varchar,
level varchar)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(song_id varchar PRIMARY KEY sortkey,
title varchar,
artist_id varchar,
year int,
duration numeric)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(artists_id varchar PRIMARY KEY sortkey,
name varchar, 
location varchar,
latitude numeric,
longitude numeric)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(start_time timestamp PRIMARY KEY sortkey,
hour int,
day int,
week int,
month varchar,
year int,
weekday varchar)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events
from {}
iam_role {} 
json {};
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs
from {}
iam_role {}
json 'auto ignorecase';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(
-- Extract songid and artist_id
WITH temp AS (
SELECT artist, song, length, song_id, artist_id
FROM staging_events, staging_songs
WHERE staging_songs.artist_name = staging_events.artist 
      AND staging_songs.title = staging_events.song 
      AND staging_songs.duration = staging_events.length
      )
--- Create final table
SELECT (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') AS start_time,
       user_id,
       level,
       song_id,
       artist_id,
       session_id,
       location,   
       user_agent
FROM staging_events
LEFT JOIN temp 
USING(artist, song, length)
);
""")

user_table_insert = ("""
INSERT INTO users
(SELECT user_id, first_name, last_name, gender, level 
 FROM staging_events
 WHERE page = 'NextSong'
);
""")

song_table_insert = ("""
INSERT INTO songs
(SELECT song_id, title, artist_id, year, duration FROM staging_songs);
""")

artist_table_insert = ("""
INSERT INTO artists
(SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
 FROM staging_songs);
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
(
WITH temp AS 
(SELECT (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second') as start_time
 FROM staging_events)

SELECT start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(DOW FROM start_time) AS weekday
FROM temp)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
