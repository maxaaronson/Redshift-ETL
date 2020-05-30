import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_stage"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_stage"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS event_stage (
    artist_name VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length VARCHAR,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    session_id INT,
    song_title VARCHAR,
    status VARCHAR,
    ts VARCHAR,
    user_agent VARCHAR,
    user_id INT)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS song_stage (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1),
    start_time VARCHAR NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL,
    firstName VARCHAR NOT NULL,
    lastName VARCHAR NOT NULL,
    gender VARCHAR,
    level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration FLOAT)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR NOT NULL,
    artist_location VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    time_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR)
""")

# STAGING TABLES

# copy log records from S3
staging_events_copy = ("""
    copy event_stage from '{}'
    IAM_ROLE '{}'
    format as json '{}'
    region 'us-west-2'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

# copy song data from S3
staging_songs_copy = ("""
    copy song_stage from '{}'
    IAM_ROLE '{}'
    format as json 'auto'
    region 'us-west-2'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
    (start_time, user_id, level, song_id, artist_id, session_id,
    location, user_agent)
    (SELECT events.start_time, events.user_id, events.level,
    songs.song_id, songs.artist_id, events.session_id,
    events.location, events.user_agent
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval
          '1 second' AS start_time, *
          FROM event_stage
          WHERE page='NextSong') events
    LEFT JOIN song_stage songs
    ON events.song_title = songs.title
    AND events.artist_name = songs.artist_name
    AND events.length = songs.duration
    WHERE user_id IS NOT NULL
    AND events.artist_name IS NOT NULL
    AND events.song_title IS NOT NULL)
""")

user_table_insert = ("""INSERT INTO users
    (SELECT DISTINCT
    user_id
    firstName,
    lastName,
    gender,
    level
    FROM event_stage
    WHERE page = 'NextSong'
    AND user_id IS NOT NULL)
""")

song_table_insert = ("""INSERT INTO songs
    (SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
    FROM song_stage)
""")

artist_table_insert = ("""INSERT INTO artists
    (SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    FROM song_stage)
""")

time_table_insert = ("""INSERT INTO time
    (start_time, hour, day, week, month, year, weekday)
    (SELECT DISTINCT
    timestamp 'epoch' + cast(ts as BIGINT)/1000 *
        interval '1 second' as start_time,
    date_part('hour', start_time) as hour,
    date_part('day', start_time) as day,
    date_part('week', start_time) as week,
    date_part('month', start_time) as month,
    date_part('year', start_time) as year,
    date_part('weekday', start_time) as weekday
    FROM event_stage
    WHERE page = 'NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create,
                        artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop, songplay_table_drop,
                      user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert,
                        time_table_insert]
