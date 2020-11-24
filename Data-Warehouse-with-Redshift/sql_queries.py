import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE','ARN') 

log_data = config.get('S3','LOG_DATA') 
log_jsonpath = config.get('S3','LOG_JSONPATH')
song_data = config.get('S3','SONG_DATA') 


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstname VARCHAR,
                                gender VARCHAR,
                                iteminsession INT,
                                lastname VARCHAR,
                                length FLOAT,
                                level VARCHAR,
                                location VARCHAR,
                                method VARCHAR,
                                page VARCHAR,
                                registration VARCHAR,
                                sessionid INT,
                                song VARCHAR,
                                status INT,
                                ts TIMESTAMP,
                                useragent VARCHAR,
                                userid INT
                                );
                                """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                num_songs INT,
                                artist_id VARCHAR,
                                artist_latitude FLOAT,
                                artist_longitude FLOAT,
                                artist_location VARCHAR,
                                artist_name VARCHAR,
                                song_id VARCHAR,
                                title VARCHAR,
                                duration FLOAT,
                                year INT 
                                );
                                """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                        start_time timestamp NOT NULL, 
                        user_id int NOT NULL,
                        level varchar NOT NULL,
                        song_id VARCHAR,
                        artist_id VARCHAR,
                        session_id int NOT NULL,
                        location VARCHAR,
                        user_agent VARCHAR NOT NULL
                        );
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                       user_id int PRIMARY KEY,
                       first_name VARCHAR NOT NULL, 
                       last_name VARCHAR NOT NULL,
                       gender VARCHAR NOT NULL,
                       level VARCHAR NOT NULL
                       );
                       """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR NOT NULL, 
                        artist_id VARCHAR NOT NULL,
                        year int,
                        duration float);
                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                       (artist_id VARCHAR PRIMARY KEY,
                       name VARCHAR NOT NULL, 
                       location VARCHAR,
                       latitude float,
                       longitude float);
                       """)

time_table_create = (""" CREATE TABLE IF NOT EXISTS time
                        (start_time timestamp PRIMARY KEY,
                        hour int NOT NULL,
                        day int NOT NULL, 
                        week int NOT NULL,
                        month int NOT NULL,
                        year int NOT NULL, 
                        weekday int NOT NULL);
                        """)

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                        from {} 
                        credentials 'aws_iam_role={}'
                        region 'us-west-2'
                        format as JSON {}
                        timeformat as 'epochmillisecs';;
                        """).format(log_data, ARN, log_jsonpath)


staging_songs_copy = ("""copy staging_songs 
                        from {} 
                        credentials 'aws_iam_role={}'
                        region 'us-west-2'
                        format as JSON 'auto';
                        """).format(song_data, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays 
                        (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
                        SELECT DISTINCT ts,
                        e.userid,
                        e.level,
                        s.song_id,
                        s.artist_id,
                        e.sessionid,
                        e.location,
                        e.useragent
                        FROM staging_events AS e
                        JOIN staging_songs AS s ON e.artist = s.artist_name
                        WHERE e.page = 'NextSong'
                        """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
                     SELECT DISTINCT userid,
                     firstname,
                     lastname,
                     gender,
                     level
                     FROM staging_events
                     WHERE page = 'NextSong'
                     AND userid IS NOT NULL
                     """)

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
                     SELECT DISTINCT song_id,
                     title,
                     artist_id,
                     year,
                     duration
                     FROM staging_songs
                     WHERE song_id IS NOT NUll
                     """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
                       SELECT DISTINCT artist_id,
                       artist_name,
                       artist_location,
                       artist_latitude,
                       artist_longitude
                       FROM staging_songs
                       WHERE artist_id IS NOT NUll
                       """)

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
                    SELECT DISTINCT ts,
                    EXTRACT(hour FROM ts) as hour,
                    EXTRACT(day FROM ts) as day,
                    EXTRACT(week FROM ts) as week,
                    EXTRACT(month FROM ts) as month,
                    EXTRACT(year FROM ts) as year,
                    EXTRACT(weekday FROM ts) as weekday
                    FROM staging_events
                    WHERE page = 'NextSong' AND ts IS NOT NUll
                    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
