import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS event_raw"
staging_songs_table_drop  = "DROP TABLE IF EXISTS song_raw"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS event_raw (
                                      artist varchar
                                      ,auth varchar
                                      ,first_name varchar
                                      ,gender varchar
                                      ,item_in_session int
                                      ,last_name varchar
                                      ,length double precision
                                      ,level varchar
                                      ,location varchar
                                      ,method varchar
                                      ,page varchar
                                      ,registration bigint
                                      ,session_id int
                                      ,song varchar
                                      ,status int
                                      ,ts bigint
                                      ,user_agent varchar
                                      ,user_id int
                                  )
                              """)

staging_songs_table_create  = ("""CREATE TABLE IF NOT EXISTS song_raw (
                                      song_id varchar
                                      ,num_songs int
                                      ,title varchar
                                      ,year int
                                      ,duration double precision
                                      ,artist_id varchar
                                      ,artist_name varchar
                                      ,artist_location varchar
                                      ,artist_latitude double precision
                                      ,artist_longitude double precision
                                  )
                              """)

songplay_table_create       = ("""CREATE TABLE IF NOT EXISTS songplays (
                                      songplay_id int identity(0,1) PRIMARY KEY
                                      ,start_time timestamp NOT NULL
                                      ,user_id int NOT NULL
                                      ,level varchar
                                      ,song_id varchar
                                      ,artist_id varchar
                                      ,session_id int
                                      ,location varchar
                                      ,user_agent varchar
                                  )
                              """)

user_table_create           = ("""CREATE TABLE IF NOT EXISTS users (
                                      user_id int PRIMARY KEY
                                      ,first_name varchar
                                      ,last_name varchar
                                      ,gender varchar
                                      ,level varchar
                                  )
                              """)

song_table_create           = ("""CREATE TABLE IF NOT EXISTS songs (
                                      song_id varchar PRIMARY KEY
                                      ,title varchar NOT NULL
                                      ,artist_id varchar
                                      ,year int
                                      ,duration numeric
                                  )
                              """)

artist_table_create         = ("""CREATE TABLE IF NOT EXISTS artists (
                                      artist_id varchar PRIMARY KEY
                                      ,name varchar NOT NULL
                                      ,location varchar
                                      ,latitude double precision
                                      ,longitude double precision
                                  )
                              """)

time_table_create           = ("""CREATE TABLE IF NOT EXISTS time (
                                      start_time timestamp PRIMARY KEY
                                      ,hour int
                                      ,day int
                                      ,week int
                                      ,month int
                                      ,year int
                                      ,weekday int
                                  )
                              """)

# STAGING TABLES

# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json

# copy category
# from 's3://mybucket/category_object_paths.json'
# iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole' 
# json 's3://mybucket/category_jsonpath.json';
# OR json 'auto';
    
staging_events_copy = ("""
                          COPY event_raw 
                          FROM {} 
                          iam_role {}
                          json {};
                      """).format(config.get("S3", "LOG_DATA"), config.get("IAM_ROLE", "ARN"), config.get("S3", "LOG_JSONPATH"))

staging_songs_copy  = ("""
                          COPY song_raw 
                          FROM {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

# https://docs.aws.amazon.com/redshift/latest/dg/r_FROM_clause30.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_Join_examples.html

songplay_table_insert = ("""INSERT INTO 
                                songplays (
                                    start_time
                                    ,user_id
                                    ,level
                                    ,song_id
                                    ,artist_id
                                    ,session_id
                                    ,location
                                    ,user_agent
                                )
                            SELECT
                                TIMESTAMP 'epoch' + er.ts/1000 * INTERVAL '1 second' AS start_time
                                ,er.user_id AS user_id
                                ,er.level AS level
                                ,sr.song_id AS song_id
                                ,sr.artist_id AS artist_id
                                ,er.session_id AS session_id
                                ,er.location AS location
                                ,er.user_agent AS user_agent
                            FROM
                                event_raw AS er
                            LEFT JOIN
                                song_raw AS sr
                            ON 
                                er.artist = sr.artist_id
                            AND
                                er.song = sr.title
                            WHERE
                                er.page = 'NextSong';
                        """)

# https://docs.aws.amazon.com/redshift/latest/dg/t_updating-inserting-using-staging-tables-.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_DISTINCT_examples.html

user_table_insert     = ("""INSERT INTO 
                                users (
                                    user_id
                                    ,first_name
                                    ,last_name
                                    ,gender
                                    ,level
                                )
                            SELECT DISTINCT
                                user_id
                                ,first_name
                                ,last_name
                                ,gender
                                ,level
                            FROM
                                event_raw
                            WHERE
                                page = 'NextSong'
                        """)

song_table_insert     = ("""INSERT INTO
                                songs (
                                    song_id
                                    ,title
                                    ,artist_id
                                    ,year
                                    ,duration
                                )
                            SELECT DISTINCT
                                song_id
                                ,title
                                ,artist_id
                                ,year
                                ,duration
                            FROM
                                song_raw
                            WHERE song_id IS NOT NULL
                        """)

artist_table_insert   = ("""INSERT INTO
                                artists (
                                    artist_id
                                    ,name
                                    ,location
                                    ,latitude
                                    ,longitude
                                )
                            SELECT DISTINCT
                                artist_id
                                ,artist_name
                                ,artist_location
                                ,artist_latitude
                                ,artist_longitude
                            FROM
                                song_raw
                            WHERE artist_id IS NOT NULL
                        """)

# https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html

time_table_insert     = ("""INSERT INTO
                                time (
                                    start_time
                                    ,hour
                                    ,day
                                    ,week
                                    ,month
                                    ,year
                                    ,weekday
                                )
                            SELECT DISTINCT
                                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
                                ,EXTRACT(hour FROM start_time)
                                ,EXTRACT(day FROM start_time)
                                ,EXTRACT(week FROM start_time)
                                ,EXTRACT(month FROM start_time)
                                ,EXTRACT(year FROM start_time)
                                ,EXTRACT(dayofweek FROM start_time)
                            FROM
                                event_raw
                            WHERE start_time IS NOT NULL
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]



