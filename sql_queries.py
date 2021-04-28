import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table  if exists staging_events_table"
staging_songs_table_drop = "drop table  if exists staging_songs_table "
songplay_table_drop = "drop table  if exists songplay_table "
user_table_drop = "drop table  if exists user_table"
song_table_drop = "drop table  if exists song_table"
artist_table_drop = "drop table  if exists artist_table"
time_table_drop = "drop table  if exists time_table "

# CREATE TABLES

staging_events_table_create= ("""create table if not exists 
                                 staging_events_table (artist text,auth text,first_name text,gender text,itemInSession                                        text,last_name text,
                                 length text,level text,location text,method text,page text,registration text,sessionId text,
                                 song text,status text,ts text,user_agent text,userId text)
""")

staging_songs_table_create = ("""create table if not exists 
                                 staging_songs_table (num_songs text,artist_id text,artist_latitude text,artist_location text,
                                 artist_name text,song_id text,title text,duration text,year text)
                             """)

songplay_table_create = ("""
create table if not exists songplay_table (songplay_id int IDENTITY(0,1) primary key, 
                                           start_time timestamp ,
                                           user_id int,
                                           level text, 
                                           song_id int , 
                                           artist_id text , 
                                           session_id int, location text, user_agent text);
""")

song_table_create = ("""create table if not exists song_table (song_id int primary key, title text, artist_id text, year int, duration DOUBLE PRECISION);
""")

user_table_create = (""" create table if not exists user_table (user_id int primary key, first_name text, last_name text, gender text, level text);
""")

artist_table_create = ("""create table if not exists artist_table(artist_id text primary key, name text, location text, latitude text, longitude text);
""")

time_table_create = ("""create table if not exists time_table(start_time timestamp primary key, hour INT, day INT, week INT , month INT, year INT, weekday INT);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events_table from 's3://udacity-dend/log_data'
                           iam_role {}
                           json 's3://udacity-dend/log_json_path.json' ;
                       """).format(config['IAM_ROLE']['ARN'])

staging_songs_copy = (""" copy staging_songs_table from 's3://udacity-dend/song_data'
                          iam_role {}
                          json 'auto';
                     """).format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = (""" insert into songplay_table
                             select timestamp 'epoch' +se.ts/1000 * interval '1 second' ,
                             se.userId,
                             se.level,
                             ss.song_id,
                             ss.artist_id,
                             se.sessionId,
                             ss.artist_location,
                             se.user_agent
                             from 
                             staging_songs_table as ss join staging_events_table as se
                             on (ss.artist_name =se.artist) and (se.song = ss.title) and (ss.duration=se.length) 
                             where se.page = 'NextSong';
""")

user_table_insert = ("""insert into user_table(user_id,first_name,last_name,gender,level)
                        select distinct(userId) as userId,first_name,last_name,gender,level from staging_events_table
                        where userId is not null and page='NextSong'  ;    
                    """)

song_table_insert = ("""insert into song_table(song_id,title,artist_id,year,duration)
                        select  (song_id,title,artist_id,year,duration) from staging_songs_table
                        where song_id is not null;
""")

artist_table_insert = ("""insert into artist_table(artist_id,name,location,latitude,longitude)
                          select artist_id,name,location,latitude,longitude
                          from staging_songs_table where artist_id is not null
""")

time_table_insert = ("""insert into time_table(start_time,hour,day,week,month,year,weekday)
                        select distinct(timestamp 'epoch' +ts/1000 * interval '1 second') as start_time,
                        extract(hour from start_time),
                        extract(day from start_time),
                        extract(week from start_time),
                        extract(month from start_time),
                        extract(year from start_time),
                        extract(weekday from start_time)
                        from staging_event_table
                        where start_time is not null;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
