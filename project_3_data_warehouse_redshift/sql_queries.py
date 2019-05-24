import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_events
    (artist varchar, auth varchar, first_name varchar, gender varchar, item_in_session integer, last_name varchar,
    length float, level varchar, location varchar, method varchar, page varchar, registration bigint, session_id integer NOT NULL, song varchar, status integer, ts bigint, user_agent varchar, user_id integer);
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs
    (num_songs integer, artist_id varchar, artist_latitude float, artist_longitude float, artist_location varchar,
    artist_name varchar, song_id varchar, title varchar, duration float, year integer);
""")

songplay_table_create = ("""create table if not exists songplays (songplay_id integer IDENTITY(0,1) PRIMARY KEY, start_time timestamp NOT NULL, user_id integer NOT NULL, level varchar, song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id integer, location varchar, user_agent varchar);
""")

user_table_create = ("""create table if not exists users (user_id integer PRIMARY KEY, first_name varchar NOT NULL, last_name varchar NOT NULL, gender varchar NOT NULL, level varchar NOT NULL);
""")

song_table_create = ("""create table if not exists songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar NOT NULL, year integer, duration float);
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar PRIMARY KEY, artist_name varchar NOT NULL, artist_location varchar, artist_latitude float, artist_longitude float);
""")

time_table_create = ("""create table if not exists time (start_time timestamp PRIMARY KEY, hour integer, day integer, week integer, month integer, year integer, weekday integer);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from 's3://udacity-dend/log-data'
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    credentials 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    JSON 'auto' truncatecolumns;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        select start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
        from (
            select distinct '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
            ,user_id
            ,level
            ,ss.song_id
            ,ss.artist_id
            ,session_id
            ,location
            ,user_agent
            from staging_events se
            left outer join staging_songs ss
              on se.artist = ss.artist_name
              and se.song = ss.title
              and se.length = ss.duration
            where page = 'NextSong'
            and ss.song_id is not null
            and ss.artist_id is not null
        );
""")

user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level)
                        select distinct user_id, first_name, last_name, gender, level
                        from staging_events
                        where page = 'NextSong';
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration)
                        select distinct song_id, title, artist_id, year, duration
                        from staging_songs;
""")

artist_table_insert = ("""insert into artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                          select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                          from staging_songs;
""")


time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday)
                        select start_date as start_time
                        ,date_part(h, start_date) as hour
                        ,date_part(d, start_date) as day
                        ,date_part(w, start_date) as week
                        ,date_part(mon, start_date) as month
                        ,date_part(y, start_date) as year
                        ,date_part(dow, start_date) as weekday
                        from (
                        select distinct '1970-01-01'::date + ts/1000 * interval '1 second' as start_date
                        from staging_events
                        where page = 'NextSong');
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
