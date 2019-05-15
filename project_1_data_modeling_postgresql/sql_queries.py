# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""create table if not exists songplays (songplay_id SERIAL PRIMARY KEY, start_time time NOT NULL, userId int NOT NULL,
                            level varchar NOT NULL, song_id varchar, artist_id varchar, sessionId int NOT NULL, location varchar,
                            userAgent varchar NOT NULL);
""")

user_table_create = ("""create table if not exists users (user_id int PRIMARY KEY, first_name varchar NOT NULL, last_name varchar NOT NULL, gender varchar NOT NULL, level varchar NOT NULL);
""")

song_table_create = ("""create table if not exists songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar NOT NULL, year int NOT NULL, duration float NOT NULL);
""")

artist_table_create = ("""create table if not exists artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar, latitude float, longitude float);
""")

time_table_create = ("""create table if not exists time (start_time time NOT NULL, hour int NOT NULL, day int NOT NULL, week int NOT NULL, month int NOT NULL, year int NOT NULL, weekday int NOT NULL);
""")

# INSERT RECORDS

# Can omit songplay_id on the insert into because serial will auto increment this
songplay_table_insert = ("""insert into songplays (start_time, userId, level, song_id, artist_id,
                            sessionId, location, userAgent) values (%s, %s, %s, %s, %s, %s, %s, %s)
""")

# from 1st submission
# user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s)
#                         on conflict (user_id) do nothing;
# """)

# We want to potentially update the paid level of our users if that changes
user_table_insert = ("""insert into users (user_id, first_name, last_name, gender, level) values (%s, %s, %s, %s, %s)
                        on conflict (user_id) do update set level = excluded.level;
""")

song_table_insert = ("""insert into songs (song_id, title, artist_id, year, duration) values (%s, %s, %s, %s, %s)
                        on conflict (song_id) do nothing;
""")

artist_table_insert = ("""insert into artists (artist_id, name, location, latitude, longitude) values
    (%s, %s, %s, %s, %s) on conflict (artist_id) do nothing;
""")


time_table_insert = ("""insert into time (start_time, hour, day, week, month, year, weekday) values
    (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
  select s.song_id, s.artist_id
  from songs s
  inner join artists a on s.artist_id = a.artist_id
  where s.title = %s
    and a.name = %s
    and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
