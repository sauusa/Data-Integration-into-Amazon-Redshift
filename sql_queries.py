import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
ARN=config.get('IAM_ROLE','ARN')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
                CREATE TABLE IF NOT EXISTS stage_events 
                (
                   artist          VARCHAR(500),
                   auth            VARCHAR(20),
                   firstName       VARCHAR(500),
                   gender          CHAR(1),
                   itemlnSession   INTEGER,
                   lastName        VARCHAR(500),
                   length          DECIMAL(10,4),
                   level           VARCHAR(10),
                   location        VARCHAR(500),
                   method          VARCHAR(20),
                   page            VARCHAR(500),
                   registration    BIGINT,
                   sessionId       INTEGER,
                   song            VARCHAR(500),
                   status          INTEGER,
                   ts              TIMESTAMP,
                   userAgent       VARCHAR(500),
                   userId          INTEGER
                )
""")

staging_songs_table_create = ("""
                CREATE TABLE IF NOT EXISTS stage_songs 
                (
                   num_songs        INTEGER,
                   artist_id        VARCHAR(50)  NOT NULL,
                   artist_latitude  DECIMAL(12, 5),
                   artist_longitude DECIMAL(12, 5),
                   artist_location  VARCHAR(500),
                   artist_name      VARCHAR(500),
                   song_id          VARCHAR(20),
                   title            VARCHAR(500),
                   duration         DECIMAL(12, 5),
                   year             INTEGER
                )
""")

songplay_table_create = ("""
                CREATE TABLE IF NOT EXISTS songplays
                (
                    songplay_id     BIGINT   IDENTITY(0,1)   PRIMARY KEY,
                    start_time      TIMESTAMP    NOT NULL,                    
                    user_id         INTEGER      NOT NULL REFERENCES users (user_id),
                    level           VARCHAR(10),
                    song_id         VARCHAR(20)  NOT NULL REFERENCES songs (song_id),
                    artist_id       VARCHAR(50)  NOT NULL REFERENCES artists (artist_id),
                    session_id      INTEGER,
                    location        VARCHAR(500),
                    user_agent      VARCHAR(500)
                )
""")

user_table_create = ("""
                CREATE TABLE IF NOT EXISTS users
                (
                    user_id         INTEGER PRIMARY KEY,
                    first_name      VARCHAR(500),
                    last_name       VARCHAR(500), 
                    gender          CHAR(1), 
                    level           VARCHAR(10)
                )
""")

song_table_create = ("""
                CREATE TABLE IF NOT EXISTS songs
                (
                    song_id         VARCHAR(20)  PRIMARY KEY, 
                    title           VARCHAR(500), 
                    artist_id       VARCHAR(50)  NOT NULL,
                    year            INTEGER, 
                    duration        DECIMAL(12, 5)
                )

""")

artist_table_create = ("""
                CREATE TABLE IF NOT EXISTS artists
                (
                    artist_id       VARCHAR(50)  PRIMARY KEY, 
                    name            VARCHAR(500), 
                    location        VARCHAR(500),
                    latitude        DECIMAL(12, 5), 
                    longitude       DECIMAL(12, 5)
                )
""")

time_table_create = ("""
                CREATE TABLE IF NOT EXISTS time
                (
                    start_time      TIMESTAMP    PRIMARY KEY, 
                    hour            INTEGER, 
                    day             INTEGER, 
                    week            INTEGER,
                    month           INTEGER, 
                    year            INTEGER, 
                    weekday         INTEGER
                )
""")

# STAGING TABLES

staging_songs_copy = ("""
    copy stage_songs from {}
    credentials 'aws_iam_role={}'
    compupdate off statupdate off
    region 'us-west-2' format as JSON 'auto';
""").format(SONG_DATA,ARN)

staging_events_copy = ("""
    copy stage_events from {}
    credentials 'aws_iam_role={}'
    compupdate off statupdate off
    region 'us-west-2' format as JSON{} TIMEFORMAT as 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id,
                          session_id, location, user_agent)
                SELECT DISTINCT
                      e.ts         as start_time,
                      e.userId     as user_id,
                      e.level      as level,
                      s.song_id    as song_id,
                      s.artist_id  as artist_id,
                      e.sessionid  as session_id,
                      e.location   as location,
                      e.userAgent  as user_agent
                FROM  public.stage_events e, public.stage_songs s
                WHERE e.artist = s.artist_name
                  and e.song   = s.title
                  and e.page   = 'NextSong'
                  and e.userId NOT IN (
                            SELECT DISTINCT s2.user_id FROM songplays s2 
                            WHERE s2.user_id    = e.userId AND  
                                  s2.session_id = e.sessionId);
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
                SELECT DISTINCT
                      e.userId     as user_id,
                      e.firstName  as first_name,
                      e.lastName   as last_name,
                      e.gender     as gender,
                      e.level      as level
                FROM  public.stage_events e
                WHERE e.page   = 'NextSong'
                  and e.userId NOT IN (
                             SELECT DISTINCT user_id FROM users);
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
                SELECT DISTINCT
                      s.song_id    as song_id,
                      s.title      as title,
                      s.artist_id  as artist_id,
                      s.year       as year,
                      s.duration   as duration
                FROM  public.stage_songs s
                WHERE s.song_id NOT IN (SELECT DISTINCT song_id FROM songs);    
""")
                
artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
                SELECT DISTINCT
                      s.artist_id        as artist_id,
                      s.artist_name      as name,
                      s.artist_location  as location,                      
                      s.artist_latitude  as latitude,
                      s.artist_longitude as longitude
                FROM  public.stage_songs s
                WHERE s.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);        
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
                SELECT DISTINCT
                      start_time,
                      EXTRACT(hour    FROM start_time)    as hour,
                      EXTRACT(day     FROM start_time)    as day,
                      EXTRACT(week    FROM start_time)    as week,
                      EXTRACT(month   FROM start_time)    as month,
                      EXTRACT(year    FROM start_time)    as year,
                      EXTRACT(weekday FROM start_time)    as weekday
                FROM  public.songplays
                WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, artist_table_create, song_table_create, user_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, artist_table_drop, song_table_drop, user_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [artist_table_insert, song_table_insert, user_table_insert, songplay_table_insert, time_table_insert]
table_list = ["stage_events", "stage_songs", "artist", "songs", "users", "songplays", "time"]