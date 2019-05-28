import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates spark session with correct configuration."""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Reads in JSON song data and then writes songs and artists tables to parquet on S3.

    Args:
        spark: The current SparkSession.
        input_data: The S3 bucket to read in the data.
        output_data: The S3 bucket to write to.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/*/*/*/*.json")

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist_id
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """Reads in JSON log data and then writes users, time and songplays tables to parquet on S3.

    Args:
        spark: The current SparkSession.
        input_data: The S3 bucket to read in the data.
        output_data: The S3 bucket to write to.
    """
    # get filepath to log data file
    # For working in the workspace: log_data = os.path.join(input_data, "log-data/*.json")
    log_data = os.path.join(input_data, "log-data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # rename the columns in df
    df = (df.withColumnRenamed('userId', 'user_id')
            .withColumnRenamed('firstName', 'first_name')
            .withColumnRenamed('lastName', 'last_name')
            .withColumnRenamed('itemInSession', 'item_in_session')
            .withColumnRenamed('sessionId', 'session_id')
            .withColumnRenamed('userAgent', 'user_agent'))

    # extract columns for users table
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users', mode='overwrite')

    # create timestamp column from original timestamp column
    # default type is string for UDFs, so we need to switch that by specifying the correct type
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), T.TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select('start_time',
                           hour(col('start_time')).alias('hour'),
                           dayofmonth(col('start_time')).alias('day'),
                           weekofyear(col('start_time')).alias('week'),
                           month(col('start_time')).alias('month'),
                           year(col('start_time')).alias('year'),
                           date_format(col('start_time'), 'EEEE').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/year=*/artist_id=*/*.parquet')
    artist_df = spark.read.parquet(output_data + 'artists/*.parquet')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.length == song_df.duration)).join(artist_df, df.artist == artist_df.artist_name).join(time_table, ['start_time'])

    # create the songplay_id column
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # select the columns of interest
    songplays_table = songplays_table.select('songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'year', 'month')

    # write songplays table to parquet files partitioned by year and month (I think this is a copy paste error because year and month aren't listed as required cols)
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays', mode='overwrite')


def main():
    """Calls functions to create Spark session and process song and log data."""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-bucket-cpm/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
