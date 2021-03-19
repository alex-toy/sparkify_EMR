import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType, DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    #spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    # song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'
    df = spark.read.json(song_data)

    # songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.write.partitionBy("year", "artist_id").format("parquet").save("songs_table.parquet")

    # artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table.write.format("parquet").save("artists_table.parquet")


def format_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0) 
    
    
def process_log_data(spark, input_data, output_data):
    # log data file
    #log_data = input_data + 'log-data'
    log_data = input_data + 'log-data/2018/11/2018-11-12-events.json'
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # users table to parquet files
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    users_table.write.format("parquet").save("users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: format_datetime(int(x)), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: format_datetime(int(x)), DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))

    
    # extract columns to create time table
    #start_time, hour, day, week, month, year, weekday
    time_table = df.select(
        'ts',
        'datetime',
        'timestamp',
        year(df.datetime).alias('year'),
        month(df.datetime).alias('month')
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").save("time_table.parquet")

    # read in song data to use for songplays table
    #song_id, title, artist_id, year, duration
    song_df = df.select('song_id', 'song', 'artist_id', 'year', 'duration')
    #itemInSession, lastName, auth, registration, ts, artist, level, status, datetime, userId, page, song, firstName, userAgent, gender, location, method, length, sessionId, timestamp

    # extract columns from joined song and log datasets to create songplays table 
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    #songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    #songplays_table
    pass


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    
def clean_files():
    os.system('rm -rf /home/workspace/songs_table.parquet')
    os.system('rm -rf /home/workspace/artists_table.parquet')
    os.system('rm -rf /home/workspace/users_table.parquet')
    os.system('rm -rf /home/workspace/time_table.parquet')   


if __name__ == "__main__":
    
    clean_files()
    
    main()
    
