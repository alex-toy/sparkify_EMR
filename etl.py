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
    song_data = input_data + 'song_data/*/*/*/*.json'
    #song_data = input_data + 'song_data/A/B/C/TRABCEI128F424C983.json'
    df = spark.read.json(song_data)

    # songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").format("parquet").save( output_data + "/songs.parquet")
    
    # artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')
    artists_table.write.mode('overwrite').format("parquet").save(output_data + "/artists.parquet")


def format_datetime(ts):
    return datetime.fromtimestamp(ts/1000.0) 
    
    
def process_log_data(spark, input_data, output_data):
    log_data = input_data + 'log-data'
    #log_data = input_data + 'log-data/2018/11/2018-11-12-events.json'
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # users table to parquet files
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level')
    users_table.write.mode('overwrite').format("parquet").save(output_data + "/users.parquet")

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
    time_table.write.mode('overwrite').partitionBy("year", "month").format("parquet").save(output_data + "/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.format("parquet").option(
        "basePath", os.path.join(output_data, "songs.parquet/")
    ).load(os.path.join(output_data, "songs.parquet/"))

    # extract columns from joined song and log datasets to create songplays table 
    #songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = df.join(
        song_df, 
        (df.song == song_df.title) &
        (df.length == song_df.duration), 'left_outer'
    ).select(
        df.timestamp, 
        col("userId").alias('user_id'), 
        df.level, 
        song_df.song_id, 
        song_df.artist_id, 
        col("sessionId").alias("session_id"), 
        df.location, 
        col("useragent").alias("user_agent"),
        year('datetime').alias('year'),
        month('datetime').alias('month')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").format("parquet").save(output_data + "/songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output_data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    
def clean_files():
    os.system('rm -rf /home/workspace/output_data/songs.parquet')
    os.system('rm -rf /home/workspace/output_data/artists.parquet')
    os.system('rm -rf /home/workspace/output_data/users.parquet')
    os.system('rm -rf /home/workspace/output_data/time.parquet')
    os.system('rm -rf /home/workspace/output_data/songplays.parquet') 


if __name__ == "__main__":
    
    #clean_files()
    
    main()
    
