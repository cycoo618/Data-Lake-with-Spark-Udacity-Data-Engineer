import configparser
from datetime import datetime
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import from_unixtime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    create spark session by reading the configuration file
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    Read song data from S3 bucket, then process it, then upload it back to S3.
    
    Will create tables: songs and artists table
    
    Parameters:
    1. spark: existed spark connection
    2. input_data: song data path on S3
    3. output_data: upload path to S3
    
    """
    # get filepath to song data file
    song_data = input_data + "song-data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("songs")
    
    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        FROM songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_data= os.path.join(output_data, "songs")
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_data, mode="overwrite")

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id,
        artist_name as name,
        artist_location as location, 
        artist_latitude as lattitude,
        artist_longitude as longitude
        FROM songs
    """)
    
    # write artists table to parquet files
    
    artists_data=os.path.join(output_data, "artists")
    artists_table.write.mode("overwrite").parquet(artists_data)


def process_log_data(spark, input_data, output_data):
    """
    
    Read log data from S3 bucket, then process it, then upload it back to S3.
    
    Will create tables: users, time, songplays
    
    Parameters:
    1. spark: existed spark connection
    2. input_data: events data's path on S3
    3. output_data: upload path to S3
    
    """
        
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    df.createOrReplaceTempView("logs")
    users_table = spark.sql("""
        SELECT DISTINCT 
            userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM logs
    """)
    
    # write users table to parquet files
    users_data=os.path.join(output_data, "users")
    users_table.write.mode("overwrite").parquet(users_data)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(x/1000),IntegerType())
    df = df.withColumn('start_time',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: from_unixtime(x),TimestampType())
    df = df.withColumn('datetime',from_unixtime('start_time'))
    
    # extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                       hour('datetime').alias('hour'),
                       dayofmonth('datetime').alias('day'),
                       weekofyear('datetime').alias('week'),
                       month('datetime').alias('month'),
                       year('datetime').alias('year'),
                       date_format('datetime','E').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_data=os.path.join(output_data, "time")
    time_table.write.partitionBy('year', 'month').parquet(time_data,mode="overwrite")

    # read in song data to use for songplays table
    df.createOrReplaceTempView("logs")
    time_table.createOrReplaceTempView("time")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT DISTINCT
            l.datetime as start_time,
            l.userId as user_id,
            l.level,
            s.song_id,
            s.artist_id,
            l.sessionId as session_id,
            l.location,
            l.userAgent as user_agent,
            t.year,
            t.month
        FROM logs l
        JOIN songs s on l.artist = s.artist_name and l.song = s.title and l.length = s.duration
        JOIN time t on t.start_time = l.datetime
    """)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_data=os.path.join(output_data, "songplays")
    songplays_table.write.partitionBy('year', 'month').parquet(songplays_data,mode="overwrite")


def main():
    """
    
    ETL Pipeline which reads song and events data from S3,
    and process the data,
    then finally upload the processed data back to S3.
    
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-my-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
