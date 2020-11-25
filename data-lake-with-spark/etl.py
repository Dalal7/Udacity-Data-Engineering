import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark session to use it in processing the data
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the song data to create the songs_table and artists_table 
    
    Parameters:
    spark: the spark session
    input_data: the input file in S3 Bucket
    output_data: the output file in S3 Bucket
    """
    
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # create a temporary view to run SQL queries
    df.createOrReplaceTempView("song_table")
    

    # extract columns to create songs table
    songs_table = spark.sql("""SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM song_table 
                            WHERE song_id IS NOT NULL
                            """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                FROM song_table
                                WHERE artist_id IS NOT NULL
                                """)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Process the log data to create the users_table, time_table, and songplays_table
    
    Parameters:
    spark: the spark session
    input_data: the input file in S3 Bucket
    output_data: the output file in S3 Bucket
    """
        
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # create a temporary view to run SQL queries
    df.createOrReplaceTempView("log_table")
    
    # extract columns for users table    
    users_table = spark.sql(""" SELECT DISTINCT userId, firstName, lastName, gender, level
                             FROM log_table
                             WHERE userId IS NOT NULL""")
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df['ts']))
    df.createOrReplaceTempView("time_table")
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT DISTINCT timestamp as start_time,
                            hour(timestamp) as hour,
                            day(timestamp) as day,
                            weekofyear(timestamp) as week,
                            month(timestamp) as month,
                            year(timestamp) as year,
                            dayofweek(timestamp) as weekday
                            FROM time_table
                            WHERE timestamp IS NOT NULL""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table/", mode="overwrite", partitionBy=["year","month"])

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
   
    # read song data file
    df = spark.read.json(song_data)
    
    # create a temporary view to run SQL queries
    df.createOrReplaceTempView("song_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table = spark.sql("""SELECT monotonically_increasing_id() as songplay_id,
                                                    to_timestamp(l.ts/1000) as start_time,
                                                    l.userId as user_id,
                                                    l.level as level,
                                                    s.song_id as song_id,
                                                    s.artist_id as artist_id,
                                                    l.sessionId as session_id,
                                                    l.location as location,
                                                    l.userAgent as user_agent,
                                                    month(to_timestamp(l.ts/1000)) as month,
                                                    year(to_timestamp(l.ts/1000)) as year,
                                                    FROM log_table l
                                                    JOIN song_table s on l.song = s.title and l.artist = s.artist_name
                                                    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
