import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, dayofweek, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
import traceback
import sys
import logging
logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
def create_local_spark_session():
    """This function creates local spark session.

        Arguments:
         None
        Returns:
         spark session with an appName "Test"
        """

    return SparkSession.builder.master("local[*]").appName("Test").getOrCreate()

def create_emr_spark_session():
    """This function creates EMR spark session.

        Arguments:
         None
        Returns:
         spark session with an appName "App"
        """

    return SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .appName("App") \
            .getOrCreate()
def process_song_data(spark, input_data, output_data):
    """This function reads song data file, extracts columns to create *songs* and *artists* tables,
        and write them to parquet files.

        Arguments:
             spark: EMR spark session,
             input_data,
             output_data
        Returns:
             None
            """

    schema = (
        spark.read.format("json").load(input_data + "/song_data/A/A/A/*.json").schema
    )
    print(schema)
    # read song data file
    df = (
        spark.read.format("json")
        .schema(schema)
        .load(input_data + "/song_data/*/*/*/*.json")
    )

    # extract columns to create songs table
    songs_table_df = (
        df.select("song_id", "title", "artist_id", "year", "duration")
        .dropDuplicates()
        .withColumn("song_id", monotonically_increasing_id())
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table_df.write.format("parquet").mode("overwrite").partitionBy(
        "year", "artist_id"
    ).save(output_data + "/songs.parquet")
    logger.warning("Stored Songs")

    # extract columns to create artists table
    artists_table_df = df.selectExpr(
        "artist_id",
        "artist_name as name",
        "artist_location as location",
        "artist_latitude as latitude",
        "artist_longitude as longitude",
    ).dropDuplicates()

    # write artists table to parquet files
    artists_table_df.write.format("parquet").mode("overwrite").save(
        output_data + "/artists.parquet"
    )
    logger.warning("Stored Artists")


def process_log_data(spark, input_data, output_data):
    """This function reads log data file, filter by actions for song plays,
        extracts columns for *users* and *time* tables,
        and write them to parquet files;
        reads in song data to use for *songplays* table,
        extract columns from joined song and log datasets to create *songplays* table,
        and writes *songplays* table to parquet files partitioned by year and month.

        Arguments:
            spark: EMR spark session,
            input_data,
            output_data
        Returns:
            None
    """
    # read log data file
    df = spark.read.format("json").load(input_data + "/log_data/*/*/*.json")

    # filter by actions for song plays
    df = df.where(df.page == lit("NextSong"))

    # extract columns for users table
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    users_table.write.format("parquet").mode("overwrite").save(
        output_data + "/users.parquet"
    )
    logger.warning("Stored Users")


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    df = (
        df.withColumn("hour", hour("start_time"))
        .withColumn("day", dayofmonth("start_time"))
        .withColumn("week", weekofyear("start_time"))
        .withColumn("month", month("start_time"))
        .withColumn("year", year("start_time"))
        .withColumn("weekday", dayofweek("start_time"))
    )

    # extract columns to create time table
    time_table_df = df.select(
        "start_time", "hour", "day", "week", "month", "year", "weekday"
    )

    # write time table to parquet files partitioned by year and month
    time_table_df.write.mode("overwrite").partitionBy("year", "month").parquet(
        output_data + "/time.parquet"
    )

    # read in song data to use for songplays table
    songs_df = (
        spark.read.format("parquet")
        .load(output_data + "/songs.parquet")
        .select("song_id", "title", "artist_id")
    )
    songs_logs_df = df.join(songs_df, (df.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table
    songplays_table_df = songs_logs_df.select(
        col("start_time"),
        col("userId").alias("user_id"),
        col("level"),
        col("song_id"),
        col("artist_id"),
        col("sessionId").alias("session_id"),
        col("location"),
        col("userAgent").alias("user_agent"),
        col("year"),
        col("month"),
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table_df.write.mode("overwrite").partitionBy("year", "month").parquet(
        output_data + "/songplays.parquet"
    )


def main(input_prefix: str = None, output_prefix: str = None):

    spark = create_emr_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    try:
        process_song_data(spark, input_prefix, output_prefix)
        print("process_song_data done.")
        logger.warning("process_song_data done.")
        process_log_data(spark, input_prefix, output_prefix)
        print("process_log_data done.")
        logger.warning("process_log_data done.")
    except:
        print(traceback.format_exc())
        logger.warning(str(sys.exc_info()), exc_info=sys.exc_info())


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Input path")
    parser.add_argument("--output", help="Output path")

    args = parser.parse_args()
    main(
        input_prefix=args.input,
        output_prefix=args.output,
    )
