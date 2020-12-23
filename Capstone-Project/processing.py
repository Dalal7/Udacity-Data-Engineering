import configparser
from datetime import datetime,timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import ( col, lit,coalesce, udf, lower, max as max_)
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType,DateType,IntegerType,StringType
import re

config = configparser.ConfigParser()
config.read('dl.cfg')

AWS_ACCESS_KEY_ID =config.get('AWS','AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    """
    Create a Spark session to use it in processing the data
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .getOrCreate()
    return spark

def get_origin_dict():
    """
    Create a dictionary that contain the origin city code and name
    """
    my_dict = {}
    with open("/home/workspace/data/I94_SAS_Labels_Descriptions.SAS" ) as f:
        lines = f.readlines()   
    for line in lines[10:276]:
        (key, val) = line.split("=")
        my_dict[int(key)] = re.sub(r"[\n\t\s']*", "", val)
    return my_dict

def get_destination_dict():
    """
    Create a dictionary that contain the destination city code and name
    """
    my_dict = {}
    with open("/home/workspace/data/I94_SAS_Labels_Descriptions.SAS") as f:
        lines = f.readlines()   
    for line in lines[303:893]:
        line= line.split(',', 1)[0]
        (key, val) = line.split("=")
        key =  re.sub(r"[\n\t\s']*", "", key)
        my_dict[str(key)] = re.sub(r"[\n\t']*", "", val)
    return my_dict


def process_immigration_data(spark, input_data, output_data):
    """
    Process the immigration data by filtering out the unknown cities, replacing city codes with city names to make it more readable, and converting the SAS dates of arrival and departure to a datetime
    
    Parameters:
    spark: the spark session
    input_data: the path of the input folder of the data in the local machine
    output_data: the output folder in S3 Bucket
    """
    path = input_data+"immigration_data"
    dirs = os.listdir(path)
    for file in dirs:
        immig_data = path + file
        immig_df = spark.read.format('com.github.saurfang.sas.spark').load(immig_data)

        # Select the needed columns
        immig_df = immig_df.select('cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate', 'i94bir', 'i94visa','biryear', 'gender', 'airline', 'fltno', 'visatype')

        # Delete unknown origin cities
        immig_df = immig_df.filter(immig_df.i94cit.isin(list(get_origin_dict().keys())))
        # Replace cities code to be more readable
        map_func = udf(lambda row : get_origin_dict().get(row,row))
        immig_df = immig_df.withColumn("i94cit", lower(map_func(col("i94cit"))))
         
        # Delete unknown destination cities 
        immig_df = immig_df.filter(immig_df.i94port!='XXX').filter(immig_df.i94port!='UNK')
        immig_df = immig_df.filter(immig_df.i94port.isin(list(get_destination_dict().keys())))
        
        # Replace cities code to be more readable
        map_func = udf(lambda row : get_destination_dict().get(row,row))
        immig_df = immig_df.withColumn("i94port", lower(map_func(col("i94port"))))

        # Convert arrival and departure dates
        convert_to_datetime = udf(lambda s: s if s is None 
                                  else (timedelta(s)+datetime(1960, 1, 1)),DateType())  

        immig_df = immig_df.withColumn("arrdate", convert_to_datetime(immig_df.arrdate))
        immig_df = immig_df.withColumn("depdate", convert_to_datetime(immig_df.depdate))
        
        immig_df.write.option("header","true").csv(input_data+'immigration_data/')
       
    
def process_temperature_data(spark, input_data, output_data):
    """
    Process the temperature data by filtering only the US cities, dropping the duplicates rows, and dropping the null values of the average temperature
    
    Parameters:
    spark: the spark session
    input_data: the path of the input folder of the data in the local machine
    output_data: the output folder in S3 Bucket
    """
    
    temp_data = input_data+'GlobalLandTemperaturesByCity.csv'
    temp_df = spark.read.format('csv').options(header='true').load(temp_data)

    temp_df = temp_df.select('dt', 'AverageTemperature', 'AverageTemperatureUncertainty', 'City', 'Country', 'Longitude', 'Latitude' ).drop_duplicates()
    temp_df = temp_df.filter(temp_df.Country=='United States')
    temp_df = temp_df.filter(temp_df.AverageTemperature.isNotNull())

    temp_df.write.option("header","true").csv(output_data+'temperature_data/')
    
    
def process_demographic_data(spark, input_data, output_data):
    """
    Process the demographic data by dropping the duplicates rows and create a new column 'Major Race' based on a group by function
    
    Parameters:
    spark: the spark session
    input_data: the path of the input folder of the data in the local machine
    output_data: the output folder in S3 Bucket
    """
    
    demo_data = input_data+'us-cities-demographics.csv'
    demo_df = spark.read.format('csv').options(header='true',sep=';').load(demo_data)

    demo_df = demo_df.select('City', 'State','Median Age','Male Population','Female Population','Total Population','Foreign-born','State Code','Race','Count').drop_duplicates(subset=['City', 'State','Race'])
    
    demo_df= demo_df.withColumn("Count",col("Count").cast(IntegerType()))
    
    # Using group by to know the major race of every city
    group_df= demo_df.groupby('City', 'State','Median Age','Male Population','Female Population','Total Population','Foreign-born','State Code').pivot('race').agg(max_('Count'))

    group_df = group_df.na.fill({'Hispanic or Latino':0, 'White':0, 'Asian':0, 'Black or African-American':0, 'American Indian and Alaska Native':0})
    cols = group_df.columns[8:13]
    maxcol = F.udf(lambda row: cols[row.index(max(row))], StringType())  
    group_df = group_df.withColumn("Major Race", maxcol(F.struct([group_df[x] for x in group_df.columns[8:13]])))
 
    group_df.write.option("header","true").csv(output_data+'demographic_data/')


def main():
    
    spark = create_spark_session()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    input_data = "/home/workspace/data/"
    output_data = "s3://udacity-dend-dalal/"
    
    process_immigration_data(spark, input_data, output_data)    
    process_temperature_data(spark, input_data, output_data) 
    process_temperature_data(spark, input_data, output_data) 
    
    spark.stop()

if __name__ == "__main__":
    main()
