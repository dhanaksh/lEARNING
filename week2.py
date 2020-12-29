from pyspark.sql import SparkSession
""" Sparksession is min entry point to any spark application """


from pyspark.sql.types import StructType,StringType,IntegerType,TimestampType,DateType,LongType
"""Data types for the schema of the dataframes """


from pyspark.sql.functions import lit,year,current_timestamp
"""Function for adding new columns to the dataframe"""

spark = SparkSession.builder.master('local[1]').appName("MyApp") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
"""To create a spark application and delta files are configured with spark"""


from delta.tables import *

"""import * refers to import all functions from delta table """

def field():
    schema = StructType()\
        .add("signal_code",StringType(),True)\
        .add("signal_value",LongType(),True)\
        .add("timestamp_utc",TimestampType(),True)\
        .add("turbine_id",IntegerType(),True)\
        .add("start_timestamp_utc",TimestampType(),True)\
        .add("source_filename",StringType(),True) \
        .add("end_timestamp_utc",TimestampType(),True) \
        .add("company_name",StringType(),True)\
        .add("signal_date",DateType(),True)
    return schema
"""Change in the schema of the dataframe """


path='/home/himanshu/Desktop/10min_data_sample.csv'
dataframe=spark.read.format('csv') \
    .option("header", True) \
    .schema(field())\
    .load(path)

"""Dataframe created by reading data from a provided sample csv by providing the path """

dataframe.show()
dataframe.printSchema()

UpdatedDF=dataframe.withColumn('Company_name',lit('XENON'))\
    .withColumn('source_filename',lit('events_data'))\
    .withColumn('singal_year',year(dataframe.timestamp_utc))\
    .withColumn('processing_date_and_time_utc',current_timestamp())

"""Transformation done in the dataframe and new dataframe is created by using reffernce of old dataframe and named as UpdatedDF"""


UpdatedDF.show()
UpdatedDF.printSchema()


newDF=spark.read.option('header',True).csv("/home/himanshu/Desktop/Duplicate.csv")
newDF.show()

"""A new dataframe is created which contains the duplicate data and used for the merge operation """

deltatable = DeltaTable.forPath(spark,"/home/himanshu/Desktop/a1")

"""A deltatable is created where the data is present in which merge and dedup operation is performed """

deltatable.alias("old_data") .merge(
    newDF.alias("new_data"),
    "old_data.signal_code  = new_data.signal_code").whenMatchedUpdateAll().whenNotMatchedInsertAll() .execute()

"""Merge and deduplication of data is done with delta table and newDF which carries the duplicate and new data"""

DisplayDF = spark.read.format("delta").load("/home/himanshu/Desktop/a1")
DisplayDF.show()

"""Updated dataframe present in the delta lake to check that o0nly new dat is stored in the deklta lake """


