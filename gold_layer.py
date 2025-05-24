
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("gold_layer").getOrCreate()

aggr_df = spark.read.format('parquet').load('/home/gopi/Desktop/Main/Medallion/output/silver_layer')

aggr_df = aggr_df.where('violation_type="RED"').groupBy('name').agg(count('*').alias('count'))
# Writing output in gold location
aggr_df.repartition(1).write.option('header',True).mode('overwrite').csv('/home/gopi/Desktop/Main/Medallion/output/gold_layer') 


