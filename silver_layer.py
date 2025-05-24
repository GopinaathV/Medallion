from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver_layer").getOrCreate()

df = spark.read.format('parquet').load('/home/gopi/Desktop/Main/Medallion/output/bronze_layer/')
df = df.dropna(how='any')
# Writing output in silver location
df.write.mode('overwrite').parquet('/home/gopi/Desktop/Main/Medallion/output/silver_layer') 



