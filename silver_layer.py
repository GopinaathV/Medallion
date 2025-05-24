from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver_layer").getOrCreate()

df = spark.read.format('parquet').load('/home/gopi/Desktop/Main/Medallion/output/bronze_layer/')
df = df.dropna(how='any') 
# To identify the source file
df = df.withColumn('input_bronze_file',input_file_path())
# Writing output in silver location
df.write.mode('overwrite').parquet('/home/gopi/Desktop/Main/Medallion/output/silver_layer') 



