from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bronze_layer").getOrCreate()
# Writing output in bronze location
(
    spark.read.option('header',True).format('csv').load('/home/gopi/Desktop/Main/Medallion/data/food_establishment_data.csv')
    .write.mode('overwrite').parquet('/home/gopi/Desktop/Main/Medallion/output/bronze_layer') 
)
