from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('TalendJob_Migrated').getOrCreate()

df1 = spark.read.option('header','true').csv('/data/input.csv')
df2 = df1.filter('age > 18')
df2.write.mode('overwrite').parquet('/data/output')

spark.stop()
