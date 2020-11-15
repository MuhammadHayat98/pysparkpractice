from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("DemAndRepub_Victories").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/vote_counts_bystate.csv")
df.printSchema()

# dem wins = 1, rep wins = 0
totalByState = df.select('state', 'dem_votes', 'rep_votes').withColumn("winning_party", func.when(func.col('dem_votes') > func.col('rep_votes'), 1).otherwise(0))
totalByState.show()


totalWins = totalByState.groupBy("winning_party").count()
res = totalWins.collect()

for result in res:
    print(result)

spark.stop()