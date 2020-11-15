from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names")

lines = spark.read.text("file:///SparkCourse/Marvel-graph")

connections = lines.withColumn("id", func.split(func.col("value"), " ")[0])

conn2 = connections.withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) 

conn3 = conn2.groupBy("id").agg(func.sum("connections").alias("connections"))

conn3.show()

minNumberOfCon = conn3.agg(func.min("connections")).first()[0]
least_connected = conn3.filter(func.col("connections") == minNumberOfCon).join(names, 'id')




least_connected.show()

# print(minNumberOfCon)

