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
connections.show()

conn2 = connections.withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) 
conn2.show()

conn3 = conn2.groupBy("id").agg(func.sum("connections").alias("connections"))
conn3.show()
mostPopular = conn3.sort(func.col("connections").desc()).first()

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

