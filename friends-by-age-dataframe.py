from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/fakefriends-header.csv")

totalsByAge = people.select("age", "friends").groupBy("age").avg("friends").sort("age")
totalsByAge.show()
spark.stop()