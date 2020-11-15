from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("total-amount-spent").getOrCreate()

schema = StructType([
    StructField("customerID", IntegerType(),True),
    StructField("itemID", IntegerType(), True), 
    StructField("amountSpent", FloatType(), True)
])

df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")
df.printSchema()

customers = df.select("customerID", "amountSpent")
amountSpent = customers.groupBy("customerID").sum("amountSpent")
amountSpentRounded = amountSpent.withColumn("amountSpent", func.round(func.col("sum(amountSpent)"),2)).select("customerID", "amountSpent").sort("amountSpent")

results = amountSpentRounded.collect()

for result in results:
    print(result[0], result[1])

spark.stop()
