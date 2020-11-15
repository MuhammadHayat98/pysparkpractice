from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of book into dataframe
inputDF = spark.read.text("file:///SparkCourse/book.txt")

# Split using a regex that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowerCaseWords = words.select(func.lower(words.word).alias("word"))

# count occurence of each word
wordCounts = lowerCaseWords.groupBy("word").count()

# sort by counts
wordCountsSorted = wordCounts.sort("count")

# display results
wordCountsSorted.show(wordCountsSorted.count())