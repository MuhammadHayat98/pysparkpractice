from pyspark import SparkConf, SparkContext
import re

def normalizeWords(word):
    return re.compile(r'\W+',re.UNICODE).split(word.lower())
    
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf= conf)

input = sc.textFile("file:///SparkCourse/Book")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda xy: (xy[1],xy[0])).sortByKey()
results = wordCountsSorted.collect()
for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if(word):
        print(word, count)