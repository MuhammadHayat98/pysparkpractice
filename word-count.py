from pyspark import SparkConf, SparkContext
import re

def normalizeWords(word):
    return re.compile(r'\W+',re.UNICODE).split(word.lower())
    
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf= conf)

input = sc.textFile("file:///SparkCourse/Book")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if(cleanWord):
        print(cleanWord,count)