from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("friends-by-age")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
#maps rdd into key value pay with 1 as value and age as key
rdd = lines.map(parseLine)
# maps totalsByAge into key value that has the age with the total number of friends and its occurence 
# then reduces the values by aggregating the number of friends per age and that age's occurence 
# ie. (33, (387,2))
totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
# calculates the average number of friends per age
# takes the values from the key and divides them both
# ie. (33,(387,2)) --> 387/2 --> (33, 193.5)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
# takes the rdd and calls collect which runs all the processes in paralell and assigns it to a list results
results = averagesByAge.collect()
for result in results:
    print(result)
