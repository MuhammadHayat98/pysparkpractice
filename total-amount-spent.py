from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID,amount)

conf = SparkConf().setMaster("local").setAppName("TotalAmount")
sc = SparkContext(conf= conf)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine).reduceByKey(lambda x, y: x+y).sortBy(lambda a: a[1])

results = rdd.collect()
for result in results:
    print(result) 

