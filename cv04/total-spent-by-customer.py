from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("spark://172.18.0.4:7077").setAppName("TotalSpentByCustomer")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    userId = fields[0]
    productId = fields[2]
    price = fields[3]
    return (userId, productId, price)

lines = sc.textFile("/files/1800.csv")

parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect();

