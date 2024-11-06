from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("spark://20a86424e1e8:7077").setAppName("TotalSpentByCustomer")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    userId = fields[0]
    productId = fields[1]
    price = fields[2]
    return (userId, productId, price)

lines = sc.textFile("/files/customer-orders.csv")

parsedLines = lines.map(parseLine) \
    .map(lambda x: (x[0], float(x[2]))) \
        .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[1], x[0])) \
                .sortByKey(ascending=False) # Bonus
                
results = parsedLines.take(20)

for result in results:
    print(f"Zákazník: {result[1]} => Útrata: {result[0]}")