from pyspark import SparkConf, SparkContext
import re
import collections

conf = SparkConf().setMaster("spark://20a86424e1e8:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/book.txt")


def preprocess(text):
    text = text.lower()
    text = re.sub(r'[^\w\s]', '', text)
    words = text.split()
    return words

words = input.flatMap(lambda x: preprocess(x))
wordCounts = words.countByValue()
sortedResults = collections.OrderedDict(sorted(wordCounts.items(), key=lambda x: x[1], reverse=True))

for word, count in list(sortedResults.items())[:20]:
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
