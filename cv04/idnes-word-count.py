from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import re
import collections

# Nastavení Spark kontextu
conf = SparkConf().setMaster("spark://20a86424e1e8:7077").setAppName("WordCount")
sc = SparkContext(conf=conf)

def normalize_json_text(text):
    text = text.lower()
    text = re.sub(r'[{}[\]:,",]+', '', text)
    return text

input = sc.textFile("/files/articles.json")

words = input.flatMap(lambda line: line.split()) \
             .map(normalize_json_text) \
             .filter(lambda word: word != "") \
             .filter(lambda word: len(word) >= 6) \
            .filter(lambda word: word not in ["content", "title", "category", "comments", "image_count", "date_published"])

wordCounts = words.map(lambda word: (word, 1)) \
                  .reduceByKey(lambda x, y: x + y)
sortedWordCounts = wordCounts.map(lambda x: (x[1], x[0])) \
                             .sortByKey(ascending=False)
top20Words = sortedWordCounts.take(20)
for count, word in top20Words:
    print(f"{word} {count}")

"""
zahraničí 39989
domácí 34806
jejich 34261
například 32304
mluvčí 32145
protože 28499
policie 28004
uvedla 26407
procent 20652
několik 19984
dalších 19500
prezident 19039
ministr 18522
kterou 17827
zhruba 16988
strany 16699
případě 16513
jednání 15763
ministerstvo 15755
všechny 15122

"""