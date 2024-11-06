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

for count, word in sortedWordCounts.take(20):
    print(f"{word} {count}")
