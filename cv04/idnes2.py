from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import re
import collections

conf = SparkConf().setMaster("spark://20a86424e1e8:7077").setAppName("WordCount")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

articles_df = spark.read.json("/files/articles-1-line.json")
articles_df.printSchema()

def preprocess(text):
    if text is None:
        return []  # Ošetření null hodnot
    text = text.lower()  # Převod na malá písmena
    text = re.sub(r'[^\w\s]', '', text)  # Odstranění interpunkce
    words = text.split()
    words = [word for word in words if len(word) >= 6]  # Pouze slova s alespoň 6 znaky
    return words

# Aplikace předzpracování a počítání slov
words = articles_df.rdd.flatMap(lambda row: preprocess(row.content))
wordCounts = words.countByValue()

# Seřazení výsledků podle četnosti a výpis top 20
sortedResults = collections.OrderedDict(sorted(wordCounts.items(), key=lambda x: x[1], reverse=True))
for word, count in list(sortedResults.items())[:20]:
    cleanWord = word.encode('ascii', 'ignore')  # Vyčistění textu pro ASCII kompatibilitu
    if cleanWord:
        print(cleanWord.decode() + " " + str(count))