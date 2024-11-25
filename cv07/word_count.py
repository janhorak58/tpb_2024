from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

# Inicializace SparkSession
spark = SparkSession.builder \
    .appName("CountWords").getOrCreate()

lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
    
words = lines.select(F.explode(F.split(F.lower(lines.value), " ")).alias("word"))

wordsMalaPismenaBezInterpunkce = words. \
    withColumn("word", F.lower(F.regexp_replace("word", "[^a-z]", "")))
wordsWithTimestamp = wordsMalaPismenaBezInterpunkce.withColumn("eventTime", F.current_timestamp()) \
    .withColumn("eventTime", F.date_format("eventTime", "yyyy-MM-dd HH:mm:ss"))
wordCounts = wordsWithTimestamp.groupBy(
    F.window(F.col("eventTime"), "30 seconds", "15 seconds"),
    F.col("word")
    ).count()



sortedWordCounts = wordCounts.orderBy("count", ascending=False) \
    .withColumn("timestamp", F.date_format(F.col("window.start"), "HH:mm:ss"))

query = sortedWordCounts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .queryName("word_counts") \
    .start()
    
query.awaitTermination()

while True:
    spark.sql("SELECT * FROM word_counts").show()
    time.sleep(5)
    
    