from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://98c0ba3813b6:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

people.select("age", "friends"). \
    groupBy("age"). \
    mean(). \
    select("age", "avg(friends)"). \
    sort("age"). \
    show()
    
    
spark.stop()

