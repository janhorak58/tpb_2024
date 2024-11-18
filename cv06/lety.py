from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, VectorIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.types import DoubleType
"""
* Příznak ve formě stringu je potřeba převést na číslo pomocí StringIndexer a OneHotEncoder

11 příznaků:
* Year
* Month
* DayofMonth
* DayOfWeek
* CRSDepTime
* CRSArrTime
* UniqueCarrier*
* CRSElapsedTime
* Origin*
* Dest*
* Distance

Label:
* ArrDelay > 0 -> 1*

"""

# Inicializace SparkSession
spark = SparkSession.builder \
    .appName("FlightDelayPrediction") \
    .config("spark.driver.memory", "12g") \
    .config("spark.executor.memory", "12g") \
    .getOrCreate()

# Načtení dat
y1988 = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/1988.csv")
y1989 = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/1989.csv")
y1990 = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/1990.csv")

# Spojení ročníků a základní předzpracování s použitím sample a repartition
years = y1988.union(y1989).union(y1990).select("Year", "Month", "DayofMonth", "DayOfWeek", 
                                              "CRSDepTime", "CRSArrTime", "UniqueCarrier", 
                                              "CRSElapsedTime", "Origin", "Dest", "Distance", "Cancelled", "ArrDelay")
years = years.repartition(100) \
    .withColumn("ArrDelay", F.col("ArrDelay").cast(DoubleType())) \
    .withColumn("delayed", F.when(F.col("ArrDelay") > 0, 1.0).otherwise(0.0)) \
    .withColumn("Distance", F.col("Distance").cast(DoubleType())) \
    .withColumn("CRSElapsedTime", F.col("CRSElapsedTime").cast(DoubleType())) \
    .withColumn("CRSDepTime", F.col("CRSDepTime").cast(DoubleType())) \
    .withColumn("CRSArrTime", F.col("CRSArrTime").cast(DoubleType())) \
    .withColumn("Year", F.col("Year").cast(DoubleType())) \
    .withColumn("Month", F.col("Month").cast(DoubleType())) \
    .withColumn("DayofMonth", F.col("DayofMonth").cast(DoubleType())) \
    .withColumn("DayOfWeek", F.col("DayOfWeek").cast(DoubleType())) \
    .filter("Cancelled = 0") \
    .na.drop() \
    .drop("Cancelled") \
        
    
years.show(5)
years.printSchema()
                  

# Předzpracování stringové proměnné
# *UniqueCarrier*
# *Origin*
# *Dest*

# Vytvoření indexerů a encoderů pro stringové sloupce
UniqueCarrierIndexer = StringIndexer(inputCol="UniqueCarrier", outputCol="UniqueCarrierIndex")
UniqueCarrierEncoder = OneHotEncoder(inputCol="UniqueCarrierIndex", outputCol="UniqueCarrierVec")

OriginIndexer = StringIndexer(inputCol="Origin", outputCol="OriginIndex")
OriginEncoder = OneHotEncoder(inputCol="OriginIndex", outputCol="OriginVec")

DestIndexer = StringIndexer(inputCol="Dest", outputCol="DestIndex")
DestEncoder = OneHotEncoder(inputCol="DestIndex", outputCol="DestVec")

# Sestavení vektoru příznaků
assembler = VectorAssembler(inputCols=
    ["Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime",  
     "CRSElapsedTime",  "Distance", "UniqueCarrierVec", "OriginVec", "DestVec"], 
    outputCol="features"
)


# Nastavení logistické regrese s menším počtem iterací
lr = LogisticRegression(featuresCol="features", labelCol="delayed", maxIter=5)

# Pipeline
pipeline = Pipeline(stages=[
    UniqueCarrierIndexer,
    OriginIndexer,
    DestIndexer,
    UniqueCarrierEncoder,
    OriginEncoder,
    DestEncoder,
    assembler, lr
])


# Rozdělení na trénovací a testovací sadu
train, test = years.randomSplit([0.9, 0.1], seed=42)

# Trénování modelu
print("Začínám trénování modelu...")
model = pipeline.fit(train)
print("Model natrénován")

# Predikce
predictions = model.transform(test)
print("Predikce hotova")

# Vyhodnocení
evaluator = MulticlassClassificationEvaluator(
    labelCol="delayed", 
    predictionCol="prediction", 
    metricName="accuracy"
)
accuracy = evaluator.evaluate(predictions)
print(f"Accuracy s logistickou regresí: {accuracy * 100:.2f}%")

# Pro srovnání - Decision Tree
dt = DecisionTreeClassifier(featuresCol="features", labelCol="delayed", maxDepth=5)
pipeline_dt = Pipeline(stages=[
    UniqueCarrierIndexer,
    OriginIndexer,
    DestIndexer,
    UniqueCarrierEncoder,
    OriginEncoder,
    DestEncoder,
    assembler, dt
])
print("Začínám trénování Decision Tree...")
model_dt = pipeline_dt.fit(train)
predictions_dt = model_dt.transform(test)
accuracy_dt = evaluator.evaluate(predictions_dt)
print(f"Accuracy s Decision Tree: {accuracy_dt * 100:.2f}%")

spark.stop()


"""
root@c6bef08c9364:/files# spark-submit lety.py 
24/11/15 11:54:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

Začínám trénování modelu...
24/11/15 11:54:54 WARN MemoryStore: Not enough space to cache rdd_148_0 in memory! (computed 17.0 MiB so far)
Model natrénován
Predikce hotova
Accuracy s logistickou regresí: 57.61%
Začínám trénování Decision Tree...
24/11/15 11:55:35 WARN MemoryStore: Not enough space to cache rdd_362_9 in memory! (computed 13.7 MiB so far)

Accuracy s Decision Tree: 56.19%
root@c6bef08c9364:/files# 




root@c6bef08c9364:/files# spark-submit lety.py 
24/11/15 12:02:34 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
+------+-----+----------+---------+----------+----------+-------------+--------------+------+----+--------+--------+-------+
|  Year|Month|DayofMonth|DayOfWeek|CRSDepTime|CRSArrTime|UniqueCarrier|CRSElapsedTime|Origin|Dest|Distance|ArrDelay|delayed|
+------+-----+----------+---------+----------+----------+-------------+--------------+------+----+--------+--------+-------+
|1988.0|  1.0|      22.0|      5.0|    1910.0|    1940.0|           WN|          90.0|   DAL| ELP|   562.0|     8.0|    1.0|
|1988.0|  1.0|       3.0|      7.0|    1205.0|    1310.0|           US|          65.0|   PIT| LGA|   335.0|     6.0|    1.0|
|1988.0|  1.0|      29.0|      5.0|    1345.0|    1559.0|           DL|         134.0|   ORD| DFW|   802.0|    -6.0|    0.0|
|1988.0|  1.0|      20.0|      3.0|    1133.0|    1210.0|           DL|          97.0|   SLC| LAX|   590.0|     8.0|    1.0|
|1988.0|  1.0|      20.0|      3.0|     951.0|    1525.0|           AA|         214.0|   LAS| ORD|  1515.0|    13.0|    1.0|
+------+-----+----------+---------+----------+----------+-------------+--------------+------+----+--------+--------+-------+
only showing top 5 rows

root
 |-- Year: double (nullable = true)
 |-- Month: double (nullable = true)
 |-- DayofMonth: double (nullable = true)
 |-- DayOfWeek: double (nullable = true)
 |-- CRSDepTime: double (nullable = true)
 |-- CRSArrTime: double (nullable = true)
 |-- UniqueCarrier: string (nullable = true)
 |-- CRSElapsedTime: double (nullable = true)
 |-- Origin: string (nullable = true)
 |-- Dest: string (nullable = true)
 |-- Distance: double (nullable = true)
 |-- ArrDelay: double (nullable = true)
 |-- delayed: double (nullable = false)

Začínám trénování modelu...
24/11/15 12:03:45 WARN MemoryStore: Not enough space to cache rdd_142_6 in memory! (computed 17.0 MiB so far)
24/11/15 12:03:45 WARN MemoryStore: Not enough space to cache rdd_142_10 in memory! (computed 17.0 MiB so far)
24/11/15 12:04:05 WARN MemoryStore: Not enough space to cache rdd_142_89 in memory! (computed 17.0 MiB so far)
24/11/15 12:04:06 WARN MemoryStore: Not enough space to cache rdd_142_98 in memory! (computed 17.0 MiB so far)
Model natrénován
Predikce hotova
Accuracy s logistickou regresí: 57.44%
Začínám trénování Decision Tree...
24/11/15 12:05:39 WARN MemoryStore: Not enough space to cache rdd_347_11 in memory! (computed 13.7 MiB so far)
24/11/15 12:05:39 WARN BlockManager: Persisting block rdd_347_11 to disk instead.
24/11/15 12:06:42 WARN MemoryStore: Not enough space to cache rdd_347_95 in memory! (computed 167.8 MiB so far)
24/11/15 12:06:42 WARN MemoryStore: Not enough space to cache rdd_347_96 in memory! (computed 73.1 MiB so far)
24/11/15 12:06:42 WARN MemoryStore: Not enough space to cache rdd_347_97 in memory! (computed 47.2 MiB so far)
24/11/15 12:06:42 WARN MemoryStore: Not enough space to cache rdd_347_98 in memory! (computed 73.1 MiB so far)
24/11/15 12:06:42 WARN MemoryStore: Not enough space to cache rdd_347_99 in memory! (computed 9.0 MiB so far)
Accuracy s Decision Tree: 55.99%

"""