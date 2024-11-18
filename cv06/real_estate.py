from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor

spark = SparkSession.builder.master("spark://c6bef08c9364:7077").appName("SparkSQL").getOrCreate()

realestate = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/realestate.csv")

realestate.show(5)

# Příznaky: Datum, Stáří, vzdálenost k veř. dopravě, počet obchodů, souřadnice, cena
assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores", "Latitude", "Longitude"], outputCol="features")
df = assembler.transform(realestate).select("PriceOfUnitArea", "features")
train_data, test_data = df.randomSplit([0.9, 0.1])

dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")

model = dt.fit(train_data)
predictions = model.transform(test_data)

predictions.select("PriceOfUnitArea", "prediction").show(10)


spark.stop()



