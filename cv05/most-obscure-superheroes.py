from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("leastPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/marvel-names.txt")

lines = spark.read.text("/files/marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Odfiltruju superhrdiny s 0 propojeními
connections = connections.filter(func.col("connections") > 0)

# Spočítam minimální počet propojení
print(connections.agg(func.min("connections")).first())
min_connections = connections.agg(func.min("connections")).first()[0]

# Vyfiltruji pouze hrdiny s minimálním počtem propojení
leastPopular = connections.filter(func.col("connections") == min_connections)

# Spojím tabulky se jmény a počtem propojení a seřadim
leastPopularNames = names.join(leastPopular, "id") \
    .sort(func.col("connections").asc(), func.col("name").asc())

leastPopularNames.select("name", "connections").show(20)
spark.stop()
