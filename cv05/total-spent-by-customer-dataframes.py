from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
spark = SparkSession.builder.master("spark://98c0ba3813b6:7077").appName("SparkSQL").getOrCreate()

schema = StructType([ \
                        StructField("customer_id", IntegerType(), True), \
                        StructField("product_id", IntegerType(), True), \
                        StructField("price", DoubleType(), True)
                        ])
orders = spark.read.schema(schema).option("header", "false").option("inferSchema", "true").csv("/files/customer-orders.csv")
    
print("Here is our inferred schema:")
orders.printSchema()

from pyspark.sql import functions as func

def round_to_2_places(value):
    print(value)
    return round(value, 2)

round_udf = func.udf(round_to_2_places, DoubleType())
orders.groupBy("customer_id"). \
    sum("price"). \
    withColumnRenamed("sum(price)", "total_spent"). \
    withColumn("total_spent", round_udf(func.col("total_spent"))). \
    sort("total_spent", ascending=False). \
    show()

spark.stop()

