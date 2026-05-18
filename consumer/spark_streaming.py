from pyspark.sql import SparkSession

from pyspark.sql.functions import *

from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("BankingStreaming") \
    .master("spark://spark-master:7077") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    ) \
    .getOrCreate()

schema = StructType([

    StructField("customer_id", IntegerType()),

    StructField("customer_name", StringType()),

    StructField("transaction_type", StringType()),

    StructField("amount", DoubleType()),

    StructField("country", StringType()),

    StructField("timestamp", DoubleType())
])


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "banking-transactions") \
    .load()


parsed_df = df.selectExpr(
    "CAST(value AS STRING)"
)


json_df = parsed_df.select(
    from_json(
        col("value"),
        schema
    ).alias("data")
).select("data.*")


high_value_df = json_df.filter(
    col("amount") > 1000
)


query = high_value_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


query.awaitTermination()