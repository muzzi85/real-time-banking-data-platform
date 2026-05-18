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
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    ) \
    .option(
        "subscribe",
        "banking-transactions"
    ) \
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

fraud_df = json_df.filter(
    col("amount") > 4000
).withColumn(
    "fraud_reason",
    lit("HIGH_AMOUNT")
).withColumn(
    "risk_level",
    lit("HIGH")
).withColumn(
    "alert_timestamp",
    current_timestamp()
)

kafka_output_df = fraud_df.select(
    to_json(
        struct("*")
    ).alias("value")
)

query = kafka_output_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    ) \
    .option(
        "topic",
        "fraud-alerts"
    ) \
    .option(
        "checkpointLocation",
        "/tmp/fraud-checkpoint"
    ) \
    .start()

query.awaitTermination()