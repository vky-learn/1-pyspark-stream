from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,struct,col,count,avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Kafkaconsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()


schema = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("activity", StringType(), True)
    ]
)   


messages = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "events")\
    .option("startingOffsets", "latest")\
    .load()

parsed_messages = messages.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

aggregated = parsed_messages.groupBy("location").agg(
    count("*").alias("total_events"),
    count("id").alias("total_users"),
    avg("id").alias("avg_id")
)

query = aggregated.writeStream\
    .outputMode("complete")\
    .format("console")\
    .start()

query.awaitTermination()
