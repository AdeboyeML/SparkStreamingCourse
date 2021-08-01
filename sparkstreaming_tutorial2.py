
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# define schema in spark
kafka_message_schema = StructType(
[
    StructField("truckNumber", StringType()),
    StructField("destination", StringType()),
    StructField("milesFromShop", StringType()),
    StructField("odometerReading", StringType())
])
spark = SparkSession.builder.appName("vehicle-status").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# read stream from kafka by subscribing to kafka topic
vehicleStatusRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "vehicle-status")\
    .option("startingOffsets", "earliest")\
    .load()

vehicleStatusStreamingDF = vehicleStatusRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

vehicleStatusStreamingDF.withColumn("value", from_json("value", kafka_message_schema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("VehicleStatus")

# query data from temp view
vehicleStatusSelectStarDF = spark.sql("select * from VehicleStatus")

vehicleStatusSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination()
