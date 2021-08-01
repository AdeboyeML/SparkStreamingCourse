from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# define schema in spark
vehicleStatusSchema = StructType(
[
    StructField("truckNumber", StringType()),
    StructField("destination", StringType()),
    StructField("milesFromShop", StringType()),
    StructField("odometerReading", StringType())
])

vehicleCheckinSchema = StructType(
[
    StructField("reservationId", StringType()),
    StructField("locationName", StringType()),
    StructField("truckNumber", StringType()),
    StructField("status", StringType())
])
spark = SparkSession.builder.appName("vehicle-checkin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# vehicle status
# read stream from kafka by subscribing to kafka topic
vehicleStatusRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "vehicle-status")\
    .option("startingOffsets", "earliest")\
    .load()

vehicleStatusStreamingDF = vehicleStatusRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

vehicleStatusStreamingDF.withColumn("value", from_json("value", vehicleStatusSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("VehicleStatus")

# query data from temp view
vehicleStatusSelectStarDF = spark.sql("select truckNumber as statusTruckNumber, destination, milesFromShop, odometerReading from VehicleStatus")

# vehicle checkin
# read stream from kafka by subscribing to kafka topic
vehicleCheckinRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "vehicle-check-in")\
    .option("startingOffsets", "earliest")\
    .load()

vehicleCheckinStreamingDF = vehicleCheckinRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

vehicleCheckinStreamingDF.withColumn("value", from_json("value", vehicleCheckinSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("VehicleCheckin")

# query data from temp view
vehicleCheckinSelectStarDF = spark.sql("select truckNumber as checkinTruckNumber, reservationId, locationName, status from VehicleCheckin")

checkinStatusDF = vehicleCheckinStatusSelectStarDF.join(vehicleCheckinSelectStarDF, expr("""
statusTruckNumber = checkinTruckNumber"""))

checkinStatusDF.writeStream.outputMode("append").format("console").start().awaitTermination()
