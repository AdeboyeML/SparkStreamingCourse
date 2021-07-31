
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("gear-position").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# read stream from kafka by subscribing to kafka topic
gearPositionRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "gear-position")\
    .option("startingOffsets", "earliest")\
    .load()

gearPositionStreamingDF = gearPositionRawStreamingDF.selectExpr("cast(key as string) truckId", "cast(value as string) gearPosition")

# create temp view
gearPositionStreamingDF.createOrReplaceTempView("GearPosition")
# query temp view using spark.sql
gearPositionSelectStarDF = spark.sql("select * from GearPosition")
# write view to a kafka broker at localhost:9092
gearPositionSelectStarDF.selectExpr("cast(truckId as string) key", "cast(gearPosition as string) value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "gear-position-updates")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()
