
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# define schema in spark
redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("zSetEntries", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)
# payment schema
paymentJSONSchema = StructType([
    StructField("reservationId", StringType()),
    StructField("customerName", StringType()),
    StructField("date", StringType()),
    StructField("amount", StringType())
])
spark = SparkSession.builder.appName("payment-json").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# vehicle status
# read stream from kafka by subscribing to kafka topic
redisServerRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "redis-server")\
    .option("startingOffsets", "earliest")\
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("RedisData")

zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as payment from RedisData")

zSetEntriesDecodedStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("payment", unbase64(zSetEntriesEncodedStreamingDF.payment).cast("string"))

zSetEntriesDecodedStreamingDF.withColumn("payment", from_json("payment", paymentJSONSchema))\
    .select(col("payment.*"))\
    .createOrReplaceTempView("Payment")

# query data from temp view
paymentSelectStarDF = spark.sql("select * from Payment")

paymentSelectStarDF.writeStream.outputMode("append").format("console").start().awaitTermination()

# kafka SINK
paymentSelectStarDF.selectExpr("cast(reservationId as string) as key", "to_json(struct(*)) as value")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "payment-json")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint3")\
    .start()\
    .awaitTermination()
