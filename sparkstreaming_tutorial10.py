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

# reservation schema
reservationJSONSchema = StructType([
    StructField("reservationId", StringType()),
    StructField("customerName", StringType()),
    StructField("truckNumber", StringType()),
    StructField("reservationDate", StringType())
])

# payment schema
paymentJSONSchema = StructType([
    StructField("reservationId", StringType()),
    StructField("customerName", StringType()),
    StructField("date", StringType()),
    StructField("amount", StringType())
])

spark = SparkSession.builder.appName("reservation-payment").getOrCreate()
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

zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as redisEvent from RedisData")

zSetEntriesDecodedStreamingDF1 = zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))
zSetEntriesDecodedStreamingDF2 = zSetEntriesEncodedStreamingDF.withColumn("redisEvent", unbase64(zSetEntriesEncodedStreamingDF.redisEvent).cast("string"))

zSetEntriesDecodedStreamingDF1.filter(col("redisEvent").contains("reservationDate"))
zSetEntriesDecodedStreamingDF2.filter(~col("redisEvent").contains("reservationDate"))

zSetEntriesDecodedStreamingDF1.withColumn("reservation", from_json("redisEvent", reservationJSONSchema))\
    .select(col("reservation.*"))\
    .createOrReplaceTempView("Reservation")

zSetEntriesDecodedStreamingDF2.withColumn("payment", from_json("redisEvent", paymentJSONSchema))\
    .select(col("payment.*"))\
    .createOrReplaceTempView("Payment")

# query data from temp view
reservationStreamingDF = spark.sql("select * from Reservation where reservationDate is not null")
paymentStreamingDF = spark.sql("select reservationId as paymentReservationId, date as paymentDate, amount as paymentAmount from Payment")

paymentReservationStreamingDF = reservationStreamingDF.join(paymentStreamingDF, 
                                                            expr(""" reservationId = paymentReservationId
                                                            """)
                                                           )
paymentReservationStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()
