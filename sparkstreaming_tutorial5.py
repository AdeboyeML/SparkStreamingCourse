from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, FloatType, BooleanType, ArrayType, DateType

# TO-DO: create deposit a kafka message schema StructType including the following JSON elements:
# {"accountNumber":"703934969","amount":415.94,"dateAndTime":"Sep 29, 2020, 10:06:23 AM"}
# Cast the amount as a FloatType
bankDepositSchema = StructType(
[
    StructField("accountNumber", StringType()),
    StructField("amount", FloatType()),
    StructField("dateandTime", DateType())
])

# TO-DO: create a customer kafka message schema StructType including the following JSON elements:
# {"customerName":"Trevor Anandh","email":"Trevor.Anandh@test.com","phone":"1015551212","birthDay":"1962-01-01","accountNumber":"45204068","location":"Togo"}
bankCustomerSchema = StructType(
[
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
    StructField("accountNumber", StringType()),
    StructField("location", StringType())
])
# TO-DO: create a spark session, with an appropriately named application name
spark = SparkSession.builder.appName("customer-deposits").getOrCreate()
#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")
#TO-DO: read the atm-visits kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
bankDepositRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-deposits")\
    .option("startingOffsets", "earliest")\
    .load()
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankDepositStreamingDF = bankDepositRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
bankDepositStreamingDF.withColumn("value", from_json("value", bankDepositSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("BankDeposits")
# TO-DO: create a temporary streaming view called "BankDeposits" 
# it can later be queried with spark.sql
#TO-DO: using spark.sql, select * from BankDeposits where amount > 200.00 into a dataframe
bankDepositSelectStarDF = spark.sql("select * from BankDeposits where amount > 200.00")
#TO-DO: read the bank-customers kafka topic as a source into a streaming dataframe with the bootstrap server localhost:9092, configuring the stream to read the earliest messages possible                                    
bankCustomersRawStreamingDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "bank-customers")\
    .option("startingOffsets", "earliest")\
    .load()
#TO-DO: using a select expression on the streaming dataframe, cast the key and the value columns from kafka as strings, and then select them
bankCustomersStreamingDF = bankCustomersRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")
#TO-DO: using the kafka message StructType, deserialize the JSON from the streaming dataframe 
bankCustomersStreamingDF.withColumn("value", from_json("value", bankCustomerSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("BankCustomers")
# TO-DO: create a temporary streaming view called "BankCustomers" 
# it can later be queried with spark.sql
bankCustomersSelectStarDF = spark.sql("select customerName, accountNumber as customerNumber from BankCustomers")
#TO-DO: using spark.sql, select customerName, accountNumber as customerNumber from BankCustomers into a dataframe
customerDepositsDF = bankCustomersSelectStarDF.join(bankDepositSelectStarDF, expr("""
    accountNumber = customerNumber 
    """))

customerDepositsDF.writeStream.outputMode("append").format("console").start().awaitTermination()


#TO-DO: join the customer dataframe with the deposit dataframe

# TO-DO: write the stream to the console, and configure it to run indefinitely, the console output will look something like this:
#. +-------------+------+--------------------+------------+--------------+
#. |accountNumber|amount|         dateAndTime|customerName|customerNumber|
#. +-------------+------+--------------------+------------+--------------+
#. |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
#. |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
#. |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
#. +-------------+------+--------------------+------------+--------------+
