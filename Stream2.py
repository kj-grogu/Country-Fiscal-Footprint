from __future__ import print_function
import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import *
from pyspark.sql import SQLContext, Row



def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']




sc = SparkContext("local[2]","NetWordCount")
ssc = StreamingContext(sc,1)
sqlContext = SQLContext(sc)

topic  = "connect-test"
kvs = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer",{topic:1})
# words = kvs.map(lambda x:x[1])
parsed = kvs.map(lambda (key, value): json.loads(value))

# words = kvs.map(lambda line: line.split(","))
# words = kvs.flatMap(lambda line: line.split(" "))

# Convert RDDs of the words DStream to DataFrame and run SQL query
def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        print(rdd.take(1))

        # Convert RDD[String] to RDD[Row] to DataFrame
        # parts = rdd.map(lambda line: json.load(line,encoding="UTF-8"))

        transactions = rdd.map(lambda p: p['payload'])

        records = transactions.map(lambda p: p.split(","))

        rowRecord = records.map(lambda p: Row(location=p[0],country=p[1],transact=p[2],transaction=p[3], activity=p[4], \
                                              function=p[5], sector=p[6], sectorExp=p[7], measure=p[8], measureExp=p[9],\
                                              time=p[10], year=p[11], unitCode=p[12], unit=p[13], powerCodeCode=p[14] \
                                              , powerCode=p[15], referencePeriodCode=p[16], referncePeriod=p[17] \
                                              , value=p[18], flagCodes=p[19], flags=p[20]))


        for x in records.collect():
            print(x)

        for y in rowRecord.collect():
            print(y)

        transactionsDataFrame = spark.createDataFrame(rowRecord)
        changeTypedDef = transactionsDataFrame.withColumn("valueDouble",transactionsDataFrame["value"].cast("double"))

        # transactionsDataFrame.createOrReplaceTempView("alltransactions")
        changeTypedDef.createOrReplaceTempView("alltransactions")

        results = spark.sql("SELECT * FROM alltransactions")
        results.show()

        resultsByCountry = spark.sql("SELECT country, SUM(valueDouble) as Spending FROM alltransactions group by country")
        resultsByCountry.show()

    except:
        pass

# parsed.pprint()
parsed.foreachRDD(process)

ssc.start()
ssc.awaitTermination()