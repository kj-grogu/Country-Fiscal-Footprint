from __future__ import print_function

import sys
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row, SparkSession

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']




sc = SparkContext("local[2]","NetWordCount")
ssc = StreamingContext(sc,1)

topic  = "connect-test"
kvs = KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer",{topic:1})
words = kvs.map(lambda x:x[1])
# words = kvs.map(lambda line: line.split(","))
# words = kvs.flatMap(lambda line: line.split(" "))

# Convert RDDs of the words DStream to DataFrame and run SQL query


def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowSplitted = rdd.flatMap(lambda line: line.split(","))
        rowRdd = rowSplitted.map(lambda w: Row(word=w))
        # rowRdd = rdd.flatMap(lambda line: line.split(","))
        wordsDataFrame = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame.
        wordsDataFrame.createOrReplaceTempView("words")

        # Do word count on table using SQL and print it
        wordCountsDataFrame = \
            spark.sql("select word, count(*) as total from words group by word")
        wordCountsDataFrame.show()

    except:
        pass

words.pprint()
words.foreachRDD(process)

ssc.start()
ssc.awaitTermination()