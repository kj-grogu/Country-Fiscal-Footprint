# kafka_spark_streaming

    Installation 

	#Kafka
	
Download  kafka from https://kafka.apache.org/downloads.html 


From the extracted location,  Start the Zookeeper Server 
Using the below command from terminal

bin/zookeeper-server-start.sh config/zookeeper.properties


Now, Start the Kafka Server. Using the below command

bin/kafka-server-start.sh config/server.properties



Now before firing the import/export kafka command, we have to make changes in the below files to tell kafka about the source and destination of import. 

Go to the config folder, open “connect-file-source.properties” file and edit the file value to the file name from which we want import. In our case we will change this file to fiscal.data. We can also change the topic on which these import will be broadcasted, but for simplicity, let's keep it to default connect-test topic

Now let’s also modify the “connect-file-sink.properties” file to let kafka know where to sink all the incoming data. For this, we will change the file value to fiscal.sink.data. This is a mandatory step, without its completion we will not be able to close the import/export loop.


With these configurations done we will  now fire the import/export command for kafka from the terminal

bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties


In order to test if the configuration is working fine or not fire the below command to insert a test csv line in to our kafka source fiscal.data file.

echo -e “This, shall, reflect, in, Fiscal.data, and,  Fiscal.sink.data, ” >> fiscal.data

If all the configs are correct this data should reflect in both fiscal.data and fiscal.sink.data files



	#Spark
	
To download spark in your system use this url 

http://spark.apache.org/downloads.html


After downloading and extracting spark, use below commands to setup spark for kafka streaming



 export PATH=$PATH:/usr/local/spark/bin



Now we can submit the spark job, our python file to process the stream

spark-submit  --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1 Stream2.py --verbose >> sparklogs.txt

If the spark Job is started successfully after submission and we can read the logs of job in sparklogs.txt file.


To be assure that spark is listening to the kafka stream, again use the same test command form kafka installation and insert a csv data in the kafka source file. If the spark stream is working expectedly then the newly added data should reflect in  the sparklogs.txt file. 

echo -e “This, shall, reflect, AGAIN  in, Fiscal.data, and,  Fiscal.sink.data, ” >> fiscal.data

	#Nodejs


Now as the pipeline is tested for streaming, let fire our app.js file which read the actual fiscal.data csv file & inserts one line at a time in the kafka source file consequently creating a stream for kafka.

	#OpenSpendingApi


 We have use openspending api to get the fiscal data of few countries from Europe package it is a very large data set and contains about Hundred thousands transactions from several countries.


Data is processed as it arrives within the stream,in our case one transaction at a time. As a data is written in to fiscal.txt a kafka message is published under connect-test, this message is read and processed by spark using kafkaConnectStream . In code  we keep printing the processed data till the time, that is for the data that has arrived till now.
