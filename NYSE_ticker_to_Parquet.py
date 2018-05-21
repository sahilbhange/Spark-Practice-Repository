# -*- coding: utf-8 -*-
"""
Created on Sun May 20 21:39:56 2018

@author: Sandman
"""

# Save NYSE ticker csv file data Parquet format using below information

# Input File : 
# Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
# Convert NYSE Data to Parquet

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 3 --executor-memory 1G src/main/python/NYSE_ticker_to_Parquet.py


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

#sqlContext = SQLContext(sc)

nyseRDD = sc.textFile("/user/sahilbhange/nyse")
#for i in nyseRDD.take(10): print(i)

nyseMap = nyseRDD.map(lambda x: x.split(","))
#for i in nyseMap.take(10): print(i)

nyseRDDMap = nyseMap.map(lambda x: (str(x[0]), str(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), int(x[6])))
#for i in nyseRDDMap.take(10): print(i)

nyseDF = nyseRDDMap.toDF(schema=["stockticker", "transactiondate", "openprice", "highprice", "lowprice", "closeprice", "volume"])

nyseDF.coalesce(1).write.format("parquet").save("/user/sahilbhange/output/nyse/")

#for i in sc.textFile("/user/sahilbhange/output/nyse/").take(10): print(i)

#sqlContext.read.parquet("/user/sahilbhange/output/nyse").show()