# -*- coding: utf-8 -*-
"""
Created on Thu May 17 01:03:07 2018

@author: Sandman
"""
# Input File
# crime.csv -- (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)

# Question: Get the mothly count of Primary Crime Type with month sorted in ascending and crime count in descending order

#Spark parameter setting for execution

#spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/mothly_cnt_primary_type.py

from pyspark import SparkConf, SparkContext

conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

sc = SparkContext(conf=conf)

#read data from hdfs into RDD
crimeData = sc.textFile("/user/sahilbhange/data/crime/csv")
#for i in crimeData.take(10): print(i)

#filter out the header
crimeDataHeader = crimeData.first()
crimeDataHeader
crimeDataNoHeader = crimeData.filter(lambda x: x != crimeDataHeader)
#for i in crimeDataNoHeader.take(10):print(i)

#convert each element into tuple ((month, crime_type), 1)
def getCrimeDetails(x):
    data = x.split(",")
    dateWithTime = data[2]
    date = dateWithTime.split(" ")[0]
    month = int(date.split("/")[2]+date.split("/")[0])
    crimeType = str(data[5])
    return ((month, crimeType), 1)
	
	
crimeDataMap = crimeDataNoHeader.map(lambda x: getCrimeDetails(x))

#for i in crimeDataMap.take(10): print(i)

#perform aggregation to get ((month, crime_type), count)
monthlyCrimeCountByType = crimeDataMap.reduceByKey(lambda x,y: x+y)

#for i in monthlyCrimeCountByType.take(10): print(i)

#convert each element into
#((month, -count), month + "\t" + count + "\t" + crime_type)
monthlyCrimeCountByTypeMap = monthlyCrimeCountByType.map(lambda x: ((x[0][0], -x[1]), str(x[0][0]) + "\t" + str(x[1]) + "\t" + x[0][1]))

#for i in monthlyCrimeCountByTypeMap.take(10): print(i)

#Sort the data
monthlyCrimeCountByTypeSorted = monthlyCrimeCountByTypeMap.sortByKey()

#for i in monthlyCrimeCountByTypeSorted.take(10): print(i)

#discard the key and get the values
monthlyCrimeCountValues = monthlyCrimeCountByTypeSorted.map(lambda x: x[1])

#for i in monthlyCrimeCountValues.take(10): print(i)

#save the data in hdfs
#Save data in gzip format
monthlyCrimeCountValues.coalesce(2).saveAsTextFile("/user/sahilbhange/output/crimes/mnthly_cnt_primary_type", compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")

