# -*- coding: utf-8 -*-
"""
Created on Thu May 17 01:03:07 2018

@author: Sandman
"""
# Input File
# crime.csv -- (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)

# Question: Get the mothly count of Primary Crime Type with month sorted in ascending and crime count in descending order

#Spark parameter setting for execution

#spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/mothly_cnt_primary_type_sql.py

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql import SparkSession


conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

CrimeData = sc.textFile("/user/sahilbhange/data/crime/csv")
Header = CrimeData.first()
CrimeDataWithoutHeader = CrimeData.filter(lambda line: line != Header)

from pyspark.sql import Row

CrimeDataWithDateAndTypeDF = CrimeDataWithoutHeader.map(lambda crime: Row(crime_date = (crime.split(",")[2]), crime_type = (crime.split(",")[5]))).toDF()

CrimeDataWithDateAndTypeDF.registerTempTable("crime_data")

crimeCountPerMonthPerTypeDF = sqlContext.sql("select cast(concat(substr(crime_date, 7, 4), substr(crime_date, 0, 2)) as int) crime_month, count(1) crime_count_per_month_per_type, crime_type from crime_data group by cast(concat(substr(crime_date, 7, 4), substr(crime_date, 0, 2)) as int), crime_type order by crime_month, crime_count_per_month_per_type desc")

#crimeCountPerMonthPerTypeDF.show(100)

crimeCountPerMonthPerTypeDFToRDD = crimeCountPerMonthPerTypeDF.rdd.map(lambda row: str(row[0]) +("\t") + str(row[1]) +("\t") + row[2]).coalesce(1).saveAsTextFile("/user/sahilbhange/output/crimes/mnthly_cnt_primary_type_sql" , compressionCodecClass="org.apache.hadoop.io.compress.GzipCodec")
