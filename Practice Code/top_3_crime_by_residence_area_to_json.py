# -*- coding: utf-8 -*-
"""
Created on Sun May 20 20:58:24 2018

@author: Sandman
"""

# Top 3 crimes based on number of incidents in Residence Area

# Input File
#crime.csv -- (ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location)

# Question: Get the top 3 crimes based on number of incidents in Residence Area

# Spark parameter setting for execution

# spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/top_3_crime_by_residence_area.py


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql import SparkSession


conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

crimeDt = sc.textFile('/user/sahilbhange/data/crime/csv')

crimeHeader = crimeDt.first()

# Remove the header from the data
crimeData = crimeDt.filter(lambda c: c != crimeHeader)

#for i in crimeData.take(10): print(i)

# import spark SQL functions for count and order
from pyspark.sql import *
from pyspark.sql.functions import *
import re

regex = re.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")

# select the crime type and crime location
crime_DF = crimeData. \
map(lambda c:
	Row(crime_type=regex.split(c)[5], 
		location_desc=regex.split(c)[7]
	   )
).toDF()


#crime_DF.printSchema()
#root
# |-- crime_type: string (nullable = true)
# |-- location_desc: string (nullable = true)

# to see the data frame count
#crime_DF.count()

# Aggregate the crime type to get the count of each crime and sort it in the descending order to select forst 3 records

top3CrimesByType_DF = crime_DF. \
filter("location_desc like 'RESIDENCE%'"). \
groupBy('crime_type'). \
agg(count('crime_type').alias('num_incidents')). \
orderBy(desc('num_incidents')).limit(3)

#top3CrimesByType_DF.show()


# Write Data to HDFS in json format
top3CrimesByType_DF.rdd.toDF(schema=['Crime Type', 'Number of Incidents']). \
write.json('/user/sahilbhange/output/crimes/top_3_crime_types')




#Spark SQL code

#from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext
#from pyspark.sql import SparkSession


#conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

#sc = SparkContext(conf=conf)

#sqlContext = SQLContext(sc)

#crime_DF.registerTempTable("crimetable")

#crime_desc = sqlContext.sql("describe crimetable")

#resultSQL_10 = sqlContext.sql("select crime_type,count(*) from crimetable where location_desc = 'RESIDENCE' group by crime_type limit 10 ")

#resultSQL = sqlContext.sql("select crime_type,count(*) as number_of_incident from crimetable where location_desc = 'RESIDENCE' group by crime_type order by number_of_incident desc limit 3 ")

#resultSQL.toJSON.repartition(1).saveAsTextFile("/user/sahilbhange/solutions/solution03/top_3_crime_types_sql")