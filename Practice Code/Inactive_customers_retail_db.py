# -*- coding: utf-8 -*-
"""
Created on Sun May 20 18:39:44 2018

@author: Sandman
"""
# Input File
# Question : Get Daily Revenue Per Product from Below files
# orders (order_id,order_date,order_customer_id,order_status)

# customers (customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode)

# Question: Get the inactive customers from the data using orders and customers files

#Spark parameter setting for execution

##spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/Inactive_customers_retail_db.py

#pyspark --master yarn --conf spark.ui.port=12643 --num-executors 2 --executor-memory 512MB

from pyspark import SparkConf, SparkContext

conf=SparkConf().setAppName("Inactive-customers-retail-db").setMaster("yarn-client")

sc = SparkContext(conf=conf)

#load the data from dfs and create Spark RDD
ordersRDD = sc.textFile("/user/sahilbhange/data/retail_db/orders")
#for i in ordersRDD.take(10): print(i)

customersRDD = sc.textFile("/user/sahilbhange/data/retail_db/customers")
#for i in customersRDD.take(10): print(i)


# Select customer_id and order_customer_id
ordersRDDKeyVal = ordersRDD.map(lambda x: (int(x.split(",")[2]), 1))
#for i in ordersRDDKeyVal.take(10): print(i)


# Select customer_id ,first_name and last name
customersRDDKeyVal = customersRDD.map(lambda x: ((int(x.split(",")[0]), (x.split(",")[2], x.split(",")[1]))))
#for i in customersRDDKeyVal.take(10): print(i)


# Join on customer_id
ordersJoinCustomers = customersRDDKeyVal.leftOuterJoin(ordersRDDKeyVal)
#for i in ordersJoinCustomers.take(100): print(i)

#Select only inactive customers i.e. (customers)
inactiveCustomersSorted = ordersJoinCustomers.filter(lambda x: x[1][1]== None).map(lambda x: ((x[1][0][0], x[1][0][1]), x[1][1])).sortByKey()
#for i in inactiveCustomersSorted.take(10): print(i)
#(or)
#inactiveCustomersSorted = ordersJoinCustomers.filter(lambda x: x[1][1]== None).map(lambda x: x[1]).sortByKey()
#for i in inactiveCustomersSorted.take(10): print(i)

#saveAsTextFile -- Customer First name and last name
inactiveCustomersSortedSaved = inactiveCustomersSorted.map(lambda x: x[0][0] + ", " + x[0][1])
inactiveCustomersSortedSaved.coalesce(1).saveAsTextFile("/user/sahilbhange/output/inactive_customers")

#run in pyspark
#for i in sc.textFile("/user/sahilbhange/output/inactive_customers_temp/part-00000").take(10): print(i)

# To validate results

#hdfs dfs -ls /user/sahilbhange/solutions/solutions02/inactive_customers/part-00000

#execution





