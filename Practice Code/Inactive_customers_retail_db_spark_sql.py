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

#spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/Inactive_customers_retail_db_spark_sql.py

#pyspark --master yarn --conf spark.ui.port=12643 --num-executors 2 --executor-memory 512MB

############################################################################################################################
################	SPARK SQL
############################################################################################################################


from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql import SparkSession


conf=SparkConf().setAppName("Crime-type-monthly-count").setMaster("yarn-client")

sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

orders = sc.textFile("/user/sahilbhange/data/retail_db/orders")
customers = sc.textFile("/user/sahilbhange/data/retail_db/customers")

from pyspark.sql import Row

ordersDF = orders.map(lambda orders: Row(orders_customer_id = int(orders.split(",")[2]))).toDF()
customersDF = customers.map(lambda customers: Row(customer_id = int(customers.split(",")[0]), lname =(customers.split(",")[2]), fname = (customers.split(",")[1]))).toDF()

ordersDF.registerTempTable("orders_df")
customersDF.registerTempTable("customers_df")

sqlContext.setConf("spark.sql.shuffle.partitions", "1")

ordersJoinCustomers = sqlContext.sql("select lname,fname from customers_df left outer join orders_df on customer_id = orders_customer_id where orders_customer_id is null order by lname,fname")

inactiveCustomerNames = ordersJoinCustomers.rdd.map(lambda row: row[0] + ", " + row[1])

inactiveCustomerNames.saveAsTextFile("/user/sahilbhange/output/inactive_customers_sprk_sql")

