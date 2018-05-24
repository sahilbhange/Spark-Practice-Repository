# -*- coding: utf-8 -*-
"""
Created on Wed May 9 21:33:37 2018

@author: Sandman
"""

# Question : Get Daily Revenue Per Product from Below files
# orders (order_id,order_date,order_customer_id,order_status)
# order_items (order_item_id,order_item_order_id,order_item_product_id,order_item_quantity,order_item_subtotal,order_item_product_price)

#Spark parameter setting for execution

#spark-submit --master yarn --conf spark.ui.port=12789 --num-executors 6 --executor-cores 2 --executor-memory 2G src/main/python/Order_items_daily_revenue.py

from pyspark import SparkConf, SparkContext

conf=SparkConf().setAppName("Dly-rev-per-prod").setMaster("yarn-client")

sc = SparkContext(conf=conf)

orders_data = sc.textFile("/user/sahilbhange/data/retail_db/orders")
orderItems_data = sc.textFile("/user/sahilbhange/data/retail_db/order_items")

ordersFiltered = orders_data. \
filter(lambda o: o.split(",")[3] in ["COMPLETE", "CLOSED"])

ordersMap = ordersFiltered. \
map(lambda o: (int(o.split(",")[0]), o.split(",")[1]))
orderItemsMap = orderItems_data. \
map(lambda oi: 
     (int(oi.split(",")[1]), (int(oi.split(",")[2]), float(oi.split(",")[4])))
   )

ordersJoin = ordersMap.join(orderItemsMap)
ordersJoinMap = ordersJoin. \
map(lambda o: ((o[1][0], o[1][1][0]), o[1][1][1]))

from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)

# If wants to read data from local directory
#productsRaw_data = open("/user/sahilbhange/data/retail_db/products/part-00000"). \
#read(). \
#splitlines()
#products = sc.parallelize(productsRaw_data)  

products = sc.textFile("/user/sahilbhange/data/retail_db/products")

productsMap = products. \
map(lambda p: (int(p.split(",")[0]), p.split(",")[2]))
dailyRevenuePerProductIdMap = dailyRevenuePerProductId. \
map(lambda r: (r[0][1], (r[0][0], r[1])))

dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)

dailyRevenuePerProduct = dailyRevenuePerProductJoin. \
map(lambda t: 
     ((t[1][0][0], -t[1][0][1]), (t[1][0][0], round(t[1][0][1], 2), t[1][1]))
   )
dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
dailyRevenuePerProductName = dailyRevenuePerProductSorted. \
map(lambda r: r[1])

dailyRevenuePerProductName.coalesce(2).saveAsTextFile("/user/sahilbhange/output/order_item_daily_rev")
