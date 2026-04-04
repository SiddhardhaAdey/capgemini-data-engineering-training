
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# start spark
spark = SparkSession.builder.appName("phase2").getOrCreate()

# load data
customers = spark.read.option("header","true").csv("/samples/customers.csv")
sales = spark.read.option("header","true").csv("/samples/sales.csv")

# remove nulls
customers = customers.dropna(subset=["customer_id"])
sales = sales.dropna(subset=["customer_id"])

# convert column
sales = sales.withColumn("total_amount", col("total_amount").cast("double"))

# total amount per customer
total = sales.groupBy("customer_id").sum("total_amount")
total.show()

# top 3 customers
top3 = total.orderBy("sum(total_amount)", ascending=False).limit(3)
top3.show()

# customers with no orders
no_orders = customers.join(sales, "customer_id", "left_anti")
no_orders.show()

# city revenue
city = customers.join(sales, "customer_id").groupBy("city").sum("total_amount")
city.show()

# average order
avg_val = sales.groupBy("customer_id").avg("total_amount")
avg_val.show()

# customers with more than one order
multi = sales.groupBy("customer_id").count().filter("count > 1")
multi.show()

# sort by spend
sorted_data = total.orderBy("sum(total_amount)", ascending=False)
sorted_data.show()

