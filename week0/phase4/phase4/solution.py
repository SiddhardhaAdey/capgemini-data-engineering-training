# ---------- IMPORTS ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, concat_ws

# ---------- SPARK SESSION ----------
spark = SparkSession.builder.appName("phase4_etl_pipeline").getOrCreate()

# ---------- LOAD DATA ----------
sales_data = spark.read.csv('/samples/sales.csv', header=True, inferSchema=True)
customer_data = spark.read.csv('/samples/customers.csv', header=True, inferSchema=True)

# ---------- DATA CLEANING ----------
sales_data = sales_data.dropna(subset=["customer_id"])
customer_data = customer_data.dropna(subset=["customer_id"])

sales_data = sales_data.dropDuplicates()
customer_data = customer_data.dropDuplicates()

sales_data = sales_data.filter(col("total_amount") > 0)

# =================================================
# 1. DAILY SALES
# =================================================
daily_sales_df = sales_data.groupBy("sale_date") \
    .agg(sum("total_amount").alias("total_sales"))

print("========= DAILY SALES =========")
daily_sales_df.show()

# =================================================
# 2. CITY-WISE REVENUE
# =================================================
combined_df = customer_data.join(sales_data, "customer_id")

city_revenue_df = combined_df.groupBy("city") \
    .agg(sum("total_amount").alias("total_revenue"))

print("========= CITY REVENUE =========")
city_revenue_df.show()

# =================================================
# 3. TOP 5 CUSTOMERS
# =================================================
top_customers_df = combined_df.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spending")) \
    .orderBy(col("total_spending").desc()) \
    .limit(5)

print("========= TOP 5 CUSTOMERS =========")
top_customers_df.show()

# =================================================
# 4. REPEAT CUSTOMERS (>1 ORDER)
# =================================================
repeat_customers_df = sales_data.groupBy("customer_id") \
    .agg(count("*").alias("order_count")) \
    .filter(col("order_count") > 1)

print("========= REPEAT CUSTOMERS =========")
repeat_customers_df.show()

# =================================================
# 5. CUSTOMER SEGMENTATION
# =================================================
customer_spending_df = combined_df.groupBy("customer_id") \
    .agg(sum("total_amount").alias("total_spending"))

customer_segment_df = customer_spending_df.withColumn(
    "segment",
    when(col("total_spending") > 10000, "Gold")
    .when((col("total_spending") >= 5000) & (col("total_spending") <= 10000), "Silver")
    .otherwise("Bronze")
)

print("========= CUSTOMER SEGMENTATION =========")
customer_segment_df.show()

# =================================================
# 6. FINAL REPORT
# =================================================
customer_name_df = customer_data.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("last_name"))
)

final_report_df = customer_name_df \
    .join(customer_segment_df, "customer_id", "left") \
    .join(repeat_customers_df, "customer_id", "left") \
    .select("full_name", "city", "total_spending", "order_count", "segment")

print("========= FINAL REPORT =========")
final_report_df.show()

# =================================================
# 7. SAVE OUTPUT
# =================================================
final_report_df.write.mode("overwrite") \
    .option("header", "true") \
    .csv('/tmp/output/report')
