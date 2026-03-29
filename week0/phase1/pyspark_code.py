from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("Phase1").getOrCreate()

data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, "Sita", "Chennai", 32),
    (3, "Arun", "Hyderabad", 28),
    (4, "Meena", "Bangalore", 35),
    (5, "Kiran", "Chennai", 22)
]

customers = spark.createDataFrame(data, ["id","name","city","age"])

customers.show()
customers.filter(customers.city == "Chennai").show()
customers.filter(customers.age > 25).show()
customers.select("name","city").show()
customers.groupBy("city").agg(count("*")).show()
