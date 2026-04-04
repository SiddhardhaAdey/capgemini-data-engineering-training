Objective
In this phase, the focus is on performing data transformations using PySpark and understanding how SQL concepts like joins and aggregations are applied to real datasets.

Problem Summary
We used customer and sales datasets to analyze data and generate useful insights.

The tasks included:

Calculating total amount spent by each customer
Identifying the top 3 customers
Finding customers with no orders
Generating city-wise revenue
Calculating average order amount
Finding customers with multiple orders
Sorting customers based on total spending
Approach
Loaded datasets using PySpark
Cleaned the data by removing rows with null customer_id
Applied transformations:
Used groupBy for aggregations
Calculated sum, average, and count
Performed joins between customer and sales datasets
Applied filtering and sorting to get final results
Key Transformations Used
groupBy() → grouping data
agg() → calculating sum, average, count
join() → combining datasets
filter() → applying conditions
orderBy() → sorting data
round() → handling decimal values
Output / Results
The following results were generated:

Total amount spent by each customer
Top 3 customers based on spending
Customers with no orders
City-wise total revenue
Average order amount per customer
Customers with more than one order
Customers sorted by total spending
All outputs are stored in the outputs folder.

Data Engineering Considerations
Removed null values to ensure accurate results
Used consistent column names across transformations
Applied rounding to manage precision issues
Verified results at each step
Challenges Faced
Confusion between similar column names (order_amount vs total_amount)
Writing correct aggregation logic
Managing decimal precision in outputs
Understanding how joins work in PySpark
Learnings
Learned how to perform aggregations in PySpark
Understood how joins work with real datasets
Realized the importance of data cleaning
Improved overall PySpark workflow understanding
Files in this Folder
pyspark code → PySpark implementation
phase2_problem_statement → Problem description
outputs → Output screenshots
README.md → Project documentation
