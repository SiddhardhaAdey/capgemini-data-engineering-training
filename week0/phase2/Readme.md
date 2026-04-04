

##  Objective
The objective of this phase is to perform data transformations using **PySpark** and understand how SQL concepts like **joins** and **aggregations** are applied to real-world datasets.

---

##  Problem Summary
In this project, customer and sales datasets were used to perform analysis and extract meaningful insights.

### Tasks Performed:
- Calculate total amount spent by each customer  
- Identify top 3 customers based on spending  
- Find customers with no orders  
- Generate city-wise revenue  
- Calculate average order amount per customer  
- Identify customers with multiple orders  
- Sort customers based on total spending  

---

##  Approach
- Loaded datasets using PySpark DataFrames  
- Cleaned data by removing rows with null `customer_id`  
- Applied transformations and aggregations  
- Performed joins between customer and sales datasets  
- Applied filtering and sorting for final insights  

---

##  Key Transformations Used
- `groupBy()` → Grouping data  
- `agg()` → Aggregations (sum, avg, count)  
- `join()` → Combining datasets  
- `filter()` → Applying conditions  
- `orderBy()` → Sorting results  
- `round()` → Handling decimal precision  

---

##  Output / Results
The following insights were generated:
- Total spending per customer  
- Top 3 highest spending customers  
- Customers with no orders  
- City-wise total revenue  
- Average order amount per customer  
- Customers with more than one order  
- Customers sorted by total spending  

 All outputs are available in the **`outputs/`** folder.

---

## Data Engineering Considerations
- Removed null values to ensure data accuracy  
- Maintained consistent column naming  
- Applied rounding to manage precision issues  
- Verified results at each transformation step  

---

##  Challenges Faced
- Confusion between similar column names (`order_amount` vs `total_amount`)  
- Writing correct aggregation logic  
- Managing decimal precision  
- Understanding join operations in PySpark  

---

## Learnings
- Gained hands-on experience with PySpark transformations  
- Understood joins and aggregations on real datasets  
- Learned the importance of data cleaning  
- Improved overall data processing workflow  

---

