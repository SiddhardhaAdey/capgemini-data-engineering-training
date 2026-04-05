
## 1. Objective
Build an end-to-end ETL pipeline using PySpark to process customer and sales data and generate actionable business insights.

---

## 2. Problem Summary
This project analyzes customer and sales datasets to extract key insights such as:
- Daily sales trends
- City-wise revenue distribution
- Top 5 customers by spending
- Repeat customers
- Customer segmentation (Gold, Silver, Bronze)
- Final aggregated reporting table

---

## 3. Approach

### Extract
- Loaded customer and sales datasets from CSV files using PySpark DataFrames
- Verified schema and data using `show()` and `printSchema()`

### Transform
- Cleaned data by removing null and inconsistent records
- Calculated daily sales using `groupBy()`
- Joined datasets to compute city-wise revenue
- Identified top customers using sorting and limiting
- Detected repeat customers using aggregation and filtering
- Implemented customer segmentation using conditional logic (`when()`)
- Combined all transformations into a final reporting dataset

### Load
- Saved the final processed data into structured CSV format for downstream use

---

## 4. Key Transformations
- `groupBy()` and `agg()` → Aggregations (sum, count)
- `join()` → Dataset integration
- `filter()` and `dropna()` → Data cleaning
- `orderBy()` and `limit()` → Ranking and top records
- `withColumn()` and `when()` → Feature engineering
- `concat()` → Column transformation

---

## 5. Results
- Daily sales summary
- City-wise revenue
- Top 5 customers
- Repeat customers (order count > 1)
- Customer segmentation (Gold, Silver, Bronze)
- Final aggregated reporting table

---

## 6. Data Engineering Considerations
- Ensured data quality by handling null and inconsistent values
- Maintained consistent column naming across transformations
- Applied aggregation logic carefully to avoid incorrect results
- Structured output for reusability in downstream systems

---

## 7. Challenges
- Handling joins and avoiding column ambiguity
- Writing correct aggregation logic
- Implementing segmentation rules effectively
- Managing multiple transformation steps

---

## 8. Learnings
- Built a complete ETL pipeline using PySpark
- Strengthened understanding of joins, aggregations, and transformations
- Learned practical customer segmentation techniques
- Improved ability to convert raw data into meaningful insights

---

## 9. Project Structure
- solution.py → PySpark implementation  
- README.md → Documentation  
- OUTPUTS/ → Result screenshots  
