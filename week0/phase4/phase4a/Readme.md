# PySpark Segmentation Project – Phase 4A

## 1. Objective
Apply bucketing and segmentation techniques in PySpark to convert continuous customer data into meaningful categories for better analysis.

---

## 2. Problem Summary
This phase focuses on analyzing customer and sales data to:
- Calculate total customer spending and order frequency
- Apply multiple segmentation techniques
- Compare different approaches to understand their impact on results

---

## 3. Approach

### Data Preparation
- Loaded customer and sales datasets from CSV files
- Verified schema and data using `show()` and `printSchema()`
- Joined datasets using `customer_id`
- Created `full_name` column using `concat_ws()`

### Aggregation
- Calculated `total_spend` using `sum()`
- Calculated `total_orders` using `count()`

### Segmentation Techniques
- **Conditional Segmentation** using `when()` (fixed thresholds)
- **Quantile-Based Segmentation** using `approxQuantile()` (data-driven)
- **Bucketizer Segmentation** using MLlib (range-based binning)
- **Window-Based Ranking** using `percent_rank()` (relative ranking)

### Analysis
- Grouped customers by segments
- Compared distribution across different segmentation methods

---

## 4. Key Transformations
- `groupBy()` and `agg()` → Aggregations  
- `join()` → Dataset merging  
- `withColumn()` → Feature creation  
- `when()` → Rule-based segmentation  
- `approxQuantile()` → Data-driven thresholds  
- `Bucketizer` → Range-based bucketing  
- Window functions (`percent_rank()`) → Ranking  

---

## 5. Results
- Customer segmentation using multiple techniques  
- Segment-wise customer distribution  
- Quantile-based segmentation insights  
- Bucketized spending categories  
- Ranking-based customer analysis  

---

## 6. Data Engineering Considerations
- Ensured correct schema using `inferSchema`
- Avoided duplication during aggregation
- Maintained consistent column naming
- Used appropriate segmentation techniques based on data distribution

---

## 7. Challenges
- Understanding differences between segmentation methods  
- Implementing quantile-based segmentation correctly  
- Applying window functions effectively  
- Comparing outputs across techniques  

---

## 8. Learnings
- Learned practical bucketing and segmentation techniques  
- Understood difference between fixed and dynamic segmentation  
- Gained experience with MLlib Bucketizer and window functions  
- Improved ability to analyze customer behavior using multiple approaches  

---

## 9. Project Structure
solution/ # PySpark implementation
phase4a_problem_statement/ # Problem description
OUTPUTS/ # Output results
README.md # Documentation
