## Objective  
In this phase, the focus is on handling messy data and applying data cleaning techniques using PySpark. This helps in understanding the importance of data quality before analysis.

---

## Problem Summary  
Worked with a customer dataset containing errors such as missing values, duplicates, and invalid data.  

Tasks performed:
- Identifying null values, duplicates, and invalid records  
- Cleaning the dataset by handling missing and incorrect values  
- Validating the cleaned data  
- Performing aggregation to count customers per city  

---

## Approach  
- Loaded dataset using PySpark  
- Identified data issues like null values, duplicates, and invalid age values  
- Cleaned the data by:  
  - Removing rows with null `customer_id`  
  - Replacing missing values with "Unknown"  
  - Removing duplicate records  
  - Filtering invalid age values (age > 0)  
- Validated results using row counts  
- Performed aggregation using `groupBy()`  

---

## Key Transformations  
- `dropna()` → remove null key values  
- `fillna()` → handle missing values  
- `dropDuplicates()` → remove duplicates  
- `filter()` → remove invalid data  
- `groupBy()` → aggregate data  
- `count()` → count customers per city  

---

## Results  
- Identified nulls, duplicates, and invalid records  
- Cleaned dataset with valid entries only  
- Compared row count before and after cleaning  
- Generated customer count per city  

---

## Data Engineering Considerations  
- Ensured data integrity by removing null keys  
- Maintained valid `customer_id` as primary key  
- Handled missing values carefully to avoid data loss  
- Removed duplicates to prevent incorrect counts  
- Filtered invalid data for accurate analysis  

---

## Challenges  
- Identifying different types of data issues  
- Deciding how to handle missing values  
- Avoiding loss of important data during cleaning  
- Understanding how bad data affects results  

---

## Learnings  
- Real-world data often requires cleaning  
- Data cleaning is essential before analysis  
- Invalid data can lead to wrong insights  
- Validation ensures correctness of results  

---

## Project Structure  
- `pyspark code/` → Data cleaning implementation  
- `outputs/` → Output screenshots  
- `phase3a_problem_statement/` → Problem description  
- `README.md` → Documentation  
