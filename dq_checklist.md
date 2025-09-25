# Data Quality (DQ) Checks: Steps & Explanations

A robust DQ process ensures that every record entering the pipeline is accurate, consistent, and fit for downstream analytics and business decisions. Below are the essential DQ checks with clear explanations for each step.

---

## 1. **Remove Non-Breakable/Invisible Spaces**
**What:** Strip unicode and non-breaking spaces from all string columns.  
**Why:** Prevents issues with keys, joins, and analytics caused by invisible characters that create false discrepancies.

---

## 2. **Trim Leading and Trailing Spaces**
**What:** Remove whitespace from the start and end of all string fields.  
**Why:** Ensures consistent matching and prevents subtle errors in key matching or aggregations.

---

## 3. **Remove/Replace Special Characters**
**What:** Remove or replace special characters in specified columns.  
**Why:** Prevents issues in numeric conversion, business logic, or integrations that expect only certain character sets.

---

## 4. **Remove Alphabets from Numeric Columns**
**What:** Remove alphabetic characters from columns that should contain only numbers.  
**Why:** Ensures columns expected to be integers or floats are clean and valid.

---

## 5. **Row-Level Duplicate Check**
**What:** Identify and handle duplicate rows based on all columns or a subset (according to config).  
**Why:** Prevents over-counting, duplicate records, merge issues, and downstream confusion.

---

## 6. **Integer Format Check**
**What:** Ensure integer columns contain only valid integers and can be cast without error.  
**Why:** Prevents data type errors and analytic calculation problems.

---

## 7. **Null/Not Null Check**
**What:** Ensure all NOT NULL or required columns have no missing/null/blank values.  
**Why:** Prevents incomplete or unusable records from entering the curated/published layer.

---

## 8. **Date Format Check**
**What:** Validate that all date columns conform to the expected format (e.g., yyyy-MM-dd).  
**Why:** Ensures correct partitioning, time-series analysis, and SCD2 versioning.

---

## 9. **Timestamp Format Check**
**What:** Validate timestamp columns for correct format and parse-ability.  
**Why:** Ensures reliable event sequencing and auditability.

---

## 10. **Standardize Date and Timestamp Formats**
**What:** Convert all dates/timestamps to a consistent format for storage and downstream use.  
**Why:** Prevents bugs in partitioning, joins, and reporting.

---

## 11. **Replace Zeros/Placeholders with Null in Date Columns**
**What:** Convert known placeholder values (e.g., 0, ‘-’, ‘Not available’) to null in date/date-like columns.  
**Why:** Prevents invalid dates from skewing time-based analytics.

---

## 12. **Cast Columns to Integer/Double**
**What:** Explicitly cast columns to integer or double as per schema.  
**Why:** Ensures schema consistency and avoids downstream type errors.

---

## 13. **Replace Nulls with Defaults (Config-Driven)**
**What:** For specific columns, replace nulls with business-approved defaults (e.g., 0 for int).  
**Why:** Ensures downstream logic does not break; should be used judiciously to avoid hiding true data issues.

---

## 14. **Decimal Columns Calculation/Normalization**
**What:** Adjust decimals, apply rounding or scaling as needed per business logic.  
**Why:** Ensures numeric values are in correct units and ready for reporting or aggregation.

---

## 15. **Reject and Audit Logging**
**What:** Any record failing a DQ check is moved to a reject set, with a reason code, and written to S3/DB for review.  
**Why:** Enables traceability, accountability, and rapid issue investigation.

---

## Incremental Load (SCD2/CDC) Must-Have DQ Checks

### 16. **Primary/Business Key Uniqueness in Incoming Batch**
**What:** Ensure that the primary/business key(s) are unique within the incremental batch.  
**Why:** Prevents duplicate updates, merges, or SCD2 versioning errors.

**Example Code:**

**Spark:**
```python
from pyspark.sql.functions import col

duplicate_keys = df.groupBy("business_key1", "business_key2").count().filter(col("count") > 1)
if duplicate_keys.count() > 0:
    print("Duplicates found in incoming batch!")
```

**SQL:**
```sql
SELECT business_key1, business_key2, COUNT(*)
FROM incoming_batch
GROUP BY business_key1, business_key2
HAVING COUNT(*) > 1;
```

---

### 17. **Watermark/Incremental Column Validation**
**What:** Check that the incremental column (such as `event_time`, `extraction_date`, or similar) is present, not null, and within expected bounds for all records in the batch.  
**Why:** Ensures only valid and correctly-timed data is processed, preventing the loading of stale, future-dated, or missing data.

**Example Code:**

**Spark:**
```python
from pyspark.sql.functions import col, current_date

invalid_watermarks = df.filter(
    col("event_time").isNull() |
    (col("event_time") > current_date()) |
    (col("event_time") < "2022-01-01")
)
if invalid_watermarks.count() > 0:
    print("Invalid watermarks found!")
```

**SQL:**
```sql
SELECT *
FROM incoming_batch
WHERE event_time IS NULL
   OR event_time > CURRENT_DATE
   OR event_time < '2022-01-01';
```

---

### 18. **Hash Column Calculation and Validation**
**What:** Confirm that the hash column (used for change detection) is correctly calculated for each record and not null.  
**Why:** Enables granular and reliable detection of changed records for SCD2 merges or CDC workflows, preventing missed or unnecessary updates.

**Example Code:**

**Spark:**
```python
from pyspark.sql.functions import sha2, concat_ws

# Assuming hash_col is SHA256 of key and value columns
df = df.withColumn("expected_hash", sha2(concat_ws("|", "col1", "col2", "col3"), 256))
invalid_hash = df.filter(df.hash_col.isNull() | (df.hash_col != df.expected_hash))
if invalid_hash.count() > 0:
    print("Hash calculation/validation failed!")
```

**SQL:**
```sql
SELECT *
FROM incoming_batch
WHERE hash_col IS NULL
   OR hash_col <> SHA2(CONCAT_WS('|', col1, col2, col3), 256);
```

---

### 19. **Duplicate/Replay Detection Across Batches**
**What:** For each incoming record, verify that the same key + watermark + hash combination does not already exist in the target tables.  
**Why:** Prevents accidental reprocessing, duplication, or replay of records across different batches, ensuring data integrity and idempotency.

**Example Code:**

**Spark:**
```python
# target_df: historical data
dupes = df.join(
    target_df,
    on=[
        "business_key1", "business_key2", "event_time", "hash_col"
    ],
    how="inner"
)
if dupes.count() > 0:
    print("Replay/duplicate records detected across batches!")
```

**SQL:**
```sql
SELECT i.*
FROM incoming_batch i
JOIN target_table t
  ON i.business_key1 = t.business_key1
 AND i.business_key2 = t.business_key2
 AND i.event_time = t.event_time
 AND i.hash_col = t.hash_col;
```

---

> **Summary:**  
> These DQ checks ensure your pipeline only ingests high-quality, reliable, and analytics-ready data.  
> Each check is designed to mitigate a specific data risk and can be enabled/disabled per table or column via configuration.
