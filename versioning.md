# Generic Versioning Approach in Published Table (SCD2)

## Purpose

To track the evolution of records over time and maintain data lineage, it's best practice to include:
- A `version` column: Indicates which version (1, 2, 3, ...) of the record it is for a given key.
- A `source_file` column: Indicates which file or batch was responsible for the change.

---

## Example Table Structure

```sql
CREATE TABLE published_table (
    -- Business key columns
    key_col1 VARCHAR,
    key_col2 VARCHAR,
    -- Data columns
    data_col1 VARCHAR,
    data_col2 DATE,
    -- Versioning and lineage columns
    version INT,
    source_file VARCHAR,
    is_active BOOLEAN,
    effective_ts TIMESTAMP,
    end_ts TIMESTAMP
    -- ...other columns as needed
);
```

---

## Versioning Logic

- **First time a key appears:**  
  - `version = 1`
- **When the same key is updated:**  
  - `version = previous version + 1`
- **`source_file`:**  
  - Set this to the current file or batch name, e.g., `datafile_20250911123455.csv`
- **`is_active`:**
  - Only the latest version per key is marked as `is_active = TRUE`
  - Previous (historic) records are marked as `is_active = FALSE` and have `end_ts` set

---

## Example Workflow

1. **Ingest new data with business keys and a batch filename**
2. **For each incoming record:**
    - Check if the key exists and is active in the published table.
    - If yes (changed):  
        - Set the old record’s `is_active = FALSE` and `end_ts = now()`
        - Insert new record with `version = previous version + 1`, `is_active = TRUE`, and `source_file = filename`
    - If no (new):  
        - Insert new record with `version = 1`, `is_active = TRUE`, and `source_file = filename`

---

## Example SQL (Pseudocode)

```sql
-- Deactivate old record if key exists and data is changed
UPDATE published_table
SET is_active = FALSE, end_ts = CURRENT_TIMESTAMP
WHERE key_col1 = :key1
  AND key_col2 = :key2
  AND is_active = TRUE
  AND (data_col1 <> :new_data1 OR data_col2 <> :new_data2);

-- Insert new version
INSERT INTO published_table
  (key_col1, key_col2, data_col1, data_col2, version, source_file, is_active, effective_ts, end_ts)
VALUES
  (:key1, :key2, :new_data1, :new_data2,
   COALESCE(
     (SELECT MAX(version) FROM published_table WHERE key_col1 = :key1 AND key_col2 = :key2), 0
   ) + 1,
   :filename, TRUE, CURRENT_TIMESTAMP, NULL);
```

> **Note:**  
> Not all databases support subqueries in the VALUES clause of an INSERT statement (for example, Amazon Redshift and some others).  
> **If your database does not support this pattern, you must compute the new version number in your ETL logic (e.g., in AWS Glue or Spark) before loading data into the target table.**

---

## Benefits

- **Traceability:** Know exactly which file or batch created or updated each record.
- **Versioning:** Easily view historical changes and roll back if needed.
- **SCD2 Compliance:** Only one active version per key; full change history retained.

---

## Summary Table

| Column      | Purpose                                  | Example Value                |
|-------------|------------------------------------------|------------------------------|
| version     | SCD2 version for the record              | 1, 2, 3, ...                 |
| source_file | Filename or batch responsible for change | datafile_20250911123455.csv  |
| is_active   | Current active record indicator          | TRUE or FALSE                |
| effective_ts| When this version became active          | 2025-09-11 12:34:55          |
| end_ts      | When this version was superseded         | 2025-09-12 09:00:00          |

---

This approach is generic and works for any SCD2 pipeline, regardless of business keys or domain.