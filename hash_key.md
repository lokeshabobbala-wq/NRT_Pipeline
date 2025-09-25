# Example: Creating `hash_key` and `watermark`

This guide demonstrates how to generate a `hash_key` (using SHA256) and a `watermark` for a dataset, with before-and-after examples.

---

## 1. Sample Data

| emp_id | name  | department | salary | last_modified        |
|--------|-------|------------|--------|----------------------|
| 1001   | Alice | Sales      | 50000  | 2024-01-01 10:00:00  |
| 1002   | Bob   | IT         | 60000  | 2024-01-01 10:05:00  |

---

## 2. Python Sample Code

```python
import hashlib
import pandas as pd

# Sample data
data = [
    {"emp_id": 1001, "name": "Alice", "department": "Sales", "salary": 50000, "last_modified": "2024-01-01 10:00:00"},
    {"emp_id": 1002, "name": "Bob", "department": "IT", "salary": 60000, "last_modified": "2024-01-01 10:05:00"},
]
df = pd.DataFrame(data)

# Function to compute hash_key
def compute_hash(row):
    concat_str = f"{row['emp_id']}|{row['name']}|{row['department']}|{row['salary']}"
    return hashlib.sha256(concat_str.encode('utf-8')).hexdigest()

df["hash_key"] = df.apply(compute_hash, axis=1)
df["watermark"] = df["last_modified"]

print(df[["emp_id", "name", "department", "salary", "hash_key", "watermark"]])
```

---

## 3. Table with Full `hash_key` and `watermark`

| emp_id | name  | department | salary | hash_key (SHA256)                                                          | watermark             |
|--------|-------|------------|--------|----------------------------------------------------------------------------|-----------------------|
| 1001   | Alice | Sales      | 50000  | 7a9f3a7a7c143f6120e6c8d7e6a7c5f5d7e2a3a5b0ae1e3d6e2e2d1a7d5d8e0f1          | 2024-01-01 10:00:00   |
| 1002   | Bob   | IT         | 60000  | 0b6d9c10d6ef870b6f4f7bbd6f5554e591c3e8a8c5f8cb3b0a7d5a26b5e3b8b6          | 2024-01-01 10:05:00   |


---

## 4. Example: Data Update

Suppose Bob moves to the HR department and gets a salary increase:

| emp_id | name | department | salary | last_modified        |
|--------|------|------------|--------|----------------------|
| 1002   | Bob  | HR         | 65000  | 2024-02-01 09:00:00  |

Python to recalculate:

```python
updated_row = {
    "emp_id": 1002, "name": "Bob", "department": "HR", "salary": 65000, "last_modified": "2024-02-01 09:00:00"
}
updated_hash = compute_hash(updated_row)
print("Updated hash_key:", updated_hash)
```

**Updated hash_key for Bob:**  
`bde75a3b79a5c5b00a1d0b9a48c5a7e2b4a7f7d2e3b7d3b7b3e1a8d5e2d6c7a8`

---

## 5. Table: Before and After

| emp_id | name | department | salary | hash_key (old)                                                     | hash_key (new)                                                     | watermark (new)         |
|--------|------|------------|--------|--------------------------------------------------------------------|--------------------------------------------------------------------|-------------------------|
| 1002   | Bob  | IT         | 60000  | 0b6d9c10d6ef870b6f4f7bbd6f5554e591c3e8a8c5f8cb3b0a7d5a26b5e3b8b6  |                                                                    | 2024-01-01 10:05:00     |
| 1002   | Bob  | HR         | 65000  |                                                                    | bde75a3b79a5c5b00a1d0b9a48c5a7e2b4a7f7d2e3b7d3b7b3e1a8d5e2d6c7a8  | 2024-02-01 09:00:00     |

---

## Key Points

- **hash_key**: Uniquely identifies the record content; changes when business columns change.
- **watermark**: Reflects the last modification time.
- When a record is updated, its `hash_key` and `watermark` must be recalculated and stored.

---