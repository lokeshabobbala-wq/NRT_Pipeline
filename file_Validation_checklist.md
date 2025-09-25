# File Validation Checks: Steps & Explanations

A robust file validation process ensures only correct, complete, and secure files enter your data pipeline. Below are the essential checks to perform, **including KMS decryption**, and an explanation of why each is needed.

---

## 1. **File Presence in S3 Landing Zone**
**What:** Ensure the expected file(s) has arrived in the S3 landing folder for the current batch/date.
**Why:** Prevents pipeline failures caused by missing data; enables SLA monitoring for data delivery.

---

## 2. **Correct File Naming Pattern**
**What:** Check that the file name matches the expected naming convention (e.g., includes expected prefix, date, region, etc.).
**Why:** Prevents accidental processing of wrong files, previous batches, test files, or malicious files.

---

## 3. **Matching Metadata File Exists**
**What:** For every data file, verify that the corresponding metadata file (e.g., SC360metadata_*.csv) is present.
**Why:** Metadata files often contain critical info (expected size, schema, load instructions); their absence suggests incomplete delivery or upstream process failure.

---

## 4. **File Size Greater Than Zero**
**What:** Confirm the file has a non-zero size (not empty).
**Why:** Catches issues where data extracts failed but still created an empty file, avoiding downstream “no data” errors.

---

## 5. **File Size Matches Metadata**
**What:** Compare the actual file size in S3 to the size recorded in the metadata file.
**Why:** Prevents partial files (truncated uploads), data loss, or accidental re-uploads from being processed.

---

## 6. **File Extension and Type Check**
**What:** Validate that the file has the correct extension (e.g., .csv, .xlsx, .parquet) as per the pipeline’s configuration.
**Why:** Ensures the file can be parsed by downstream jobs; prevents pipeline failures due to unsupported formats.

---

## 7. **KMS Decryption (AWS Key Management Service)**
**What:** Attempt to decrypt the file using the correct AWS KMS key before processing.
**Why:**  
- Ensures data is encrypted at rest for compliance/security.
- Prevents unauthorized access—only jobs/services with correct permissions can decrypt.
- If decryption fails, it may indicate wrong keys, file tampering, or unauthorized delivery.

---

## 8. **File Integrity Check (Can Be Opened/Parsed)**
**What:** Open the file (e.g., try to read header/first row) to ensure it is not corrupted or truncated.
**Why:** Catches file corruption, incomplete transfers, or format mismatches before expensive downstream processing.

---

## 9. **Timeliness/SLA Check**
**What:** Validate that the file arrived within the expected time window for the batch (using S3 last-modified timestamp).
**Why:** Monitors upstream SLAs and enables rapid alerting/escalation if data is late.

---

## 10. **Duplicate Check**
**What:** Ensure there are no duplicate files for the same batch/time window (based on name, key, or metadata).
**Why:** Prevents duplicate processing, accidental overwrites, or confusion in downstream data lineage.

---

## 11. **Audit Logging**
**What:** Record every file validation action, result, and any error or rejection reason in an audit log (DB or S3).
**Why:** Provides traceability, compliance, and easy troubleshooting; enables automated alerting for failures.

---

## 12. **Reject/Quarantine Handling**
**What:** Move files that fail any check to a dedicated reject or quarantine folder, and trigger notification/alert.
**Why:** Ensures that only valid files proceed, failed files are isolated for investigation, and issues are visible to support teams.

---

# Example Flow with Explanations

1. **Check file exists in S3 and matches expected pattern**
2. **Check matching metadata file exists**
3. **Check file size > 0**
4. **Check file extension/type**
5. **Check file size matches metadata**
6. **Decrypt with KMS**
7. **Check file can be opened/read**
8. **Check file is on time (SLA)**
9. **Check for duplicates**
10. **Log all actions and results**
11. **Move valid files to processing/archive; move invalid to reject/quarantine and alert**

---

> **In summary:**  
> These steps protect your pipeline from missing, incomplete, corrupted, unauthorized, or out-of-date files—ensuring only valid, secure, and timely data is ingested for processing.
