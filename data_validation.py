import logging
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, DoubleType

class DataValidationException(Exception):
    pass

class DataValidator:
    """
    Class to perform all DQ checks as per config, handle rejects, and audit logging.
    """
    def __init__(self, df: DataFrame, config: dict, logger=None, file_name=None):
        self.df = df
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.file_name = file_name or "unknown_file"
        self.dq_checks = config.get("dq_checks", {})
        self.required_columns = config.get("required_columns", [])
        self.rejected_df = None  # DataFrame of rejected records with reasons
        self.audit_info = {}     # Dict to store audit info for external logging

    def select_required_columns(self):
        try:
            self.logger.info("Selecting required columns.")
            self.df = self.df.select([F.col(c) for c in self.required_columns])
        except Exception as e:
            self.logger.error(f"Could not select required columns: {e}")
            raise DataValidationException("Failed at select_required_columns")

    def remove_invisible_spaces(self):
        self.logger.info("Removing invisible spaces.")
        self.df = self.df.select(
            *[F.regexp_replace(F.col(c), "\u00A0", "").alias(c) if c in self.required_columns else F.col(c) for c in self.df.columns]
        )

    def trim_spaces(self):
        self.logger.info("Trimming spaces.")
        self.df = self.df.select(
            *[F.trim(F.col(c)).alias(c) if c in self.required_columns else F.col(c) for c in self.df.columns]
        )

    def remove_special_characters(self):
        self.logger.info("Removing special characters.")
        cleanup_cols = self.config.get("column_cleanup_columns", self.required_columns)
        self.df = self.df.select(
            *[F.regexp_replace(F.col(c), r"[\$@#,]", "").alias(c) if c in cleanup_cols else F.col(c) for c in self.df.columns]
        )

    def remove_alpha_from_numeric(self):
        self.logger.info("Removing alphabets from numeric columns.")
        int_cols = self.config.get("Integer_cleanup_columns", [])
        if int_cols:
            self.df = self.df.select(
                *[F.regexp_extract(F.col(c), r'[0-9]+', 0).alias(c) if c in int_cols else F.col(c) for c in self.df.columns]
            )

    def row_duplicate_check(self):
        self.logger.info("Performing row-level duplicate check.")
        pre_count = self.df.count()
        self.df = self.df.dropDuplicates(self.required_columns)
        post_count = self.df.count()
        if pre_count != post_count:
            self.logger.warning(f"{pre_count - post_count} duplicate rows found and removed.")
            # Optionally collect and save rejected duplicates to self.rejected_df for auditing

    def integer_format_check(self):
        self.logger.info("Checking integer format.")
        int_cols = self.config.get("int_columns", [])
        if int_cols:
            for col in int_cols:
                self.df = self.df.withColumn(
                    col,
                    F.when(F.col(col).rlike("^\d+$"), F.col(col)).otherwise(None)
                )

    def null_notnull_check(self):
        self.logger.info("Checking not-null columns.")
        notnull_cols = self.config.get("notnull_columns", [])
        for col in notnull_cols:
            self.df = self.df.filter(F.col(col).isNotNull() & (F.col(col) != ""))

    def date_format_check(self):
        self.logger.info("Checking date format columns.")
        date_columns = self.config.get("date_columns", [])
        for date_map in date_columns:
            for col, fmt in date_map.items():
                self.df = self.df.withColumn(
                    col,
                    F.when(F.to_date(F.col(col), fmt).isNotNull(), F.col(col)).otherwise(None)
                )

    def timestamp_format_check(self):
        self.logger.info("Checking timestamp format columns.")
        ts_columns = self.config.get("timestamp_columns", [])
        for ts_map in ts_columns:
            for col, fmt in ts_map.items():
                self.df = self.df.withColumn(
                    col,
                    F.when(F.to_timestamp(F.col(col), fmt).isNotNull(), F.col(col)).otherwise(None)
                )

    def standardize_date_timestamp(self):
        self.logger.info("Standardizing date columns.")
        date_columns = self.config.get("date_columns", [])
        for date_map in date_columns:
            for col, fmt in date_map.items():
                self.df = self.df.withColumn(
                    col,
                    F.date_format(F.to_date(F.col(col), fmt), "yyyy-MM-dd")
                )

    def replace_zeros_placeholders_with_null_in_date(self):
        self.logger.info("Replacing zero/placeholder values with null in date columns.")
        placeholders = [0, '-', 'Not available', '0.0', 'NA', 'nan', '', None]
        date_columns = self.config.get("date_columns", [])
        for date_map in date_columns:
            for col in date_map.keys():
                self.df = self.df.withColumn(
                    col,
                    F.when(F.col(col).isin(placeholders), None).otherwise(F.col(col))
                )

    def cast_columns_to_integer_double(self):
        self.logger.info("Casting columns to Integer/Double.")
        int_cols = self.config.get("cast_to_int_columns", [])
        double_cols = self.config.get("cast_to_double_columns", [])
        for c in int_cols:
            self.df = self.df.withColumn(c, F.col(c).cast(IntegerType()))
        for c in double_cols:
            self.df = self.df.withColumn(c, F.col(c).cast(DoubleType()))

    def decimal_column_normalization(self):
        self.logger.info("Normalizing decimal columns.")
        decimal_cols = self.config.get("decimal_cal_columns", [])
        for c in decimal_cols:
            self.df = self.df.withColumn(c, F.round(F.col(c), 2)) # Example: 2 decimals

    def reject_file_and_audit(self, reason="DQ Checks not enabled for file"):
        self.logger.error(f"File rejected: {self.file_name} - Reason: {reason}")
        # Set audit info for failed file
        self.audit_info['status'] = "Rejected"
        self.audit_info['reason'] = reason
        # Optionally, write reject file to S3 or DB here
        # Optionally, trigger audit log function here

    def run_all_checks(self):
        """
        Runs all enabled DQ checks as per config.
        Handles rejection if dq_checks is missing/empty.
        Returns: Clean DataFrame if passed, else None (and sets audit_info)
        """
        try:
            if not self.dq_checks or not any(self.dq_checks.values()):
                self.reject_file_and_audit("No DQ checks enabled in config (critical misconfiguration).")
                return None
            self.select_required_columns()
            if self.dq_checks.get("remove_invisible_spaces"): self.remove_invisible_spaces()
            if self.dq_checks.get("trim_spaces"): self.trim_spaces()
            if self.dq_checks.get("remove_special_characters"): self.remove_special_characters()
            if self.dq_checks.get("remove_alpha_from_numeric"): self.remove_alpha_from_numeric()
            if self.dq_checks.get("row_duplicate_check"): self.row_duplicate_check()
            if self.dq_checks.get("integer_format_check"): self.integer_format_check()
            if self.dq_checks.get("null_notnull_check"): self.null_notnull_check()
            if self.dq_checks.get("date_format_check"): self.date_format_check()
            if self.dq_checks.get("timestamp_format_check"): self.timestamp_format_check()
            if self.dq_checks.get("standardize_date_timestamp"): self.standardize_date_timestamp()
            if self.dq_checks.get("replace_zeros_placeholders_with_null_in_date"): self.replace_zeros_placeholders_with_null_in_date()
            if self.dq_checks.get("cast_columns_to_integer_double"): self.cast_columns_to_integer_double()
            if self.dq_checks.get("decimal_column_normalization"): self.decimal_column_normalization()
            # Add further DQ checks and their corresponding config keys as needed

            self.audit_info['status'] = "Passed"
            self.audit_info['reason'] = "All enabled DQ checks passed"
            return self.df
        except Exception as e:
            self.reject_file_and_audit(str(e))
            return None

    # Additional method to fetch audit info for use in audit tables/logging
    def get_audit_info(self):
        return self.audit_info

    # Optionally, implement a method to write rejected records to S3, DB, or log
    # def write_rejected_records(self, s3_bucket, s3_key): ...

# Example usage:
# validator = DataValidator(spark_df, config, logger, file_name="EGI_2_0_EMEA_INC_20240926000000.xlsx")
# clean_df = validator.run_all_checks()
# if clean_df is None:
#     # Handle rejection, use validator.get_audit_info() for audit log
