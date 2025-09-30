import logging
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType

class DataValidationException(Exception):
    pass

class DataValidator:
    """
    Performs DQ checks as per config, handles rejects, and audit logging.
    Designed for simple configs with only column_type_map and minimal keys.
    """
    def __init__(self, df: DataFrame, config: dict, logger=None, file_name=None):
        self.df = df
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self.file_name = file_name or "unknown_file"
        self.dq_checks = config.get("dq_checks", {})
        self.column_type_map = config.get("column_type_map", {})
        self.required_columns = list(self.column_type_map.keys())
        self.audit_info = {}

    def select_required_columns(self):
        try:
            self.logger.info("Selecting required columns.")
            self.df = self.df.select([F.col(c) for c in self.required_columns])
        except Exception as e:
            self.logger.error(f"Could not select required columns: {e}")
            raise DataValidationException("Failed at select_required_columns")

    def trim_spaces(self):
        self.logger.info("Trimming spaces.")
        for c, dtype in self.df.dtypes:
            if dtype == 'string':
                self.df = self.df.withColumn(c, F.trim(F.col(c)))
        return self

    def remove_special_characters(self):
        self.logger.info("Removing special characters.")
        for c in self.df.columns:
            self.df = self.df.withColumn(
                c, F.regexp_replace(F.col(c), r'[\x00-\x1f\x7f-\x9f]', '')
            )
        return self

    def remove_alpha_from_numeric(self):
        self.logger.info("Removing alphabets from numeric columns.")
        # Example: remove all non-numeric characters from decimal columns
        for c, dtype in self.column_type_map.items():
            if dtype.startswith("decimal"):
                self.df = self.df.withColumn(c, F.regexp_replace(F.col(c), r"[^0-9.\-]", ""))
        return self

    def row_duplicate_check(self):
        self.logger.info("Performing row-level duplicate check.")
        pre_count = self.df.count()
        self.df = self.df.dropDuplicates(self.required_columns)
        post_count = self.df.count()
        if pre_count != post_count:
            self.logger.warning(f"{pre_count - post_count} duplicate rows found and removed.")

    def null_notnull_check(self):
        self.logger.info("Checking not-null columns.")
        notnull_cols = self.config.get("notnull_columns", [])
        for col in notnull_cols:
            self.df = self.df.filter(F.col(col).isNotNull() & (F.col(col) != ""))

    def decimal_column_normalization(self):
        self.logger.info("Normalizing decimal columns (removing commas).")
        for c, dtype in self.column_type_map.items():
            if dtype.startswith("decimal"):
                self.df = self.df.withColumn(c, F.regexp_replace(F.col(c), ",", ""))
        return self

    def cast_columns_to_types(self, decimal_precision=30, decimal_scale=15):
        self.logger.info("Casting columns to their final types as per column_type_map.")
        df = self.df
        for col_name, dtype in self.column_type_map.items():
            if dtype == "string":
                df = df.withColumn(col_name, F.col(col_name).cast("string"))
            elif dtype == "date":
                df = df.withColumn(col_name, F.col(col_name).cast("date"))
            elif dtype == "timestamp":
                df = df.withColumn(col_name, F.col(col_name).cast("timestamp"))
            elif dtype.startswith("decimal"):
                df = df.withColumn(
                    col_name, F.col(col_name).cast(DecimalType(decimal_precision, decimal_scale))
                )
        self.df = df
        return self

    def reject_file_and_audit(self, reason="DQ Checks not enabled for file"):
        self.logger.error(f"File rejected: {self.file_name} - Reason: {reason}")
        self.audit_info['status'] = "Rejected"
        self.audit_info['reason'] = reason

    def run_all_checks(self):
        """
        Runs all enabled DQ checks as per config.
        Returns: Clean DataFrame if passed, else None (and sets audit_info)
        """
        try:
            if not self.dq_checks or not any(self.dq_checks.values()):
                self.reject_file_and_audit("No DQ checks enabled in config (critical misconfiguration).")
                return None
            self.select_required_columns()
            if self.dq_checks.get("trim_spaces"): self.trim_spaces()
            if self.dq_checks.get("remove_special_characters"): self.remove_special_characters()
            if self.dq_checks.get("remove_alpha_from_numeric"): self.remove_alpha_from_numeric()
            if self.dq_checks.get("row_duplicate_check"): self.row_duplicate_check()
            if self.dq_checks.get("null_notnull_check"): self.null_notnull_check()
            if self.dq_checks.get("decimal_column_normalization"): self.decimal_column_normalization()
            # Add further DQ checks as needed

            # Always enforce final types at the end
            self.cast_columns_to_types()

            self.audit_info['status'] = "Passed"
            self.audit_info['reason'] = "All enabled DQ checks passed"
            return self.df
        except Exception as e:
            self.reject_file_and_audit(str(e))
            return None

    def get_audit_info(self):
        return self.audit_info
