def _quote(col):
    if col.startswith('"') and col.endswith('"'):
        return col
    return f'"{col}"'

def _coalesce_trim(col_alias, col_name, default="''"):
    return f"COALESCE(TRIM({col_alias}.{_quote(col_name)}), {default})"

class SCD2SQLGenerator:
    SCD2_AUDIT_DEFAULTS = [
        ("record_hash", lambda: 'stg."record_hash"'),
        ("start_date", lambda: "GETDATE()"),
        ("end_date", lambda: "NULL"),
        ("is_current_fg", lambda: "'Y'"),
        ("batch_run_dt", lambda: "GETDATE()"),
        ("created_by", lambda: "'glue'"),
        ("create_dt", lambda: "GETDATE()"),
        ("last_update_dt", lambda: "GETDATE()"),
        ("source_nm", lambda: "'sc360-dev-nrt-dataload-processor'"),
        ("source_file_name", lambda: 'stg."source_file_name"')
    ]

    def __init__(
        self,
        target_table: str,
        staging_table: str,
        primary_key_columns: list,
        hash_columns: list,
        surrogate_key: str = "r_revenue_egi_nrt_id"
    ):
        self.target_table = target_table
        self.staging_table = staging_table
        self.primary_key_columns = primary_key_columns
        self.hash_columns = hash_columns
        self.surrogate_key = surrogate_key

        # Compose final columns: PKs + hash columns + audit columns, deduplicated, surrogate key removed if present
        self.final_columns = []
        for col in (primary_key_columns + hash_columns):
            if col not in self.final_columns:
                self.final_columns.append(col)
        for col, _ in self.SCD2_AUDIT_DEFAULTS:
            if col not in self.final_columns:
                self.final_columns.append(col)
        if self.surrogate_key and self.surrogate_key in self.final_columns:
            self.final_columns.remove(self.surrogate_key)

    def _pk_join_condition(self, tgt_alias="tgt", stg_alias="stg"):
        # Use COALESCE(TRIM(...),'') for all keys (customize default for date columns if needed)
        return "\n    AND ".join([
            f"{_coalesce_trim(tgt_alias, col)} = {_coalesce_trim(stg_alias, col)}"
            for col in self.primary_key_columns
        ])

    def generate_insert_sql(self) -> str:
        insert_cols_str = ",\n    ".join([_quote(col) for col in self.final_columns])
        select_exprs = []
        for col in self.final_columns:
            found = False
            for audit_col, default_func in self.SCD2_AUDIT_DEFAULTS:
                if audit_col == col:
                    select_exprs.append(default_func())
                    found = True
                    break
            if not found:
                select_exprs.append(f'stg.{_quote(col)}')
        select_cols_str = ",\n    ".join(select_exprs)

        join_conditions = self._pk_join_condition("tgt", "stg") + "\n    AND tgt.\"is_current_fg\" = 'Y'"

        where_clause = (
            f'tgt.{_quote(self.surrogate_key)} IS NULL OR tgt."record_hash" <> stg."record_hash"'
        )

        sql = f"""INSERT INTO {self.target_table} (
                    {insert_cols_str}
                )
                SELECT
                    {select_cols_str}
                FROM {self.staging_table} stg
                LEFT JOIN {self.target_table} tgt
                    ON {join_conditions}
                WHERE {where_clause};
                """
        return sql

    def generate_update_sql(self) -> str:
        key_conditions = self._pk_join_condition()
        sql = f"""UPDATE {self.target_table} tgt
                SET
                    "end_date" = GETDATE(),
                    "is_current_fg" = 'N'
                FROM {self.staging_table} stg
                WHERE
                    {key_conditions}
                    AND tgt."is_current_fg" = 'Y'
                    AND tgt."record_hash" <> stg."record_hash";
                """
        return sql
