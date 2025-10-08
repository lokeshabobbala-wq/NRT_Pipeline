from pyspark.sql import DataFrame, Window, functions as F

def deduplicate_egi_2_0_inc(clean_df: DataFrame) -> DataFrame:
    """
    Applies priority logic and deduplication for EGI_2_0_INC file type.
    Returns the cleaned DataFrame.
    """
    # Add CP_Type_Priority
    clean_df = clean_df.withColumn(
        "CP_Type_Priority",
        F.when(F.col("cp_backlog_sni_revenue") == "REV", 1)
         .when(F.col("cp_backlog_sni_revenue") == "BACKLOG", 2)
         .otherwise(3)
    )

    # Add Quantity_Priority
    clean_df = clean_df.withColumn(
        "Quantity_Priority",
        F.when(
            (F.col("sum_of_sales_order_base_quantity") != 0) & (F.col("sum_of_cp_gross_revenue_(usd)") != 0), 1
        ).when(
            (F.col("sum_of_sales_order_base_quantity") == 0) & (F.col("sum_of_cp_gross_revenue_(usd)") != 0), 2
        ).otherwise(3)
    )

    # Partition columns for deduplication
    partition_cols = [
        "order_type",
        "order_create_calendar_date",
        "sales_order_identifier",
        "sales_order_line_item_number",
        "source_type",
        "shipment_date",
        "delivery_identifier",
        "region"
    ]
    window_spec = Window.partitionBy(*partition_cols).orderBy(
        F.col("CP_Type_Priority").asc(),
        F.col("Quantity_Priority").asc()
    )

    # Deduplicate
    clean_df = clean_df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")

    # Drop helper columns
    clean_df = clean_df.drop("CP_Type_Priority", "Quantity_Priority")

    return clean_df
