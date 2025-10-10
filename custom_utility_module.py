from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.functions import lit, col, concat_ws, udf
from pyspark.sql.types import StringType

def fiscal_quarter_from_date(date_val):
    if not date_val:
        return None
    if isinstance(date_val, str):
        if len(date_val) < 7:
            return None
        y, m = date_val[:4], int(date_val[5:7])
    elif hasattr(date_val, "year") and hasattr(date_val, "month"):
        y, m = str(date_val.year), date_val.month
    else:
        return None
    q = ((int(m) - 1) // 3) + 1
    return f"{y}-Q{q}"

def deduplicate_egi_2_0_inc(df: DataFrame, **kwargs) -> DataFrame:
    df = df.withColumn(
        "CP_Type_Priority",
        F.when(F.col("cp_backlog_sni_revenue") == "REV", 1)
         .when(F.col("cp_backlog_sni_revenue") == "BACKLOG", 2)
         .otherwise(3)
    )
    df = df.withColumn(
        "Quantity_Priority",
        F.when(
            (F.col("sum_of_sales_order_base_quantity") != 0) & (F.col("sum_of_cp_gross_revenue_(usd)") != 0), 1
        ).when(
            (F.col("sum_of_sales_order_base_quantity") == 0) & (F.col("sum_of_cp_gross_revenue_(usd)") != 0), 2
        ).otherwise(3)
    )
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
    df = df.withColumn("row_num", F.row_number().over(window_spec)).filter(F.col("row_num") == 1).drop("row_num")
    df = df.drop("CP_Type_Priority", "Quantity_Priority")
    return df

def add_egi_2_0_inc_mapped_columns(df: DataFrame, **kwargs) -> DataFrame:
    fiscal_quarter_udf = udf(fiscal_quarter_from_date, StringType())
    return (
        df
        .withColumn("sap_order_key", concat_ws("-", col("sales_order_identifier"), col("sales_order_line_item_number")))
        .withColumn("region_id", lit(500))
        .withColumn("fiscal_quarter", fiscal_quarter_udf(col("order_create_calendar_date")))
        .withColumn("profit_center_hierarchy_level_3_description", lit("WW"))
    )
    
def add_empty_bs_prod_columns(df: DataFrame) -> DataFrame:
    """
    Adds empty string columns 'bs_prod' and 'rptg_bs_prod' for WW_Cx_Fulfllmnt_Bklg_data file type.
    """
    return (
        df
        .withColumn("bs_prod", lit(""))
        .withColumn("rptg_bs_prod", lit(""))
    )
    
def process_egi_2_0_inc(df: DataFrame, **kwargs) -> DataFrame:
    # Chain deduplication and column mapping
    df = deduplicate_egi_2_0_inc(df)
    df = add_egi_2_0_inc_mapped_columns(df)
    return df

LOGICAL_FILE_PROCESSORS = {
    "EGI_2_0_INC": process_egi_2_0_inc,
    #"WW_Cx_Fulfllmnt_Bklg_data": add_empty_bs_prod_columns
    # Add more file-specific functions here as needed
}

def process_by_logical_file_name(df, logical_file_name):
    func = LOGICAL_FILE_PROCESSORS.get(logical_file_name)
    if func:
        result_df = func(df)
    else:
        result_df = df
    # Cache only if count is expensive and you'll reuse result_df
    result_df = result_df.cache()
    count = result_df.count()
    # Optionally, unpersist after count if result_df won't be used again
    # result_df.unpersist()
    return result_df, count
       