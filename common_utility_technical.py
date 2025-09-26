from awsglue.utils import getResolvedOptions
from xlwt import Workbook
from pyspark.sql.functions import col, count, collect_list, size, lit, unix_timestamp, from_unixtime, when, concat, udf
from pyspark.sql import functions as F
from pyspark.sql import Window, DataFrame, SparkSession
from pyspark.sql.types import StructType, IntegerType, StringType, StructField, DoubleType
import pandas as pd
import numpy as np
from functools import reduce
import datetime as dt
from datetime import date
import time
import boto3
import json
import os
import traceback
import sys
import gzip
import io
import logging
import getpass
import pyxlsb
from urllib.parse import urlparse
import datetime
import openpyxl
import ast
import csv
from common_utility_funtional import Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class common_lib():
    """ Main class to support wrapper library"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.setLevel(logging.ERROR)

        # Create the Spark session
        self.spark = SparkSession.builder.appName("DQ").config('spark.sql.codegen.wholeStage', 'false').getOrCreate()
        # Loggin initialization
        self.dq_logging = Logging(self.spark, "DQ", self.log_file_name)

    # Method to read the config file details
    def read_config_file_details(self):
        try:
            self.columns_to_select = self.config_file_content[self.file_key][self.sheet_name]['columns_to_select'] if 'columns_to_select' in self.config_file_content[self.file_key][
                self.sheet_name] else 0
            self.required_column_list = self.config_file_content[self.file_key][self.sheet_name]['required_column_list']
            self.input_file_extension = self.config_file_content[self.file_key][self.sheet_name]['input_file_extension']
            self.header_row_number_to_select = self.config_file_content[self.file_key][self.sheet_name]['header_row_number_to_select']
            self.int_check_columns = self.config_file_content[self.file_key][self.sheet_name]['int_columns']
            self.string_check_columns = self.config_file_content[self.file_key][self.sheet_name]['string_columns']
            self.null_check_columns = self.config_file_content[self.file_key][self.sheet_name]['notnull_columns']
            self.primary_columns = self.config_file_content[self.file_key][self.sheet_name]['primary_columns']
            self.date_columns = self.config_file_content[self.file_key][self.sheet_name]['date_columns']
            self.dq_logging.info(f"[DEBUG] Raw date_columns: {self.date_columns}, type: {type(self.date_columns)}")
            if isinstance(self.date_columns, str):
                self.date_columns = ast.literal_eval(self.date_columns)
                self.dq_logging.info(f"[DEBUG] Parsed date_columns: {self.date_columns}")
            self.convert_to_date = self.config_file_content[self.file_key][self.sheet_name]['convert_to_date'] if 'convert_to_date' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.timestamp_columns = self.config_file_content[self.file_key][self.sheet_name]['timestamp_columns']
            self.column_cleanup_columns = self.config_file_content[self.file_key][self.sheet_name]['column_cleanup_columns']
            self.Integer_cleanup_columns = self.config_file_content[self.file_key][self.sheet_name]['Integer_cleanup_columns']
            self.edi_cleanup_columns = self.config_file_content[self.file_key][self.sheet_name]['edi_cleanup_columns']
            # self.cast_to_int_columns = self.config_file_content[self.file_key][self.sheet_name]['cast_to_int_columns']
            self.replace_zero_null = self.config_file_content[self.file_key][self.sheet_name]['replace_zero_null']
            self.cast_to_double_columns = self.config_file_content[self.file_key][self.sheet_name]['cast_to_double_columns'] if 'cast_to_double_columns' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.cast_to_int_columns = self.config_file_content[self.file_key][self.sheet_name]['cast_to_int_columns'] if 'cast_to_int_columns' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.replace_zero_null = self.config_file_content[self.file_key][self.sheet_name]['replace_zero_null'] if 'replace_zero_null' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.drop_null_data_columns = self.config_file_content[self.file_key][self.sheet_name]['drop_null_data_columns'] if 'drop_null_data_columns' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.remove_trailer_rows = self.config_file_content[self.file_key][self.sheet_name]['remove_trailer_rows'] if 'remove_trailer_rows' in self.config_file_content[self.file_key][self.sheet_name] else 0
            self.remove_columns = self.config_file_content[self.file_key][self.sheet_name]['remove_columns'] if 'remove_columns' in self.config_file_content[self.file_key][self.sheet_name] else []
            self.read_sheet_by_sheet_index = self.config_file_content[self.file_key][self.sheet_name]['read_sheet_by_sheet_index'] if 'read_sheet_by_sheet_index' in self.config_file_content[self.file_key][self.sheet_name] else 0
            self.excel_engine_openpyxl = self.config_file_content[self.file_key][self.sheet_name]['excel_engine_openpyxl'] if 'excel_engine_openpyxl' in self.config_file_content[self.file_key][self.sheet_name] else 0
            self.decimal_cal_columns = self.config_file_content[self.file_key][self.sheet_name]['decimal_cal_columns'] if 'decimal_cal_columns' in self.config_file_content[self.file_key][self.sheet_name] else {}
            self.allow_duplicates = self.config_file_content[self.file_key][self.sheet_name]['allow_duplicates'] if 'allow_duplicates' in self.config_file_content[self.file_key][self.sheet_name] else -1

            if self.file_name.split('.')[-1].lower() == 'csv':
                self.delimiter = self.config_file_content[self.file_key][self.sheet_name]['delimiter']

            self.dq_logging.info(f'{self.file_name}, Config file read successfully')
        except Exception as e:
            self.exception_error(e,"read_config_file_details", "")

    # Method to convert XLSX, XLSM, XLSB & XLS file to relational
    def master_data_conversion(self):
        """
        Performs master data conversion by handling different file formats and regions, 
        applying necessary transformations, and returning a Spark DataFrame.

        Args:
            self (object): Instance of the class containing attributes like input file path, 
                        region name, S3 client, and required column lists.

        Returns:
            pyspark.sql.DataFrame: Converted and processed DataFrame ready for further use.
        """
        try:
            # Common initialization for all regions
            self.input_file_bucket = self.input_file_path.split('/', 3)[2]
            self.input_file_key = self.input_file_path.split('/', 3)[3]
            self.file_extension = self.input_file_path.split('/', 3)[3]
            self.excel_file_s3_object = self.s3_client.get_object(Bucket=self.input_file_bucket, Key=self.input_file_key)
            print("self.input_file_bucket", self.input_file_bucket)

            # ========== Start of Combined Conditional Logic ==========

            if self.region_name in ["AMS", "APJ", "EMEA"]:
                valid_keys = ['Build_Plan_Wired', 'Inventory_Target', 'Build_Plan_Wireless', 'HPN_Product_Homologation_Matrix']
            elif self.region_name == "WW":
                valid_keys = ['Inventory_Target', 'HPN_Product_Homologation_Matrix']
            # Handle Pivot Files (All Regions)
            if any(key in self.file_key for key in valid_keys):
                pivot_output_df = self.handle_pivoting()
                self.dq_logging.info(f'{self.file_name}, File format conversion completed.')
                return pivot_output_df

            # AMS-specific Fusion Case
            elif self.file_key in ["AMS_Fusion_OM_HOLDS_CM_SKU"]:
                self.sheet_name = "AMS_Fusion_OM_HOLDS_CM_SKU"
                print("Working on AMS_Fusion Transformation")
                excel_file_pd_df = pd.read_excel(
                    self.excel_file_s3_object['Body'].read(), 
                    names=self.required_column_list,
                    sheet_name=self.sheet_name,
                    header=self.header_row_number_to_select)
                return self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))

            # OSS Handling (All Regions)
            elif ('APJ_OSS' in self.file_key 
                        or 'EMEA_OSS' in self.file_key 
                        or ('AMS_OSS' == self.file_key and self.region_name == 'AMS') 
                        or ('AMS_OSS' in self.file_key and self.region_name != 'AMS')
                    ):
                oss_df = self.oss_data_validations()
                self.dq_logging.info(f'{self.file_name}, OSS conversion completed.')
                return oss_df

            # .xlsb Handling (All Regions)
            elif self.file_name.split('.')[-1].lower() == 'xlsb':
                if self.file_key in ['Reservation_list']:
                    excel_file_pd_df_file = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=self.sheet_name,
                        engine='pyxlsb',
                        header=self.header_row_number_to_select,
                        keep_default_na=False)
                    excel_file_pd_df = excel_file_pd_df_file.loc[:, ~excel_file_pd_df_file.columns.str.contains('^Unnamed')]
                    excel_file_pd_df.columns = self.required_column_list
                else:
                    excel_file_pd_df = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=self.sheet_name,
                        engine='pyxlsb',
                        header=self.header_row_number_to_select,
                        names=self.required_column_list,
                        keep_default_na=False
                    )

                # Date Conversion Logic (All Regions)
                for item in [self.date_columns, self.timestamp_columns]:
                    for each_column in item:
                        for cname, date_format in each_column.items():
                            excel_file_pd_df[cname] = pd.to_numeric(excel_file_pd_df[cname], errors='ignore')
                            excel_file_pd_df[cname] = pd.TimedeltaIndex(excel_file_pd_df[cname], unit='d') + dt.datetime(1899, 12, 30)
                
                excel_file_pd_df = self.remove_rows(excel_file_pd_df)
                return self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))

            # .xls Handling (Combined from All Regions)
            elif (self.file_name.split('.')[-1].lower() == 'xls' and 
                any(key in self.file_key for key in ["LSR-HPD206-604", "CN_inv_data", "JP_inv_data", "AMS_OSS", "APJ_OSS", "EMEA_OSS", "AMS_Fusion_OM_HOLDS_CM_SKU"])):
                
                file_split = self.input_file_key.split('/')
                file_name = file_split[-1]
                self.s3_client.download_file(self.input_file_bucket, self.input_file_key, file_name)
                
                # APJ-specific encoding
                encoding = "ISO-8859-1" if 'APJ' == self.region.upper() else "utf-8"
                with io.open(file_name, "r", encoding=encoding) as source_file:
                    data = source_file.readlines()

                # Excel Conversion Logic
                xldoc = Workbook()
                sheet = xldoc.add_sheet(self.sheet_name, cell_overwrite_ok=True)
                for i, row in enumerate(data):
                    for j, val in enumerate(row.replace('\n', '').split('\t')):
                        sheet.write(i, j, val)
                xldoc.save(file_name)

                excel_file_pd_df = pd.read_excel(
                    file_name,
                    sheet_name=self.sheet_name,
                    header=self.header_row_number_to_select,
                    names=self.required_column_list,
                    dtype=str
                )
                excel_file_pd_df = self.remove_rows(excel_file_pd_df)
                return self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))

            # ========== Special Cases from All Regions ==========
            else:
                # Sheet Index Handling
                if self.read_sheet_by_sheet_index != 0:
                    sheet_index = int(self.sheet_name)
                    excel_file_pd_df = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=sheet_index,
                        header=self.header_row_number_to_select,
                        names=self.required_column_list,
                        dtype=str,
                        engine='openpyxl'
                    )

                # DeliveryReport/Openpyxl Handling
                elif self.file_key in ['DeliveryReport'] or self.excel_engine_openpyxl != 0:
                    excel_file_pd_df = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=self.sheet_name,
                        header=self.header_row_number_to_select,
                        names=self.required_column_list,
                        engine='openpyxl'
                    )

                # Part Master/SFDC Handling
                elif 'APJ_SFDC' in self.file_key or 'Part_Master_D-All_SKUs' in self.file_key:
                    sheet_param = int(self.sheet_name) if 'Part_Master_D-All_SKUs' in self.file_key else self.sheet_name
                    excel_file_pd_df = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=sheet_param,
                        header=self.header_row_number_to_select,
                        names=self.required_column_list,
                        dtype=str,
                        keep_default_na=False,
                        engine='openpyxl'
                    )

                # APJ-specific Column Selection
                elif self.columns_to_select != 0 and 'APJ' == self.region.upper():
                    excel_file_pd_df_file = pd.read_excel(
                        self.excel_file_s3_object['Body'].read(),
                        sheet_name=self.sheet_name,
                        header=self.header_row_number_to_select,
                        dtype=str,
                        engine='openpyxl'
                    )
                    excel_file_pd_df = excel_file_pd_df_file.iloc[:, 0:self.columns_to_select]
                    excel_file_pd_df.columns = self.required_column_list

                # APJ_Open_SFDC Handling (Missing in Previous Version)
                else:
                    header=self.header_row_number_to_select
                    sheet_name=self.sheet_name
                    names=self.required_column_list
                    
                    if self.file_key == 'APJ_Open_SFDC':
                        excel_file_pd_df = pd.read_excel(
                            self.excel_file_s3_object['Body'].read(),
                            sheet_name=self.sheet_name,
                            header=self.header_row_number_to_select,
                            names=self.required_column_list,
                            dtype=str,
                            keep_default_na=False,
                            engine='openpyxl'
                        )

                    # EMEA Safe Order Handling
                    elif self.file_key in ['EMEA_Safe_Order_Cutoff_Date_Monthly', 'EMEA_Safe_Order_Cutoff_Date_Quarterly', 'Supplier_Sanmina_855']:
                        excel_file_pd_df = pd.read_excel(
                            self.excel_file_s3_object['Body'].read(),
                            sheet_name=None,
                            header=self.header_row_number_to_select,
                            dtype=str,
                            engine='openpyxl'
                        )
                        for sheet in excel_file_pd_df.keys():
                            if self.sheet_name in sheet:
                                self.sheet_name = sheet
                                break
                        excel_file_pd_df = excel_file_pd_df[self.sheet_name]
                        excel_file_pd_df.columns = self.required_column_list

                    # Unnamed Column Handling (Multiple Regions)
                    elif any(key in self.file_key for key in ['Supplier_Sanmina', 'JAPAN_stockable', 'APJ_Deal_backlogreport', 'Deal_backlogreport',
                                                            'Y1_MULTI_SEL_RPT', 'AMS_OSSreport-Aruba']):
                        excel_file_pd_df_file = pd.read_excel(
                            self.excel_file_s3_object['Body'].read(),
                            sheet_name=self.sheet_name,
                            header=self.header_row_number_to_select,
                            dtype=str,
                            engine='openpyxl'
                        )
                        excel_file_pd_df = excel_file_pd_df_file.loc[:, ~excel_file_pd_df_file.columns.str.contains('^Unnamed')]
                        excel_file_pd_df.columns = self.required_column_list

                    # Default Case
                    else:
                        excel_file_pd_df = pd.read_excel(
                            self.excel_file_s3_object['Body'].read(),
                            sheet_name=self.sheet_name,
                            header=self.header_row_number_to_select,
                            names=self.required_column_list,
                            dtype=str,
                            engine='openpyxl'
                        )

                    # Common Post-Processing
                    excel_file_pd_df = self.remove_null_rows(excel_file_pd_df)
                    excel_file_pd_df = self.remove_rows(excel_file_pd_df)

                # AMS-specific Date Handling
                if "AMS_QS_DAILY_FILE" in self.file_key:
                    for cname in self.convert_to_date:
                        excel_file_pd_df[cname] = excel_file_pd_df[cname].replace({'-': None, '2958465': None})

                # Date Conversion for All
                for cname in self.convert_to_date:
                    excel_file_pd_df[cname] = pd.to_numeric(excel_file_pd_df[cname], errors='ignore')
                    excel_file_pd_df[cname] = pd.TimedeltaIndex(excel_file_pd_df[cname], unit='d') + dt.datetime(1899, 12, 30)

                return self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))

        # ========== Combined Error Handling ==========
        except Exception as e:
            self.exception_error(e, "xldoc", "DataValidation")
            # Directly send the SNS message
            app_name = 'sc360'
            rejected_file_name = self.file_key
            self.process_name = 'DataValidation - MasterData Conversion'
            rejection_file_path = 'NA'
            self.dq_logging.send_sns_message(self.pipeline_run_id, app_name, self.exec_env, self.region_name, rejected_file_name, self.process_name, str(e), rejection_file_path, self.job_name, self.loggroupname, self.job_run_id, self.sns_arn)
    
    def get_stack_expression(self,num_months):
        """
        Generates a stack expression for unpivoting month columns.
        
        Args:
            num_months (int): Number of month columns to be stacked.

        Returns:
            str: SQL stack expression for Spark selectExpr().
        """
        return f"stack({num_months}, " + ', '.join([f"'Month{i}', Month{i}" for i in range(1, num_months + 1)]) + ") as (BUILD_PLAN_REF_MTH_DATE, BUILD_PLAN_QT)"
    
    # Method to handle the pivoting explicitly
    def handle_pivoting(self):
        """
        Handles the pivoting (unpivoting) of various types of input files by transforming 
        wide-format data into long-format. Converts Pandas DataFrame into Spark DataFrame 
        and applies necessary column mappings.

        Supported file types:
        - Build Plan Wired
        - Build Plan Wireless
        - Inventory Target
        - HPN Product Homologation Matrix

        Args:
            self (object): Instance of the class containing file metadata, Spark session, 
                        region information, and required transformations.

        Returns:
            pyspark.sql.DataFrame: Transformed DataFrame with pivoted data.

        Raises:
            Exception: Logs any errors encountered during the pivoting process.
        """
        try:
            excel_file_pd_df = pd.read_excel(self.excel_file_s3_object['Body'].read(), sheet_name=self.sheet_name,
                                             header=self.header_row_number_to_select, dtype=str,engine = 'openpyxl')
            excel_file_pd_df = self.remove_rows(excel_file_pd_df)
            if 'Build_Plan_Wired' in self.file_key:
                num_months = 18 if self.region == "WW" else 12
                original_month_columns = list(excel_file_pd_df.columns)[3:]

                # Define Spark schema for Build Plan Wired file
                schema = StructType([
                    StructField("PRODUCT_ID", StringType(), True),
                    StructField("SUPPLIER", StringType(), True),
                    StructField("KEY_FIGURE", StringType(), True)
                ] + [StructField(f"Month{i}", StringType(), True) for i in range(1, num_months + 1)])

                # Convert Pandas DataFrame to Spark DataFrame
                bp_df = self.spark.createDataFrame(excel_file_pd_df.replace('nan', ''), schema=schema)

                # Apply unpivot transformation using stack function
                unPivot_DF = bp_df.selectExpr("PRODUCT_ID", "SUPPLIER", "KEY_FIGURE", self.get_stack_expression(num_months))
                # Replace generic month column names with actual month values
                mapping_dict = {f"Month{i+1}": original_month_columns[i] for i in range(num_months)}
                final_bp_df = unPivot_DF.replace(mapping_dict)

                # Handle missing values in BUILD_PLAN_QT column
                pivot_output_df = final_bp_df.withColumn('BUILD_PLAN_QT', F.when(col('BUILD_PLAN_QT').isNull(), '').otherwise(col('BUILD_PLAN_QT')))
            elif 'Build_Plan_Wireless' in self.file_key:
                num_months = 12
                original_month_columns = list(excel_file_pd_df.columns)[2:]

                # Define Spark schema for Build Plan Wireless file
                schema = StructType([
                    StructField("Operation_Family", StringType(), True),
                    StructField("KEY_FIGURE", StringType(), True)
                ] + [StructField(f"Month{i}", StringType(), True) for i in range(1, num_months + 1)])

                # Convert Pandas DataFrame to Spark DataFrame
                bp_df = self.spark.createDataFrame(excel_file_pd_df.replace('nan', ''), schema=schema)

                # Apply unpivot transformation using stack function
                unPivot_DF = bp_df.selectExpr("Operation_Family", "KEY_FIGURE", self.get_stack_expression(num_months))

                # Replace generic month column names with actual month values
                mapping_dict = {f"Month{i+1}": original_month_columns[i] for i in range(num_months)}
                final_bp_df = unPivot_DF.replace(mapping_dict)

                # Handle missing values in BUILD_PLAN_QT column
                pivot_output_df = final_bp_df.withColumn('BUILD_PLAN_QT', F.when(col('BUILD_PLAN_QT').isNull(), '').otherwise(col('BUILD_PLAN_QT')))
            elif 'Inventory_Target' in self.file_key:
                # Convert wide-format data into long-format using Pandas melt function
                pivot_output_df = excel_file_pd_df.melt(
                    id_vars=excel_file_pd_df.columns[0:3], 
                    var_name='REF_MONTH', 
                    value_name='SAFETY_TARGET'
                )

                # Rename columns based on predefined schema
                pivot_output_df.columns = self.required_column_list

                # Handle missing values
                pivot_output_df['SAFETY_TARGET'] = pivot_output_df['SAFETY_TARGET'].fillna('')

                # Convert Pandas DataFrame to Spark DataFrame
                pivot_output_df = self.spark.createDataFrame(pivot_output_df.astype(str).replace('nan', '').replace('NaT', ''))

            elif 'HPN_Product_Homologation_Matrix' in self.file_key:
                # Define identifier columns
                id_cols = list(excel_file_pd_df.columns[0:3]) + list(excel_file_pd_df.columns[-5:])

                # Convert wide-format data into long-format using Pandas melt function
                pivot_output_df = excel_file_pd_df.melt(id_vars=id_cols, var_name='COUNTRY', value_name='STATUS')

                # Ensure proper column order
                col_order = list(pivot_output_df.columns[0:3]) + list(pivot_output_df.columns[-2:]) + list(pivot_output_df.columns[3:8])
                pivot_output_df = pivot_output_df[col_order]

                # Rename columns to match the required schema
                pivot_output_df.columns = self.required_column_list

                # Handle missing values
                pivot_output_df['STATUS'] = pivot_output_df['STATUS'].fillna('')

                # Convert Pandas DataFrame to Spark DataFrame
                pivot_output_df = self.spark.createDataFrame(pivot_output_df.astype(str).replace('nan', '').replace('NaT', ''))

            return pivot_output_df

        except Exception as e:
            self.exception_error(e,"handle_pivoting", "")

    def oss_data_validations(self):
        try:
            self.rejected_df_list_oss = []
            if self.file_name.split('.')[-1].lower() in ('xlsb', 'XLSB'):
                self.dq_logging.info(f'{self.header_row_number_to_select}{self.required_column_list}{self.sheet_name}')

                excel_file_pd_df_file = pd.read_excel(self.excel_file_s3_object['Body'].read(), sheet_name=self.sheet_name,
                                                 engine='pyxlsb', header=self.header_row_number_to_select)

                excel_file_pd_df = excel_file_pd_df_file.loc[:, ~excel_file_pd_df_file.columns.str.contains('^Unnamed')]

                excel_file_pd_df.columns = self.required_column_list

                # convert date columns
                for item in self.date_columns, self.timestamp_columns:
                    for each_column in item:
                        for cname, date_format in each_column.items():
                            excel_file_pd_df[cname] = pd.TimedeltaIndex(excel_file_pd_df[cname], unit='d') + dt.datetime(1899, 12, 30)
            else:
                excel_file_pd_df_file = pd.read_excel(self.excel_file_s3_object['Body'].read(), sheet_name=self.sheet_name,
                                                 header=self.header_row_number_to_select, dtype = object)
                excel_file_pd_df = excel_file_pd_df_file.loc[:, ~excel_file_pd_df_file.columns.str.contains('^Unnamed')]
                excel_file_pd_df.columns = self.required_column_list
            self.final_output_file_name = self.file_key + "_" + time.strftime('%Y%m%d%H%M%S') + ".csv"
            actual_count = 0
            returns_count = 0
            raw_counts = 0
            if 'APJ_OSS' in self.file_key:

                oss_df = self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))
                oss_df_schema = oss_df.schema
                raw_counts = oss_df.count()
                actual_ords_df = oss_df.filter((((col('ORDER_REASON_CODE').isin('DOE', 'Watson', 'NGQ', 'NGQ - Partner')) & 
                                                 (col('ORDER_TYPE_SHORT') == 'ZOR')) | ((col('ORDER_TYPE_SHORT') == '313') & 
                                                (col('ORDER_TYPE_SHORT') == 'ZDR'))) | 
                                                ((col('ORDER_REASON_CODE').isin('EOP', 'ELF', 'EPrime', 'E-Prime', 'eprime/Elite', 'ePrime')) & 
                                                 (col('ORDER_TYPE_SHORT') == 'ZOR')) | (col('ORDER_TYPE_SHORT') == 'ZOR') | (col('ORDER_TYPE_SHORT') == 'ZI2O'))

                actual_count = actual_ords_df.count()
                self.rejected_df_list_oss.append(actual_ords_df)
                print('oss_data_validations check for APJ_OSS Failed for actual_ords_df: ',self.rejected_df_list_oss)

                return_ords_df = oss_df.filter(((col('ORDER_TYPE_SHORT') == 'ZCR') & (
                    col('ORDER_REASON_CODE').isin('4RN', '01', '1', 'ZXA', 'OS', 'N02', 'ARS', 'ZO2', '1CA', '102'))) \
                                               | ((col('ORDER_REASON_CODE').isin('03', '3', '313')) & (
                            col('ORDER_TYPE_SHORT') == 'ZDR')) \
                                               | ((col('ORDER_TYPE_SHORT') == 'ZRE') & (
                    col('ORDER_REASON_CODE').isin('4RR', '4CQ', '306', 'ZXA', '2', 'I14', 'I15', 'I21', 'A08', '308',
                                                  'N11', 'A13', '305', 'N02', '1CD', '301', 'N01', '302', 'N09'))) \
                                               | ((col('ORDER_TYPE_SHORT') == 'ZTP') & (
                    col('ORDER_REASON_CODE').isin('N03', 'I55', 'N15', '00', 'A15', '105', '4CS'))) \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZDR', 'ZCR')) & (
                    col('ORDER_REASON_CODE').isin('1CC'))) \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZRE', 'ZCR')) & (
                    col('ORDER_REASON_CODE').isin('1CE', '305', '307', 'A13'))) \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZDR', 'ZCR')) & (
                    col('ORDER_REASON_CODE').isin('4CT', '1CP', 'N02'))) \
                                               # | (col('ORDER_TYPE_SHORT') == 'ZOR') \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZRE', 'ZCR')) & (
                    col('ORDER_REASON_CODE').isin('ACR', 'A06', '1CF'))) \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZRE', 'ZTP')) & (
                    col('ORDER_REASON_CODE').isin('4CT', '102', '4WS'))) \
                                               | ((col('ORDER_TYPE_SHORT').isin('ZMRE', 'ZTP')) & (
                    col('ORDER_REASON_CODE').isin('4RR', '304', '4RN'))))

                returns_count = return_ords_df.count()
                self.rejected_df_list_oss.append(return_ords_df)
                print('oss_data_validations check for APJ_OSS Failed for return_ords_df: ',self.rejected_df_list_oss)
                
            elif  self.file_key == 'AMS_OSS':
                oss_df = self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))
                oss_df_schema = oss_df.schema
                raw_counts = oss_df.count()
                actual_ords_df = oss_df.filter(((col('ORDER_REASON_CODE').isin('EDI', 'EDI (OIS)', 'NGQ',
                                                                               'NGQ - Partner')) & (
                                                            col('ORDER_TYPE_SHORT') == 'ZOR')) | (
                                                       (col('ORDER_REASON_CODE').isin('DOE', 'Watson', 'AOE', 'DUS',
                                                                                      'C2K', 'E-Claims', 'eClaims',
                                                                                      'OMH', 'ePrime/Elite',
                                                                                      'EPRIME')) & (
                                                           col('ORDER_TYPE_SHORT').isin('ZOR', 'ZDR'))) | (
                                                           col('ORDER_TYPE_SHORT') == 'ZOR') | (
                                                       col('ORDER_TYPE_SHORT') == 'ZDR') | (
                                                           col('ORDER_TYPE_SHORT') == 'ZNC') | (
                                                           col('ORDER_TYPE_SHORT') == 'ZI2O'))
                actual_count = actual_ords_df.count()
                self.rejected_df_list_oss.append(actual_ords_df)
                print('oss_data_validations check for AMS_OSS Failed for actual_ords_df: ',self.rejected_df_list_oss)
                
                return_ords_df = oss_df.filter(((col('ORDER_REASON_CODE').isin('B13', 'B17', 'B27', 'B31', 'B37', '100', 'B14', 'D06')) & 
                                                (col('ORDER_TYPE_SHORT') == 'ZCR')) | 
                                                ((col('ORDER_REASON_CODE').isin('B18', 'B43', 'HS', 'ZEF', '301', '307',
                                                                                'B05', 'B07', 'B10', 'B12', 'E01', 'Z9X',
                                                                                '102', '103', '303', 'B09', '305', '306',
                                                                                'E55', '001', '308', 'B30', '304', '310',
                                                                                '50', '311', 'B11', 'Z9V', 'B22', 'B03',
                                                                                'B24', 'B25', 'B06', 'I21', '302', 'I07',
                                                                                '101', 'PER', 'B21', '050')) & (col('ORDER_TYPE_SHORT').isin('ZRE','ZCR'))))

                returns_count = return_ords_df.count()
                self.rejected_df_list_oss.append(return_ords_df)
                print('oss_data_validations check for AMS_OSS Failed for return_ords_df: ',self.rejected_df_list_oss)

            elif 'EMEA_OSS' in self.file_key:
                oss_df = self.spark.createDataFrame(excel_file_pd_df.astype(str).replace('nan', '').replace('NaT', ''))
                oss_df_schema = oss_df.schema
                raw_counts = oss_df.count()
                actual_ords_df = oss_df.filter(((col('ORDER_ORIGIN').isin('SEDI', 'EDI1', 'EPR', 'OET')) & (
                            col('ORDER_TYPE_SHORT') == 'ZOR') & (col('ORDER_REASON_CODE') != '410')) | (
                                                       ((col('ORDER_ORIGIN').isin('AOE', 'CSN', 'DOE', 'WAT')) & (
                                                                   col('ORDER_TYPE_SHORT') == 'ZOR') & (
                                                                    col('ORDER_REASON_CODE') != '410')) | (
                                                                   (col('ORDER_REASON_CODE') == '106') & (
                                                                       col('ORDER_TYPE_SHORT') == 'ZDR'))) | (
                                                       col('ORDER_TYPE_SHORT') == 'ZOR') | (
                                                           col('ORDER_TYPE_SHORT') == 'ZI2O'))

                actual_count = actual_ords_df.count()
                self.rejected_df_list_oss.append(actual_ords_df)
                print('oss_data_validations check for EMEA_OSS Failed for actual_ords_df: ',self.rejected_df_list_oss)

                return_ords_df = oss_df.filter(((col('ORDER_REASON_CODE').isin('405', '406', '410', '411', '412', '420',
                                                                               '421', '415', '460', '414', 'C22', 'C23',
                                                                               'C24', 'C25', 'C26', 'C27', 'C28',
                                                                               'C29')) & (
                                                            col('ORDER_TYPE_SHORT') == 'ZRE')) | (
                                                       (col('ORDER_REASON_CODE').isin('C30', 'C31', 'C32', 'C37', 'C90',
                                                                                      '240')) & (
                                                                   col('ORDER_TYPE_SHORT') == 'ZCR')) | ((col(
                    'ORDER_REASON_CODE').isin('050', '50', '100', '105', '110', '120', '130', '140', 'C17', 'C21',
                                              '210',
                                              '260', '280', 'C54', 'C14', 'C45', '410', '405', '406', '411', '412',
                                              '420',
                                              '421', '415', '460', '414', 'C22', 'C23', 'C24', 'C25', 'C26', 'C27',
                                              'C28',
                                              'C29', 'FPN', 'C31')) & (col('ORDER_TYPE_SHORT').isin('ZCR', 'ZDR'))) | (
                                                       (col('ORDER_REASON_CODE').isin('C32', 'C37', 'C30', 'C90')) & (
                                                           col('ORDER_TYPE_SHORT') == 'ZDR')))

                returns_count = return_ords_df.count()
                self.rejected_df_list_oss.append(return_ords_df)
                print('oss_data_validations check for EMEA_OSS Failed for return_ords_df: ',self.rejected_df_list_oss)

                #total_ords = (actual_count + returns_count)
                if self.region == 'WW':
                    total_ords = (actual_count + returns_count)
                else:
                    total_ords  = raw_counts
                if (raw_counts == total_ords):
                    return oss_df

            else:

                # return oss_apj_rejected_df
                if len(self.rejected_df_list_oss) != 0:
                    reject_df = reduce(DataFrame.union, tuple(self.rejected_df_list_oss))

                reject_df = oss_df.exceptAll(reject_df)

                reject_df_pandas_df = reject_df.toPandas()

                rejected_file_name = "mismatching_reords_" + self.final_output_file_name
                reject_df_pandas_df.to_csv(rejected_file_name, index=False, encoding='utf-8', sep='|')
                reject_s3_bucket = self.reject_data_path.split('/', 3)[2]
                if len(self.config_file_content[self.file_key].keys()) > 1:
                    reject_s3_key = self.reject_data_path.split('/', 3)[3] + self.file_key + "_" + self.sheet_name_refined + "/" + rejected_file_name
                else:
                    reject_s3_key = self.reject_data_path.split('/', 3)[3] + self.file_key + "/" + rejected_file_name
                self.s3_client.put_object(Body=open(rejected_file_name, 'rb'), Bucket=reject_s3_bucket, Key=reject_s3_key)

                self.dq_logging.info("Rejected records are written to rejected path successfully.")
                
                self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                self.input_file_full_oss = oss_df.withColumn("reject_reason", lit('Records counts is not matching'))\
                                                .withColumn("rejected_column", lit('Full Record')).select(self.req_column_list + ['reject_reason', 'rejected_column'])

                if len(self.config_file_content[self.file_key].keys()) > 1:
                    reject_s3_key = self.reject_file_path.split('/', 3)[3] + self.file_key + "_" + self.sheet_name_refined + "/" + "rejected_" + self.final_output_file_name
                else:
                    reject_s3_key = self.reject_file_path.split('/', 3)[3] + self.file_key + "/" + "rejected_" + self.final_output_file_name

                reject_s3_bucket = self.reject_file_path.split('/', 3)[2]
                input_file_full_pandas = self.input_file_full_oss.toPandas()
                input_file_full_pandas.to_csv(self.final_output_file_name, index=False, encoding='utf-8', sep='|')
                print("Actual & Returns counts is not matching with Source file")

                self.s3_client.put_object(Body=open(self.final_output_file_name, 'rb'), Bucket=reject_s3_bucket,
                                          Key=reject_s3_key)
                self.dq_logging.info('OSS Rejected file' + self.final_output_file_name);

                self.dq_logging.info(f'{self.file_name}, OSS Rejected file are written to respective paths successfully., Success')

                empty_schema = StructType([])
                empty_df = self.spark.createDataFrame(self.spark.sparkContext.emptyRDD(), oss_df_schema)
                return empty_df

        except Exception as e:
            self.exception_error(e,"oss_data_validations", "")

    def remove_null_rows(self, excel_file_pd_df):
        """
        Removes rows from the DataFrame that contain null (NaN) values in specified columns.
        
        Args:
            self (object): Instance of the class containing attributes such as `drop_null_data_columns`.
            excel_file_pd_df (pd.DataFrame): The input Pandas DataFrame to be cleaned.

        Returns:
            pd.DataFrame: The DataFrame after removing rows with null values in specified columns.

        Exceptions:
            Catches any exceptions that occur and logs an error message.
        """
        try:
            # Log the list of columns being checked for null values
            self.dq_logging.info('self.remove_null_rows' + str(self.drop_null_data_columns))
            # Check if there are any columns specified for null-value removal
            if len(self.drop_null_data_columns) > 0:
                # Iterate over the columns in the drop_null_data_columns list
                for cname in self.drop_null_data_columns:
                    # Remove rows where the specified column has null (NaN) values
                    excel_file_pd_df = excel_file_pd_df.dropna(axis=0, subset=[str(cname)], inplace=False)
            else:
                # Log a message if no valid columns are specified for null removal
                self.dq_logging.info(f"Not a valid value for remove null rows : {self.remove_trailer_rows}")
            
            # Return the cleaned DataFrame
            return excel_file_pd_df
        except Exception as e:
            self.exception_error(e,"remove_null_rows", "")

    def remove_rows(self, excel_file_pd_df):
        try:
            self.dq_logging.info('self.remove_trailer_rows' + str(self.remove_trailer_rows))
            if self.remove_trailer_rows != 0:
                excel_file_pd_df = excel_file_pd_df.iloc[:int(self.remove_trailer_rows)]
            else:
                self.dq_logging.info(f"Not a valid value for remove trailer rows : {self.remove_trailer_rows}")
            # Remove rows where all elements are NaN or empty strings
            excel_file_pd_df = excel_file_pd_df.dropna(how='all')  # Remove full-NaN rows
            excel_file_pd_df = excel_file_pd_df.loc[~(excel_file_pd_df == '').all(axis=1)]  # Remove all-empty-string rows
            return excel_file_pd_df
        except Exception as e:
            self.exception_error(e,"remove_rows", "")

    # Method for DQ1 - Removing invisible characters in files
    def trimming_non_breakable_spaces(self):
        try:
            # Only apply to files not in the excluded list
            if self.file_key not in ['EGORAS_APJ','EGORAS_AMS','EGORAS_EMEA']:
                # Build a mapping of column transformations
                updated_columns = []
                for c_name in self.input_file_df.columns:
                    # Only update columns present in required_column_list if it exists
                    if hasattr(self, 'required_column_list') and c_name in self.required_column_list:
                        updated_columns.append(F.regexp_replace(F.col(c_name), "\u00A0", "").alias(c_name))
                    else:
                        updated_columns.append(F.col(c_name))
                # Apply all transformations at once
                self.input_file_df = self.input_file_df.select(*updated_columns)
                # Select required columns if needed
                if hasattr(self, 'required_column_list'):
                    self.input_file_df = self.input_file_df.select(*[F.col(f"`{col_name}`") for col_name in self.required_column_list])

            self.dq_logging.info(f'{self.file_name}, Trimming Non Breakable Spaces completed successfully')
        except Exception as e:
            self.exception_error(e, "trimming_non_breakable_spaces", "DQ1")

    # Method for DQ2 - Trimming Leading & Trailing Spaces
    def trimming_spaces(self):
        try:
            # Only apply for the correct file_key
            if self.file_key not in ['EGORAS_APJ','EGORAS_EMEA','EGORAS_AMS']:
                # Trim all columns in a single transformation
                trimmed_columns = [
                    F.trim(F.col(c_name)).alias(c_name) for c_name in self.input_file_df.columns
                ]
                # Apply the transformation and select required columns (if needed)
                self.input_file_df = self.input_file_df.select(*trimmed_columns)
                if hasattr(self, "required_column_list") and self.required_column_list:
                    self.input_file_df = self.input_file_df.select(
                        *[F.col(f"`{col_name}`") for col_name in self.required_column_list]
                    )
            self.dq_logging.info(f'{self.file_name}, Trimming Leading & Trailing Spaces completed successfully')
        except Exception as e:
            self.exception_error(e, "trimming_spaces", "DQ2")

    # Method for DQ3 - Remove Special characters in column values
    def replace_speacial_characters_columns(self):
        try:
            if hasattr(self, "column_cleanup_columns") and len(self.column_cleanup_columns) > 0:
                # Prepare column transformations for all columns in one go
                new_columns = []
                for c in self.input_file_df.columns:
                    if c in self.column_cleanup_columns:
                        if 'EMEA_S4_Returns' in self.file_key:
                            new_columns.append(F.regexp_replace(F.col(c), r"[\#]", "1").alias(c))
                        else:
                            new_columns.append(F.regexp_replace(F.col(c), r"[\$@#,]", "").alias(c))
                    else:
                        new_columns.append(F.col(c))
                # Apply all transformations at once
                self.input_file_df = self.input_file_df.select(*new_columns)
                # Select only required columns if specified
                if hasattr(self, "required_column_list") and self.required_column_list:
                    self.input_file_df = self.input_file_df.select(
                        *[F.col(f"`{col_name}`") for col_name in self.required_column_list]
                    )
            self.dq_logging.info(f'{self.file_name}, Special characters are removed from the columns in the columns completed successfully')
        except Exception as e:
            self.exception_error(e, "replace_speacial_characters_columns", "DQ3")

    def remove_empty_columns(self):
        try:
            self.dq_logging.info('self.remove_empty_columns' + str(self.remove_columns))
            required_column_list = self.required_column_list
            self.dq_logging.info('remove_empty_columns' + str(self.required_column_list))
            if len(self.remove_columns) > 0:
                for column_name in reversed(self.required_column_list):
                    if column_name in self.remove_columns:
                        self.dq_logging.info('remove_empty_columns' + str(column_name))
                        required_column_list.remove(column_name)
                self.input_file_df = self.input_file_df.select(required_column_list)
                self.required_column_list = required_column_list
        except Exception as e:
            self.exception_error(e,"remove_empty_columns", "DQ3")

    # Method for DQ4 - Removing Alphabets
    def removing_alphabets(self):
        try:
            if hasattr(self, "Integer_cleanup_columns") and len(self.Integer_cleanup_columns) > 0:
                # Prepare column transformations: extract numbers from specified columns, keep others unchanged
                new_columns = [
                    F.regexp_extract(F.col(col_name), r'[0-9]+', 0).alias(col_name) if col_name in self.Integer_cleanup_columns
                    else F.col(col_name)
                    for col_name in self.input_file_df.columns
                ]
                # Select only required columns if specified
                if hasattr(self, "required_column_list") and self.required_column_list:
                    self.input_file_df = self.input_file_df.select(*new_columns).select(*[F.col(col) for col in self.required_column_list])
                else:
                    self.input_file_df = self.input_file_df.select(*new_columns)

            self.dq_logging.info(f'{self.file_name}, Alphabets are removed from integer columns completed successfully')
        except Exception as e:
            self.exception_error(e, "removing_alphabets", "DQ4")

    # Method for DQ5 - Remove '~' from Regions_PO_No column and appending '00'
    def edi_columns_cleanup(self):
        try:
            if len(self.edi_cleanup_columns) > 0:
                for each_column in self.edi_cleanup_columns:
                    self.input_file_df = self.input_file_df \
                        .withColumn(each_column, F.trim(F.regexp_replace(col(each_column), '\\~', ''))) \
                        .withColumn(each_column,
                                    when((col(each_column).startswith('5') | col(each_column).startswith('7')),
                                         (concat(lit("00"), \
                                                 col(each_column)))).otherwise(col(each_column)))
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    self.input_file_df = self.input_file_df.select(self.req_column_list)

            self.dq_logging.info(f'{self.file_name}, Special characters are removed from the columns in the EDI file completed successfully')
        except Exception as e:
            self.exception_error(e,"edi_columns_cleanup", "DQ5")

    # Method for DQ6 - Row level duplicate check
    def row_level_duplicate_check(self, region):
        try:
            # Region & common exclusions
            region_exclusions = {
                "ams": ['Gpsy_Giant_HPN', 'Local_Sample', 'NonLocal_Sample'],
                "emea": ['SC_BFT_Calculated_History_Data'],
                "ww": ['Gpsy_Giant_HPN', 'Local_Sample', 'NonLocal_Sample', 'CONSOL_Refresh_Tracker_With_Pricing'],
                "apj": []
            }
            common_exclusions = [
                "Part_Master_D-All_SKUs", "Supplier_Accton_Wired", 
                "SFDC", "AMS_OM_CrossBorderReport"
            ]

            if self.file_key in common_exclusions or self.file_key in region_exclusions.get(region, []):
                self.dq_logging.info(f'Row level duplicates check skipped for {region.upper()}')
                return

            print(f"-------Performing {region.upper()} row-level duplicate check----------")

            # Break deep transformation lineage early to prevent StackOverflow
            #self.input_file_df = self.input_file_df.select("*").cache()
            #self.input_file_df.count()  # Force evaluation

            # Find duplicate rows
            
            #tmp_input_file_df = self.master_data_conversion().select("*")
            tmp_input_file_df = self.input_file_df
            tmp_input_file_df.persist()
            
            duplicate_rows = (
                tmp_input_file_df
                .groupBy(tmp_input_file_df.columns)
                .count()
                .filter(F.col("count") > 1)
            )

            reject_row_count = duplicate_rows.selectExpr("sum(count - 1)").collect()[0][0]
            reject_row_count = reject_row_count if reject_row_count is not None else 0
            print(f"---------Initial Reject Row Count: {reject_row_count}--------")

            # Drop duplicate logic
            drop_duplicates_list = {
                "ams": ['ARUBA EXTERNAL_STOCKABLE_NONSTOCKABLE_LIST', 'JAPAN_stockable', 'Country_Mapping',
                        'Material_Type_Mapping', 'CRITICAL_DEAL_LIST', 'EGORAS_AMS', 'EGORAS_EMEA', 'EGORAS_APJ',
                        'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE', 'APJ_Open_PO', 'HPN_Product_Homologation_Matrix',
                        'CM_SKU'],
                "emea": ['JAPAN_stockable', 'Country_Mapping', 'Material_Type_Mapping', 'CRITICAL_DEAL_LIST',
                         'EGORAS_AMS', 'EGORAS_EMEA', 'EGORAS_APJ', 'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE',
                         'APJ_Open_PO', 'HPN_Product_Homologation_Matrix'],
                "ww": ['JAPAN_stockable', 'Country_Mapping', 'Material_Type_Mapping', 'CRITICAL_DEAL_LIST', 'EGORAS_AMS',
                       'EGORAS_EMEA', 'EGORAS_APJ', 'APJ_DSM_Report_ALL_HPE', 'APJ_Open_PO',
                       'HPN_Product_Homologation_Matrix', 'CM_SKU', 'Base_Product_Data'],
                "apj": ['JAPAN_stockable', 'Country_Mapping', 'CRITICAL_DEAL_LIST', 'EGORAS_AMS', 'EGORAS_EMEA',
                        'EGORAS_APJ', 'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE', 'APJ_Open_PO',
                        'HPN_Product_Homologation_Matrix']
            }

            if reject_row_count > 0 and self.file_key in drop_duplicates_list.get(region, []):
                self.input_file_df = self.input_file_df.dropDuplicates()
                reject_row_count = 0
                print(f"---------Post-Drop Reject Count: {reject_row_count}--------")

            # Reset reject count logic
            reset_reject_count_list = {
                        "ams": ['AMS_S4_I', 'AMS_Big_Deal_Tracker', 'AMS_S4_OM_HOLDS_CM_SKU', 'AMS_Fusion_OM_HOLDS_CM_SKU',
                                'OM_APJ_SNI', 'Reservation_list', 'CustomerType_OSS_AMS', 'S4_AMS_OM_HOLDS_CM_SKU',
                                'EMEA_S4_E2E_Dashboard', 'AMS_S4_E2E_Dashboard', 'EGI_Detailed_AMS', 'EGI_Detailed_APJ',
                                'EGI_Detailed_EMEA', 'Supplier_Accton_Wired', 'SFDC', 'AMS_OM_CrossBorderReport',
                                'EMEA_Channel_SellOut', 'AMS_Channel_SellOut', 'APJ_Channel_SellOut', 'AMS_Inventory',
                                'APJ_Inventory', 'EMEA_Inventory'],
                        "emea": ['EMEA_Ringfencing', 'EMEA_S4_E2E_Dashboard', 'AMS_S4_E2E_Dashboard', 'OM_APJ_SNI',
                                'Reservation_list', 'EMEA_Channel_SellOut', 'AMS_Channel_SellOut', 'APJ_Channel_SellOut',
                                'APJ_Inventory', 'EMEA_Inventory'],
                        "ww": ['Rebalance_Indicator','ARUBA-SKU_Info','VAD_Split_Tracker','EMEA_S4_E2E_Dashboard',
                               'AMS_S4_E2E_Dashboard', 'EGI_Detailed_AMS','QTD_WW_Shipments', 'EGI_Detailed_APJ',
                               'EGI_Detailed_EMEA', 'Supplier_Accton_Wired', 'SFDC', 'AMS_OM_CrossBorderReport','EMEA_Channel_SellOut',
                               'AMS_Channel_SellOut','APJ_Channel_SellOut'],
                        "apj": ['APJ_Big_Deal_Tracker', 'Material_Type_Mapping', 'EGI_Detailed_APJ', 'EMEA_S4_E2E_Dashboard',
                                'AMS_S4_E2E_Dashboard', 'OM_APJ_SNI', 'Reservation_list', 'EMEA_Channel_SellOut',
                                'AMS_Channel_SellOut', 'APJ_Channel_SellOut', 'APJ_Inventory']
            }

            if reject_row_count > 0 and self.file_key in reset_reject_count_list.get(region, []):
                reject_row_count = 0
                print(f"---------Post-Reset Reject Count: {reject_row_count}--------")

            # Handle allow_duplicates flag
            if reject_row_count > 0:
                if self.allow_duplicates == 0:
                    self.input_file_df = self.input_file_df.dropDuplicates()
                    reject_row_count = 0
                    print("Dropped duplicates due to allow_duplicates=0")
                elif self.allow_duplicates == 1:
                    print("Allow Dups value =1")
                    reject_row_count = 0

            # Final rejection if still > 0
            if reject_row_count > 0:
                dump_df = self.input_file_df.exceptAll(self.input_file_df.dropDuplicates())
                self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]
                dump_df = dump_df.withColumn('reject_reason', lit('row level duplicate')) \
                                 .withColumn('rejected_column', lit('Full Record')) \
                                 .select(self.req_column_list + ['reject_reason', 'rejected_column'])

                self.rejected_df_list.append(dump_df)
                print(f"Rejected {reject_row_count} rows")

                app_name = "Sc360"
                rejection_file_path = "NA"
                self.process_name = f'Data-Validation Duplicate Check - {region.upper()}'
                self.dq_logging.send_sns_message(
                    self.pipeline_run_id, app_name, self.exec_env, self.region_name,
                    self.file_key, self.process_name, self.error_message,
                    rejection_file_path, self.job_name, self.loggroupname, self.job_run_id, self.sns_arn
                )

            # Logging
            self.dq_logging.info(f"Final reject count for {region.upper()}: {reject_row_count}")
            self.dq_logging.info(f"{self.file_name}, {region.upper()} duplicate check completed. Duplicates: {reject_row_count}")
            #self.input_file_df.unpersist()

        except Exception as e:
            self.exception_error(e, "row_level_duplicate_check", "DQ6")


    # Method for DQ6.1 - Row level duplicate check for csv
    def row_level_duplicate_check_csv(self, region):
        try:
            # Define region-specific exclusions
            region_exclusions = {
                "ams": ['Gpsy_Giant_HPN', 'Local_Sample', 'NonLocal_Sample'],
                "emea": ['SC_BFT_Calculated_History_Data'],
                "apj": []  # No extra exclusions for APJ
            }

            # Common exclusions (check for NOT IN these values)
            common_exclusions = [
                "Part_Master_D-All_SKUs", "Supplier_Accton_Wired", 
                "SFDC", "AMS_OM_CrossBorderReport"
            ]

            # Check if file should be processed (original logic: NOT in exclusions)
            if (self.file_key not in common_exclusions and 
                self.file_key not in region_exclusions.get(region, [])):
                
                print("-------Performing row-level duplicate check----------")
                tmp_input_file_df = self.read_csv_file()
                print("-------File read completed----------")
                tmp_input_file_df.persist()

                print("-------Removing duplicates----------")
                # Group by all columns to find duplicate rows
                duplicate_rows = (
                    tmp_input_file_df
                    .groupBy(tmp_input_file_df.columns)
                    .count()
                    .filter(F.col("count") > 1)
                )

                # Sum up (count - 1) for each duplicate group to get total reject count
                reject_row_count = duplicate_rows.selectExpr("sum(count - 1)").collect()[0][0]
                reject_row_count = reject_row_count if reject_row_count is not None else 0
                print(f"---------Reject Row Count: {reject_row_count}--------")

                # Define region-specific duplicate removal rules
                remove_duplicates = {
                    "ams": ['ARUBA EXTERNAL_STOCKABLE_NONSTOCKABLE_LIST', 'JAPAN_stockable', 'Country_Mapping',
                            'Material_Type_Mapping', 'CRITICAL_DEAL_LIST', 'EGORAS_AMS', 'EGORAS_EMEA', 'EGORAS_APJ',
                            'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE', 'APJ_Open_PO', 'HPN_Product_Homologation_Matrix',
                            'CM_SKU'],
                    "emea": ['JAPAN_stockable', 'Country_Mapping', 'Material_Type_Mapping', 'CRITICAL_DEAL_LIST',
                            'EGORAS_AMS', 'EGORAS_EMEA', 'EGORAS_APJ', 'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE',
                            'APJ_Open_PO', 'HPN_Product_Homologation_Matrix'],
                    "apj": ['JAPAN_stockable', 'Country_Mapping', 'CRITICAL_DEAL_LIST', 'EGORAS_AMS', 'EGORAS_EMEA',
                            'EGORAS_APJ', 'QTD_WW_Shipments', 'APJ_DSM_Report_ALL_HPE', 'APJ_Open_PO',
                            'HPN_Product_Homologation_Matrix']
                }

                if reject_row_count > 0 and self.file_key in remove_duplicates.get(region, []):
                    self.input_file_df = self.input_file_df.dropDuplicates(self.input_file_df.columns)
                    reject_row_count = 0

                # Files where reject_row_count should be reset to 0
                reset_reject_count = {
                    "ams": ['AMS_S4_I', 'AMS_Big_Deal_Tracker', 'AMS_S4_OM_HOLDS_CM_SKU', 'AMS_Fusion_OM_HOLDS_CM_SKU',
                            'OM_APJ_SNI', 'Reservation_list', 'CustomerType_OSS_AMS', 'S4_AMS_OM_HOLDS_CM_SKU',
                            'EMEA_S4_E2E_Dashboard', 'AMS_S4_E2E_Dashboard', 'EGI_Detailed_AMS', 'EGI_Detailed_APJ',
                            'EGI_Detailed_EMEA', 'Supplier_Accton_Wired', 'SFDC', 'AMS_OM_CrossBorderReport',
                            'EMEA_Channel_SellOut', 'AMS_Channel_SellOut', 'APJ_Channel_SellOut', 'AMS_Inventory',
                            'APJ_Inventory', 'EMEA_Inventory'],
                    "emea": ['EMEA_Ringfencing', 'EMEA_S4_E2E_Dashboard', 'AMS_S4_E2E_Dashboard', 'OM_APJ_SNI',
                            'Reservation_list', 'EMEA_Channel_SellOut', 'AMS_Channel_SellOut', 'APJ_Channel_SellOut',
                            'APJ_Inventory', 'EMEA_Inventory'],
                    "apj": ['APJ_Big_Deal_Tracker', 'Material_Type_Mapping', 'EGI_Detailed_APJ', 'EMEA_S4_E2E_Dashboard',
                            'AMS_S4_E2E_Dashboard', 'OM_APJ_SNI', 'Reservation_list', 'EMEA_Channel_SellOut',
                            'AMS_Channel_SellOut', 'APJ_Channel_SellOut', 'APJ_Inventory']
                }

                if reject_row_count > 0 and self.file_key in reset_reject_count.get(region, []):
                    reject_row_count = 0

                # Handling allow_duplicates logic
                if reject_row_count > 0:
                    if self.allow_duplicates == 0:
                        self.input_file_df = self.input_file_df.dropDuplicates(self.input_file_df.columns)
                        reject_row_count = 0
                    elif self.allow_duplicates == 1:
                        print("Allow Dups value =1")  # Add missing print statement
                        reject_row_count = 0

                # Send rejection report if needed
                if reject_row_count > 0:
                    dump_df = self.input_file_df.exceptAll(self.input_file_df.dropDuplicates(self.input_file_df.columns))
                    app_name = "Sc360"
                    self.process_name = 'Data-Validation Duplicate Check For CSV' if region == "emea" else 'Data-Validation Duplicate Check'
                    rejection_file_path = "NA"
                    self.dq_logging.send_sns_message(self.pipeline_run_id, app_name, self.exec_env, self.region_name,
                                        self.file_key, self.process_name, self.error_message, rejection_file_path, self.job_name, self.loggroupname, self.job_run_id, self.sns_arn)
                    print("Rejection handling triggered.")
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    dump_df = dump_df.withColumn('reject_reason', lit('row level duplicate')).withColumn(
                        'rejected_column', lit('Full Record')).select(
                        self.req_column_list + ['reject_reason', 'rejected_column'])
                    self.rejected_df_list.append(dump_df)

                # Logging
                self.dq_logging.info(f"Rejected row count for {region.upper()}: {reject_row_count}")
                self.dq_logging.info(
                    f'{self.file_name}, Row level duplicates check completed successfully for csv. Row level duplicate count : {reject_row_count} ')
                tmp_input_file_df.unpersist()
            else:
                self.dq_logging.info('Row level duplicates check skipped')  # Fix typo: "skiped" -> "skipped"

        except Exception as e:
            self.exception_error(e,"row_level_duplicate_check_csv", "DQ6.1")
            
    # Method for DQ7 - Integer Column check
    def integer_check(self):
        try:
            if len(self.int_check_columns) > 0:
                for each_column in self.int_check_columns:
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    reject_df_temp = self.input_file_df.select(self.req_column_list) \
                                                        .withColumn('tempcol', self.input_file_df[each_column].cast("long")) \
                                                        .withColumn("reject_reason",
                                                            F.when(((F.trim(col('tempcol')) == '') | (col('tempcol').isNull())),
                                                            lit(each_column + "_integer_format_incorrect")).otherwise(0)) \
                                                        .withColumn('rejected_column', lit(each_column)).where(col('reject_reason') != '0').drop('tempcol')
                    if reject_df_temp.count() > 0:
                        self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                        reject_df_temp = reject_df_temp.select(self.req_column_list + ['reject_reason', 'rejected_column'])
                        print(reject_df_temp.select(['reject_reason', 'rejected_column']).show(5))
                        sys.exit(1)
                        self.rejected_df_list.append(reject_df_temp)

            self.dq_logging.info(f'{self.file_name}, Integer check operation completed successfully')
        except Exception as e:
            self.exception_error(e,"integer_check", "DQ7")
            
    def integer_check_csv(self):
        try:
            if len(self.int_check_columns) > 0:
                for each_column in self.int_check_columns:
                    tmp_input_file_df = self.read_csv_file()
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    reject_df_temp = tmp_input_file_df.select(self.req_column_list) \
                        .withColumn('tempcol', tmp_input_file_df[each_column].cast("long")) \
                        .withColumn("reject_reason", F.when(((F.trim(col('tempcol')) == '') | (col('tempcol').isNull())), lit(each_column + "_integer_format_incorrect")).otherwise(0)) \
                        .withColumn('rejected_column', lit(each_column)).where(col('reject_reason') != '0').drop('tempcol')
                    reject_df_temp.persist()
                    if reject_df_temp.count() > 0:
                        reject_df_temp = reject_df_temp.select(
                            self.req_column_list + ['reject_reason', 'rejected_column'])
                        print(reject_df_temp.select(['reject_reason', 'rejected_column']).show(5))
                        self.rejected_df_list.append(reject_df_temp)

            self.dq_logging.info(f'{self.file_name}, Integer check operation completed successfully for csv')
            #reject_df_temp.unpersist()
        except Exception as e:
            self.exception_error(e,"integer_check_csv", "DQ7.1")

    # Method for DQ8 - Null Column check
    def null_check(self):
        try:
            if len(self.null_check_columns) > 0:
                for each_column in self.null_check_columns:
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    reject_df_temp = self.input_file_df.select(self.req_column_list) \
                        .withColumn('tempcol', F.when(((F.trim(col(each_column)) == '') | (col(each_column).isNull())),
                                                      1).otherwise(0)) \
                        .withColumn("reject_reason", F.when((F.trim(col('tempcol')) == 1),
                                                            lit(each_column + "_null_check_failed")).otherwise(0)) \
                        .withColumn('rejected_column', lit(each_column)).where(col('reject_reason') != '0').drop(
                        'tempcol')
                    if reject_df_temp.count() > 0:
                        reject_df_temp = reject_df_temp.select(
                            self.req_column_list + ['reject_reason', 'rejected_column'])
                        self.rejected_df_list.append(reject_df_temp)

            self.dq_logging.info(f'{self.file_name}, Null check operation completed successfully')
        except Exception as e:
            self.exception_error(e,"null_check", "DQ8")

    # Method for DQ9 - Date Format check
    def date_format_check(self):
        try:
            if len(self.date_columns) > 0:
                for each_column in self.date_columns:
                    for cname, date_format in each_column.items():
                        self.dq_logging.info(f"Processing column: {cname}, expected format: {date_format}")
                        self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                        reject_df_temp = self.input_file_df.select(self.req_column_list).where(
                            (F.trim(col(cname)) != '') & (F.trim(col(cname)).isNotNull()) & (F.trim(col(cname)) != None)).withColumn('tempcol',
                                                                                                      from_unixtime(unix_timestamp(cname,date_format)).cast('date')) \
                            .withColumn("reject_reason",
                                        F.when(((F.trim(col('tempcol')) == '') | (col('tempcol').isNull())),
                                               lit(f"{cname}_date_format_incorrect")).otherwise(0))\
                                                .withColumn('rejected_column', lit(cname)).where(col('reject_reason') != '0').drop('tempcol')

                        if reject_df_temp.count() > 0:
                            self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                            reject_df_temp = reject_df_temp.select(
                                self.req_column_list + ['reject_reason', 'rejected_column'])
                            self.dq_logging.info('Got Rejection in Date Format Check')
                            self.dq_logging.info(reject_df_temp.select(['reject_reason', 'rejected_column',cname]).show(5))
                            self.rejected_df_list.append(reject_df_temp)

            self.dq_logging.info(f'{self.file_name}, Date format check operation completed successfully')
        except Exception as e:
            self.exception_error(e,"date_format_check", "DQ9")

    # Method for DQ14 - Date Format check
    def standarize_date_format(self):
        try:
            # replace none with empty string
            if self.file_key in ['AMS_S4_E2E_Dashboard'] :
                self.input_file_df.na.fill(value = '')
    
            if len(self.date_columns) > 0:
                for each_column in self.date_columns:
                    for cname, date_format in each_column.items():
                        self.dq_logging.info(f"Processing column: {cname}, expected format: {date_format}")
                        self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                        self.input_file_df = self.input_file_df.select(self.req_column_list)\
                                                                .withColumn('tempcol', from_unixtime(unix_timestamp(cname,date_format)).cast('date')).drop(cname)\
                                                                .withColumnRenamed('tempcol', cname).drop('tempcol')

            self.dq_logging.info(f'{self.file_name}, Standardize Date format operation completed successfully')
        except Exception as e:
            self.exception_error(e,"standarize_date_format", "DQ14")

    # Method for DQ15 - Timestamp Format check
    def standardize_timestamp_format(self):
        try:
            if len(self.timestamp_columns) > 0:
                for each_column in self.timestamp_columns:
                    for cname, timestamp_format in each_column.items():
                        # Standardize the timestamp format and update the column
                        self.input_file_df = self.input_file_df.withColumn(cname,from_unixtime(unix_timestamp(cname, timestamp_format)).cast('timestamp'))

            self.dq_logging.info(f'{self.file_name}, Standardize timestamp format  operation completed successfully')
        except Exception as e:
            self.exception_error(e,"standardize_timestamp_format", "DQ15")

    # Method for DQ10 - Timestamp Format check
    def timestamp_format_check(self):
        try:
            if len(self.timestamp_columns) > 0:
                for each_column in self.timestamp_columns:
                    for cname, timestamp_format in each_column.items():
                        self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                        reject_df_temp = self.input_file_df.select(self.req_column_list).where(
                            (F.trim(col(cname)) != '') & (F.trim(col(cname)).isNotNull())).withColumn('tempcol',
                                                                                                      from_unixtime(
                                                                                                          unix_timestamp(
                                                                                                              cname,
                                                                                                              timestamp_format)).cast(
                                                                                                          'timestamp')) \
                            .withColumn("reject_reason",
                                        F.when(((F.trim(col('tempcol')) == '') | (col('tempcol').isNull())),
                                               lit(cname + "_timestamp_format_incorrect")).otherwise(0)).withColumn(
                            'rejected_column', lit(cname)).filter(
                            col('reject_reason') != '0').drop('tempcol')
                        if not reject_df_temp.isEmpty():
                            self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                            reject_df_temp = reject_df_temp.select(
                                self.req_column_list + ['reject_reason', 'rejected_column']
                            )
                            self.rejected_df_list.append(reject_df_temp)
            
            self.dq_logging.info(f'{self.file_name}, Timestamp format check operation completed successfully')
        except Exception as e:
            self.exception_error(e,"timestamp_format_check", "DQ10")

    # Method for DQ11 - Integer Column check
    def cast_to_integer(self):
        try:
            if len(self.cast_to_int_columns) > 0:
                for each_column in self.cast_to_int_columns:
                    # Cast the column to 'long' and rename in one efficient operation
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    self.input_file_df = self.input_file_df.select(self.req_column_list).withColumn(each_column, self.input_file_df[each_column].cast("long"))

                # self.input_file_df.show(5,truncate=False)
            self.dq_logging.info(f'{self.file_name}, Cast to Integer operation completed successfully')
        except Exception as e:
            self.exception_error(e,"cast_to_integer", "DQ11")

    def cast_to_double(self):
        try:
            if len(self.cast_to_double_columns) > 0:
                for each_column in self.cast_to_double_columns:
                    # Directly cast the column to 'double' (DoubleType)
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    self.input_file_df = self.input_file_df.select(self.req_column_list).withColumn(each_column,self.input_file_df[each_column].cast(DoubleType()))

            self.dq_logging.info(f'{self.file_name}, Cast to Integer operation completed successfully')
        except Exception as e:
            self.exception_error(e,"cast_to_double", "DQ11")

    # Method for DQ12 - Replace Null Values check
    def replace_null_values(self):
        try:
            if len(self.cast_to_int_columns) > 0:
                for each_column in self.cast_to_int_columns:
                    self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                    self.input_file_df = self.input_file_df.fillna({each_column: 0}).select(self.req_column_list)

            self.dq_logging.info(f'{self.file_name}, Cast to Integer operation completed successfully')
        except Exception as e:
            self.exception_error(e,"replace_null_values", "DQ12")

    # DQ13 replace_zeros_with_null_date
    def replace_zeros_with_null_date_column(self):
        try:
            date_zero = []
            if self.file_key == 'AMS_QS_DAILY_FILE':
                date_char = ['-']
                if len(self.replace_zero_null) > 0:
                    for each_column in self.replace_zero_null:
                        for x in date_char:
                            for y in x:
                                print("replace_zeros_with_null_date_column " + str(y))
                                self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                                self.input_file_df = self.input_file_df.withColumn(each_column,
                                                                                   when((col(each_column) == y),"").otherwise(col(each_column))).select(self.req_column_list)
    
                        print("replace_zeros_with_null_date_column")
            else:
                date_zero = [0]
                date_char = ['#', '-', 'X']
                date_string = ['TB-ADV', 'Not available']
                if len(self.replace_zero_null) > 0:
                    for each_column in self.replace_zero_null:
                        for x in date_zero, date_char, date_string:
                            for y in x:
                                print("replace_zeros_with_null_date_column" + str(y))
                                self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                                self.input_file_df = self.input_file_df.withColumn(each_column,
                                                                                   when((col(each_column) == y), "").otherwise(col(each_column))).select(self.req_column_list)
    
                        print("replace_zeros_with_null_date_column")

            self.dq_logging.info(f'{self.file_name}, Replace_zeros_with_null_date_column in the columns completed successfully')
        except Exception as e:
            self.exception_error(e,"replace_null_values", "DQ13")

    # Method to write the Excel output to the Output directory
    def rejected_records_handle_write_output(self, csv=False):
        try:
            self.output_file_bucket_del = self.output_file_path_del.split('/', 3)[2]
            print(self.output_file_bucket_del)
            self.output_file_key_del_key = self.output_file_path_del.split('/', 3)[3]
            self.output_file_key_del = self.output_file_key_del_key + str(self.file_key) + "/"

            self.sheet_name_refined = self.sheet_name.replace(' ', '_')
            if len(self.config_file_content[self.file_key].keys()) > 1:
                self.final_output_file_name = self.file_key + "~" + self.sheet_name_refined + "_" + time.strftime(
                    '%Y%m%d%H%M%S') + ".csv"
            else:
                self.final_output_file_name = self.file_key + "_" + time.strftime('%Y%m%d%H%M%S') + ".csv"

            if len(self.config_file_content[self.file_key].keys()) > 1:
                final_output_file_path = self.output_file_path + self.file_key + "~" + self.sheet_name_refined + "/" + self.final_output_file_name
            else:
                final_output_file_path = self.output_file_path + self.file_key + "/" + self.final_output_file_name

            # **Select input file processing method**
            if csv:
                self.input_file_df = self.read_csv_file()
            '''
            else:
                #self.input_file_df = self.input_file_df
                self.input_file_df = self.master_data_conversion().select("*")
            '''
            self.input_file_df.persist()
            print("-------Input File Read Completed----------")

            if len(self.rejected_df_list) != 0:
                reject_df = reduce(DataFrame.union, tuple(self.rejected_df_list)).persist()
                self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
                for each_col in self.required_column_list:
                    reject_df = reject_df.withColumn(f"`{each_col}`", F.when((F.trim(col(f"`{each_col}`")) == ''), None)
                                                    .otherwise(col(f"`{each_col}`"))) \
                                        .select(self.req_column_list + ['reject_reason', 'rejected_column'])
                    self.input_file_df = self.input_file_df.withColumn(f"`{each_col}`", F.when((F.trim(col(f"`{each_col}`")) == ''), None)
                                                            .otherwise(col(f"`{each_col}`"))) \
                                                .select(self.req_column_list)

                rejected_rows_df = reject_df.drop('reject_reason', 'rejected_column') \
                                            .select(self.req_column_list) \
                                            .dropDuplicates(self.required_column_list) \
                                            .persist()

                self.input_file_full = self.input_file_df
                self.input_file_df = self.input_file_df.select(self.req_column_list).exceptAll(rejected_rows_df).persist()
                self.raw_count = self.input_file_df.count()

                reject_summary_df = reject_df.groupBy(self.req_column_list) \
                                            .agg(collect_list(col('reject_reason')).alias("reject_reason_list"),
                                                collect_list(col('rejected_column')).alias("rejected_column_list")) \
                                            .withColumnRenamed('reject_reason_list', 'reject_reason') \
                                            .withColumnRenamed('rejected_column_list', 'rejected_column')

                self.reject_count = reject_summary_df.count()
                self.dq_logging.info("rejected_count {}".format(self.reject_count))

                # Convert to Pandas and write rejected records to AWS S3 Reject path
                rejected_file_name = "rejected_" + self.final_output_file_name
                reject_summary_df.toPandas().to_csv(rejected_file_name, index=False, encoding='utf-8', sep='|')

                reject_s3_bucket = self.reject_data_path.split('/', 3)[2]
                reject_s3_key = self.reject_data_path.split('/', 3)[3] + self.file_key + "/" + rejected_file_name

                self.s3_client.put_object(Body=open(rejected_file_name, 'rb'), Bucket=reject_s3_bucket, Key=reject_s3_key)
                self.dq_logging.info("Rejected records written to S3 successfully.")

                # Write Final Output
                input_file_full_pandas = self.input_file_full.toPandas()
                input_file_full_pandas.to_csv(self.final_output_file_name, index=False, encoding='utf-8', sep='|')

                reject_s3_bucket = self.reject_file_path.split('/', 3)[2]
                reject_s3_key = self.reject_file_path.split('/', 3)[3] + self.file_key + "/" + rejected_file_name

                self.s3_client.put_object(Body=open(self.final_output_file_name, 'rb'), Bucket=reject_s3_bucket,
                                        Key=reject_s3_key)
                self.dq_logging.info('Rejected file ' + self.final_output_file_name)

                self.error_message = 'Full File Rejected as we have {} rejection records'.format(self.reject_count)

                rejection_file_path = urlparse('s3://' + reject_s3_bucket + '/' + reject_s3_key)

                # Send rejection email
                app_name = "SC360"
                self.dq_logging.send_sns_message(self.pipeline_run_id, app_name, self.exec_env, self.region_name,
                                    rejected_file_name, self.process_name, self.error_message, rejection_file_path.geturl(), self.job_name, self.loggroupname, self.job_run_id, self.sns_arn)

                self.dq_logging.info(
                    '{}, Rejected records & rejected file are written to respective paths successfully.'.format(
                        self.file_name))
            else:
                self.raw_count = self.input_file_df.count()
                self.input_file_df = self.input_file_df.withColumn('SOURCE_NAME', lit(self.file_name)) \
                                            .withColumn('CREATED_BY', lit(self.getuser)) \
                                            .withColumn('BATCH_RUN_DT', lit(self.batch_date))

                input_file_df_pandas = self.input_file_df.toPandas()
                print(input_file_df_pandas.columns)
                extra_cols = ["SOURCE_NAME", "CREATED_BY", "BATCH_RUN_DT"]
                valid_cols = [col for col in self.required_column_list if col in input_file_df_pandas.columns]+ \
                                        [col for col in extra_cols if col in input_file_df_pandas.columns]
                input_file_df_pandas = input_file_df_pandas[valid_cols]
                print(input_file_df_pandas.columns)
                s3 = boto3.resource('s3')
                bucketName = s3.Bucket(self.output_file_bucket_del)
                bucketName.objects.filter(Prefix=self.output_file_key_del).delete()

                input_file_df_pandas.to_csv(self.final_output_file_name, index=False, encoding='utf-8', sep='|')
                
                output_s3_bucket = final_output_file_path.split('/', 3)[2]
                output_s3_key = final_output_file_path.split('/', 3)[3]

                self.s3_client.put_object(Body=open(self.final_output_file_name, 'rb'), Bucket=output_s3_bucket,
                                        Key=output_s3_key)
                self.dq_logging.info("Data quality checked output is written to output path successfully.")

        except Exception as e:
            self.exception_error(e,"rejected_records_handle_write_output", "")


    # DQ11 Empty File Rejection
    def empty_file_rejection(self):
        try:
            self.sheet_name_refined_empty_file = ''.join(e for e in self.sheet_name if e.isalnum())
            self.final_output_file_name = self.file_key + "_" + time.strftime('%Y%m%d%H%M%S') + ".csv"
            self.req_column_list = [col(f"`{col_name}`") for col_name in self.required_column_list]  # Handle special characters
            self.input_file_df = self.input_file_df.withColumn("reject_reason", lit('Empty File')).select(self.req_column_list + ['reject_reason'])

            config_keys_len = len(self.config_file_content[self.file_key].keys())
            if config_keys_len > 1:
                final_empty_file_path = self.output_file_path + self.file_key + "_" + self.sheet_name_refined_empty_file + "/" + self.final_output_file_name
            else:
                final_empty_file_path = self.output_file_path + self.file_key + "/" + self.final_output_file_name

            if config_keys_len > 1:
                reject_s3_key = self.reject_file_path.split('/', 3)[3] + self.file_key + "_" + self.sheet_name_refined_empty_file + "/" + "rejected_" + self.final_output_file_name
            else:
                rejected_file_name = "rejected_" + self.final_output_file_name
                reject_s3_key = self.reject_file_path.split('/', 3)[3] + self.file_key + "/" + rejected_file_name
                reject_s3_bucket = self.reject_file_path.split('/', 3)[2]
                input_file_full_pandas = self.input_file_df.toPandas()
                input_file_full_pandas.to_csv(self.final_output_file_name, index=False, encoding='utf-8', sep='|')

                self.s3_client.put_object(Body=open(self.final_output_file_name, 'rb'), Bucket=reject_s3_bucket, Key=reject_s3_key)
                self.dq_logging.info('Rejected Empty file' + self.final_output_file_name)
                
                reject_s3_bucket = self.reject_data_path.split('/', 3)[2]
                reject_s3_key = self.reject_data_path.split('/', 3)[3] + self.file_key + "/" + rejected_file_name
                rejection_file_path = urlparse('https://' + reject_s3_bucket + '.s3.amazonaws.com' + '/' + reject_s3_key)
                app_name = "SC360"
                # Send rejection Email
                self.dq_logging.send_sns_message(self.pipeline_run_id, app_name, self.exec_env, self.region_name, rejected_file_name,
                                      self.process_name, self.error_message, rejection_file_path.geturl(), self.job_name, self.loggroupname, self.job_run_id, self.sns_arn)

                self.dq_logging.info(f'{self.file_name}, Rejected records & rejected file are written to respective paths successfully.')
        except Exception as e:
            self.exception_error(e,"empty_file_rejection", "")

    # Method to read the CSV file and get the PySpark Dataframe
    def read_csv_file(self):
        try:
            self.input_file_bucket = self.input_file_path.split('/', 3)[2]
            self.input_file_key = self.input_file_path.split('/', 3)[3]
            self.csv_file_s3_object = self.s3_client.get_object(Bucket=self.input_file_bucket, Key=self.input_file_key)

            csv_pandas_df = self.handle_csv_pivoting()

            self.dq_logging.info(f'{self.file_name}, CSV file was read successfully')

            return csv_pandas_df
        except Exception as e:
            self.exception_error(e,"read_csv_file", "")

    # Method to handle the pivoting explicitly
    def handle_csv_pivoting(self):
        try:
            large_string = self.csv_file_s3_object['Body'].read().decode("utf-8", errors='ignore')

            if '08212020' in self.file_key:
                csv_pandas_df = pd.read_csv(io.StringIO(large_string), delimiter=self.delimiter,
                                            header=self.header_row_number_to_select, encoding="utf-8").astype(str).replace('nan', '').replace('NaT', '')

                original_month_columns = list(csv_pandas_df.columns)[4:]
                # Schema for the Build Plan Wired file to be imposed
                month_fields = [StructField(f"Month{i}", StringType(), True) for i in range(1, 18)]
                pivot_file_schema = StructType([StructField("THEATER", StringType(), True), StructField("BASE_PRODUCT", StringType(), True),
                                                StructField("DEMAND_TYPE", StringType(), True), StructField("KEY_FIGURE", StringType(), True)] + month_fields)
                                                
                # Clean up fully empty or whitespace-only rows before Spark conversion
                csv_pandas_df = csv_pandas_df.dropna(how='all')  # removes all-NaN rows
                csv_pandas_df = csv_pandas_df[~(csv_pandas_df.applymap(lambda x: str(x).strip()) == '').all(axis=1)]  # removes rows with only whitespace


                bp_df = self.spark.createDataFrame(csv_pandas_df.replace('nan', '').replace('NaT', ''),
                                                   schema=pivot_file_schema)

                mnths = [[f"'Month{i}', Month{i}" for i in range(1, 18)]]
                stack_expression = f"stack(17, {', '.join(mnths)}) as (MONTH_NAME, SALES_FORECAST_QT)"
                unPivot_DF = bp_df.selectExpr("THEATER", "BASE_PRODUCT", "DEMAND_TYPE", "KEY_FIGURE",stack_expression)
                final_columns_dictionary = {"Month" + str(i + 1): original_month_columns[i] for i in range(17)}
                final_bp_df = unPivot_DF.replace(final_columns_dictionary)
                final_bp_df = final_bp_df.withColumn("MONTH_NAME", concat(F.split(F.col("MONTH_NAME"), " ").getItem(0), F.split(F.col("MONTH_NAME"), " ").getItem(1)))
                pivot_output_df = final_bp_df.withColumn('SALES_FORECAST_QT', F.when(F.isnan(col('SALES_FORECAST_QT')), '').otherwise(col('SALES_FORECAST_QT')))

            else:
                lst_str_cols = self.string_check_columns
                dict_dtypes = {x: 'str' for x in lst_str_cols}
                csv_pandas_df = pd.read_csv(io.StringIO(large_string), delimiter=self.delimiter, header=self.header_row_number_to_select, names=self.required_column_list,
                                            encoding="utf-8",dtype=dict_dtypes).astype(str).replace('nan', '').replace('NaT', '')
                # Remove fully empty rows and whitespace-only rows
                csv_pandas_df = csv_pandas_df.dropna(how='all')
                csv_pandas_df = csv_pandas_df[~(csv_pandas_df.applymap(lambda x: str(x).strip()) == '').all(axis=1)]

                pivot_output_df = self.spark.createDataFrame(csv_pandas_df)

            return pivot_output_df

        except Exception as e:
            self.exception_error(e,"handle_csv_pivoting", "")

    # Method to update the audit log table in AWS RDS
    def update_audit_log(self):
        try:
            batch_id = str(self.batch_id)
            batch_run_date = str(self.batch_date)
            region_name = str(self.region_name)
            data_pipeline_name = str(self.data_pipeline_name)
            data_pipeline_run_id = str(self.pipeline_run_id)
            process_name = str(self.process_name)
            file_name = self.file_key
            source_platform = 'S3'
            target_platform = 'S3'
            activity = 'EMRActivity'
            environment = str(self.exec_env)
            script_path = str(self.script_path)
            execution_status = str(self.dv_execution_status)
            error_message = self.error_message[:2000].replace("'", "''")
            execution_start_time = self.execution_start_time
            execution_end_time = self.execution_end_time
            execution_duration = int((dt.datetime.strptime(str(self.execution_end_time),'%Y-%m-%d %H:%M:%S') - dt.datetime.strptime(str(self.execution_start_time), '%Y-%m-%d %H:%M:%S')).total_seconds())
            landing_count = self.landing_count
            raw_count = self.raw_count
            reject_count = self.reject_count
            total_count = self.raw_count + self.reject_count
            log_timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            source_name = str(self.file_name)
            destination_name = self.final_output_file_name if self.file_name.split('.')[-1].lower() in ('xlsm', 'xlsb', 'xls', 'xlsx', 'csv') else self.scits_output_file_name

            # RDS insert query
            rds_insert_query = f"""
                                    INSERT INTO audit.sc360_audit_log 
                                            (batchid, batchrundate, regionname, datapipelinename, pipelinerunid, processname, filename, sourceplatform, targetplatform, activity, 
                                            environment, scriptpath, executionstatus, errormessage, executionstarttime, executionendtime, executiondurationinseconds, landingcount, 
                                            rawcount, rejectcount, totalcount, logtimestamp, sourcename, destinationname)
                                            VALUES ('{batch_id}', '{batch_run_date}', '{region_name}', '{data_pipeline_name}', '{data_pipeline_run_id}', '{process_name}',
                                                    '{file_name}', '{source_platform}', '{target_platform}', '{activity}', '{environment}', '{script_path}', '{execution_status}',
                                                    '{error_message}', '{execution_start_time}', '{execution_end_time}', {execution_duration}, {landing_count}, 
                                                    {raw_count}, {reject_count}, {total_count}, '{log_timestamp}', '{source_name}', '{destination_name}');
                                """
            
            client = boto3.client('rds-data')
            #response_data = client.execute_statement(resourceArn=self.resourceArn, secretArn=self.secretArn, database=self.database, sql=rds_insert_query)
            
            max_retries = 3
            # Retry logic for insert query
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"[Attempt {attempt}] Executing RDS Insert SQL")
                    response_data = client.execute_statement(
                        resourceArn=self.resourceArn,
                        secretArn=self.secretArn,
                        database=self.database,
                        sql=rds_insert_query
                    )
                    break
                except Exception as e:
                    print(f"[Attempt {attempt}] RDS query failed: {e}")
                    if attempt == max_retries:
                        raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                    time.sleep(3 * attempt)

            # Update the FileValidation process status
            rds_update_query = f"""UPDATE audit.sc360_audit_log SET executionstatus = '{execution_status}' WHERE batchid = '{batch_id}' 
                                                                AND batchrundate = '{batch_run_date}' 
                                                                AND processname = 'FileValidation' 
                                                                AND filename = '{file_name}' 
                                                                AND executionstatus not in('FileValidationFailed');
                                """

            #response_data = client.execute_statement(resourceArn=self.resourceArn, secretArn=self.secretArn, database=self.database, sql=rds_update_query)
            
            # Retry logic for update query
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"[Attempt {attempt}] Executing RDS Update SQL")
                    response_data = client.execute_statement(
                        resourceArn=self.resourceArn,
                        secretArn=self.secretArn,
                        database=self.database,
                        sql=rds_update_query
                    )
                    break
                except Exception as e:
                    print(f"[Attempt {attempt}] RDS query failed: {e}")
                    if attempt == max_retries:
                        raise RuntimeError(f"RDS query failed after {max_retries} attempts") from e
                    time.sleep(3 * attempt)

            self.dq_logging.info('{}, Audit log table updated successfully.'.format(self.file_name))
        except Exception as e:
            self.exception_error(e,"update_audit_log", "", update_audit=False)

    # Method to get the Exception Description
    def exc_description_func(self, e):
        try:
            exc_description = str(e).translate(str.maketrans('', '', '<\'>\"",\n\\n')).replace('\n', '')
            return exc_description
        except Exception as e:
            self.exception_error(e,"exc_description_func", "")

    # Method to get the Exception type
    def exc_type_func(self, exc_type):
        try:
            exc_type = str(exc_type).translate(str.maketrans('', '', '<\'>\"",\n\\n')).replace('\n', '')
            return exc_type
        except Exception as e:
            self.exception_error(e,"exc_type_func", "")

    def decimal_columns_calculation(self):
        try:
            if len(self.decimal_cal_columns) > 0:
                for each_column in self.decimal_cal_columns:
                    for cname, round_val in each_column.items():
                        # Multiply the column by 100 and round it (if necessary) in a single step
                        temp_col = (col(cname) * 100)
                        
                        if round_val != -1:
                            self.input_file_df = self.input_file_df.withColumn(cname, F.round(temp_col, round_val))
                        else:
                            self.input_file_df = self.input_file_df.withColumn(cname, F.ceil(temp_col))

            self.dq_logging.info('{}, Cast to Integer operation completed successfully'.format(self.file_name))
        except Exception as e:
            self.exception_error(e,"decimal_columns_calculation", "")

    # Method to get the Exception details info
    def frameName_func(self, exc_tb):
        try:
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            frameName = str(fname).translate(str.maketrans('', '', '<\'>command-\"\n\\n",'))
            return frameName
        except Exception as e:
            self.exception_error(e,"frameName_func", "")

    def exception_error(self, e, function_name, method="", update_audit=True):
        try:
            # Log the error at the beginning
            self.dq_logging.error(f"Exception caught while performing the {function_name} in method {method}.")
            
            # Update execution status and error message
            self.dv_execution_status = 'Failed'
            self.execution_end_time = time.strftime('%Y-%m-%d %H:%M:%S')
            self.error_message = f"{str(e)} - Exception caught while performing the {function_name} function."
            
            # Optionally update audit log
            if update_audit:
                self.update_audit_log()

            # Capture the exception traceback for logging
            exc_type, exc_value, exc_tb = sys.exc_info()
            file_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            
            # Log detailed error information
            self.dq_logging.error(f"Exception in {function_name}: {traceback.format_exc()}")
            self.dq_logging.error(f"Error message: {e}")

            # Raise a more informative exception
            raise Exception(f"Exception in {function_name} while reading file {self.file_name}: {e}")
        
        except Exception as inner_e:
            # In case of any failure in the exception handling itself, log and raise the inner exception
            self.dq_logging.error(f"Error occurred while handling exception in {function_name}: {str(inner_e)}")
            raise inner_e

    
