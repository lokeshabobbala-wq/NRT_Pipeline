import sys
import os
import json
import boto3
import traceback
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, sha2, col, lit, current_timestamp
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

from logging_util import Logging
from file_validation import FileValidator
from data_validation import DataValidator
from sql_generator import SCD2SQLGenerator, SNAPSHOTSQLGenerator
from custom_utility_module import deduplicate_egi_2_0_inc

import ast
import datetime
from typing import Dict, Any, Optional, Tuple

def get_redshift_secret(secret_name, region_name="us-east-1"):
    secrets_client = boto3.client('secretsmanager', region_name=region_name)
    response = secrets_client.get_secret_value(SecretId=secret_name)
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        creds = ast.literal_eval(response['SecretString'])
        dbname = creds.get("redshift_database") or creds.get("engine")
        port = creds.get("redshift_port") or creds.get("port")
        username = creds.get("redshift_username") or creds.get("username")
        password = creds.get("redshift_password") or creds.get("password")
        host = creds.get("redshift_host") or creds.get("host")
        return {
            "dbname": dbname,
            "port": port,
            "username": username,
            "password": password,
            "host": host
        }
    else:
        raise Exception(f"Failed to retrieve credentials for {secret_name}")

def insert_glue_audit_log_rdsdata(rds_client, resourceArn, secretArn, database, audit_data: dict):
    """
    Centralized audit log insert. Only pass changed variables in audit_data.
    """
    import datetime
    logtimestamp = datetime.datetime.utcnow()

    sql = """
        INSERT INTO audit.sc360_audit_log_nrt_bkp (
            batchid, batchrundate, regionname, datapipelinename, pipelinerunid, processname, filename,
            sourceplatform, targetplatform, activity, environment, "interval", scriptpath,
            executionstatus, errormessage, executionstarttime, executionendtime, executiondurationinseconds,
            datareadsize, datawrittensize, landingcount, rawcount, rejectcount, previouscount, updatedcount,
            insertedcount, totalcount, logtimestamp, jobid, clusterid, sourcename, destinationname,
            expected_start_time, expected_end_time, freshness, data_refresh_mode
        ) VALUES (
            :batchid, :batchrundate::date, :regionname, :datapipelinename, :pipelinerunid, :processname, :filename,
            :sourceplatform, :targetplatform, :activity, :environment, :interval, :scriptpath,
            :executionstatus, :errormessage, :executionstarttime::timestamp, :executionendtime::timestamp, :executiondurationinseconds,
            :datareadsize, :datawrittensize, :landingcount, :rawcount, :rejectcount, :previouscount, :updatedcount,
            :insertedcount, :totalcount, :logtimestamp::timestamp, :jobid, :clusterid, :sourcename, :destinationname,
            :expected_start_time::timestamp, :expected_end_time::timestamp, :freshness, :data_refresh_mode
        )
    """

    all_fields = [
        'batchid', 'batchrundate', 'regionname', 'datapipelinename', 'pipelinerunid',
        'processname', 'filename', 'sourceplatform', 'targetplatform', 'activity',
        'environment', 'interval', 'scriptpath', 'executionstatus', 'errormessage',
        'executionstarttime', 'executionendtime', 'executiondurationinseconds', 'datareadsize',
        'datawrittensize', 'landingcount', 'rawcount', 'rejectcount', 'previouscount',
        'updatedcount', 'insertedcount', 'totalcount', 'logtimestamp', 'jobid', 'clusterid',
        'sourcename', 'destinationname', 'expected_start_time', 'expected_end_time',
        'freshness', 'data_refresh_mode'
    ]

    def _param(name, value):
        # Only set type if required, otherwise string
        if value is None:
            return {'name': name, 'value': {'isNull': True}}
        if isinstance(value, float):
            return {'name': name, 'value': {'doubleValue': float(value)}}
        if isinstance(value, int):
            return {'name': name, 'value': {'longValue': int(value)}}
        return {'name': name, 'value': {'stringValue': str(value)}}

    params = [_param(field, audit_data.get(field)) for field in all_fields]

    try:
        response = rds_client.execute_statement(
            resourceArn=resourceArn,
            secretArn=secretArn,
            database=database,
            sql=sql,
            parameters=params
        )
        print("RDS Data API Response:")
        print(response)
    except Exception as e:
        print(f"Failed to insert Glue audit log via RDS Data API: {e}")

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

def apply_column_mapping_with_withColumn(df, mapping):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    fiscal_quarter_udf = udf(fiscal_quarter_from_date, StringType())
    mapped_df = df
    for tgt, spec in mapping.items():
        if "source" in spec:
            mapped_df = mapped_df.withColumn(tgt, col(spec["source"]))
        elif "lit" in spec:
            mapped_df = mapped_df.withColumn(tgt, lit(spec["lit"]))
        elif "concat_ws" in spec:
            sep = spec["concat_ws"].get("sep", "")
            cols = spec["concat_ws"]["columns"]
            mapped_df = mapped_df.withColumn(tgt, concat_ws(sep, *[col(c) for c in cols]))
        elif "function" in spec and spec["function"] == "fiscal_quarter_from_date":
            mapped_df = mapped_df.withColumn(tgt, fiscal_quarter_udf(col(spec["args"][0])))
    return mapped_df

class NRTGlueJob:
    def __init__(self):
        self.args = self._parse_args()
        self.spark = self._get_spark_session()
        self.logger = self._get_logger()
        self.full_config = self._load_full_config()
        self.config = self._load_config_from_s3(self.args['CONFIG_S3_PATH'])
        self.s3_client = boto3.client('s3')
        self.clusteridentifier = self.args['clusteridentifier']
        self.redshiftdatabase = self.args['redshiftdatabase']
        self.redshiftuser = self.args['redshiftuser']
        self.redshiftsecret_arn = self.args['redshiftsecret_arn']
        self.redshiftclient = boto3.client('redshift-data')
        self.rds_client = boto3.client('rds-data')
        self.resourcearn = self.args['resourcearn']
        self.secretarn = self.args['secretarn']
        self.database = self.args['database']
        self.redshift_url = self.args.get("redshift_url")
        self.redshift_secret_name = self.args.get("redshift_secret_name")

        # Audit context defaults
        self.audit_context = self._default_audit_context()

    @staticmethod
    def _parse_args():
        return getResolvedOptions(
            sys.argv,
            [
                'S3_BUCKET', 'MAIN_KEY', 'METADATA_KEY', 'CONFIG_S3_PATH',
                'CURATED_TABLE', 'PUBLISHED_TABLE', 'FULL_CONFIG',
                'database', 'env', 'resourcearn',
                'schema', 'secretarn', 'sns_arn', 'KMS_ID', 'redshift_secret_name',
                'redshiftsecret_arn', 'redshiftdatabase', 'clusteridentifier', 'redshiftuser', 'rds_secret_name', 'redshift_url'
            ]
        )

    @staticmethod
    def _get_spark_session():
        spark = (
            SparkSession.builder
            .appName("DQ")
            .config('spark.sql.codegen.wholeStage', 'false')
            .getOrCreate()
        )
        spark.conf.set("spark.sql.shuffle.partitions", 20)
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        return spark

    def _get_logger(self):
        return Logging(
            spark_context=self.spark,
            app_name="NRT_PIPELINE",
            log_file_name="NRTGluejob_dataload.log"
        )

    def _load_full_config(self):
        return json.loads(self.args['FULL_CONFIG'])

    @staticmethod
    def _load_config_from_s3(config_s3_path):
        s3 = boto3.client('s3')
        bucket, key = config_s3_path.replace("s3://", "").split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read())

    def _send_notification(self, error_message):
        try:
            sns = boto3.client('sns')
            sns.publish(
                TargetArn=self.args['sns_arn'],
                Message=error_message,
                Subject='NRT Data Validation Failure'
            )
            self.logger.info("SNS notification sent.")
        except Exception as exc:
            self.logger.error(f"Failed to send SNS notification: {exc}")
            self.logger.error(traceback.format_exc())

    def _get_entity_config(self):
        entity_name = os.path.splitext(os.path.basename(self.args['MAIN_KEY']))[0].split('_')[0]
        entity_config_key = None
        for k in self.config:
            if self.args['MAIN_KEY'].split('/')[-1].startswith(k):
                entity_config_key = k
                break
        if not entity_config_key:
            entity_config_key = list(self.config.keys())[0]
        return self.config[entity_config_key]

    def _list_s3_keys(self, prefix):
        all_objects = self.s3_client.list_objects_v2(Bucket=self.args['S3_BUCKET'], Prefix=prefix)
        contents = all_objects.get('Contents', [])
        all_data_keys = [
            obj['Key'] for obj in contents
            if not obj['Key'].endswith('/') and not os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]
        all_metadata_keys = [
            obj['Key'] for obj in contents
            if os.path.basename(obj['Key']).startswith('SC360metadata_')
        ]
        return all_data_keys, all_metadata_keys

    def _load_pandas_df(self, decrypted_bytes, expected_extension, entity_config):
        if expected_extension.lower() == "csv":
            df = pd.read_csv(StringIO(decrypted_bytes.decode("utf-8")))
        elif expected_extension.lower() in ("xlsx", "xls"):
            excel_engine = entity_config.get("excel_engine", "openpyxl")
            df = pd.read_excel(BytesIO(decrypted_bytes), dtype=str, engine=excel_engine)
        else:
            raise Exception(f"Unsupported extension: {expected_extension}")
        return df

    def _rename_and_clean_pandas_df(self, pandas_df, column_type_map):
        pandas_df.columns = list(column_type_map.keys())
        pandas_df = pandas_df.replace({np.nan: None, pd.NaT: None})
        return pandas_df

    def _execute_redshift_sql(self, sql):
        try:
            response = self.redshiftclient.execute_statement(
                ClusterIdentifier=self.clusteridentifier,
                Database=self.redshiftdatabase,
                SecretArn=self.redshiftsecret_arn,
                Sql=sql,
                WithEvent=True
            )
            print(f"Redshift SQL executed: {sql}")
            print(f"Redshift response: {response}")
            return response
        except Exception as e:
            print(f"Redshift SQL execution error: {e}")
            raise

    def _write_to_redshift(self, spark_df, table_options):
        sc = SparkContext.getOrCreate()
        glue_context = GlueContext(sc)
        dyf = DynamicFrame.fromDF(spark_df, glue_context, "dyf_final")
        glue_context.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="redshift",
            connection_options=table_options
        )
        print("Data written to Redshift table successfully.")
        
    @staticmethod
    def extract_file_info(main_key):
        """
        Extracts file details from a given S3 key or file path.
        Returns: main_file, main_file_no_ext, logical_file_name
        """
        main_file = os.path.basename(main_key)
        main_file_no_ext = os.path.splitext(main_file)[0]
        logical_file_name = main_file_no_ext.rsplit('_', 1)[0]
        return main_file, main_file_no_ext, logical_file_name
        
    def _default_audit_context(self) -> dict:
        today = datetime.date.today()
        main_file, main_file_no_ext, logical_file_name = self.extract_file_info(self.args['MAIN_KEY'])
        return {
            "batchid": f"WW_Batch_{today.strftime('%Y%m%d')}",
            "batchrundate": today,
            "regionname": "WW",
            "datapipelinename": "NRT_Glue_DataPipeline",
            "pipelinerunid": self.args.get("JOB_RUN_ID") or os.environ.get("JOB_RUN_ID") or "",
            "activity": "NRT_Data_Load",
            "environment": self.args['env'],
            "interval": "",
            "scriptpath": os.path.abspath(__file__),
            "jobid": self.args.get("JOB_RUN_ID")[:50] or os.environ.get("JOB_RUN_ID")[:50] or "",
            "clusterid": self.clusteridentifier,
            "freshness": None,
            "logical_file_name": logical_file_name,
            "filename": main_file,
            "sourcename": main_file,
            "destinationname": main_file,
            "data_refresh_mode": "NRT"
        }
    
    def file_validation(self, entity_config, audit) -> dict:
        """File validation step."""
        result = {"executionstatus": "Success", "errormessage": None}
        try:
            allowed_prefixes = entity_config.get("allowed_prefixes", [""])
            expected_extension = entity_config.get("input_file_extension", "csv")
            rds_secret_name = self.args['secretarn']
            kms_id = self.args['KMS_ID']

            landing_prefix = os.path.dirname(self.args['MAIN_KEY'])
            all_data_keys, all_metadata_keys = self._list_s3_keys(landing_prefix)

            validator = FileValidator(
                bucket=self.args['S3_BUCKET'],
                region=audit["regionname"],
                logger=self.logger,
                allowed_prefixes=allowed_prefixes,
                rds_secret_name=rds_secret_name,
                expected_extension=expected_extension,
                kms_id=kms_id
            )

            file_key = self.args['MAIN_KEY']
            metadata_key = self.args['METADATA_KEY']

            start_time = datetime.datetime.utcnow()
            valid, decrypted_bytes = validator.validate_file(
                file_key, metadata_key, entity_config,
                all_data_keys=all_data_keys, all_metadata_keys=all_metadata_keys
            )
            read_size = validator.s3.head_object(Bucket=self.args['S3_BUCKET'], Key=file_key)['ContentLength'] if validator.s3 and file_key else None

            if not valid:
                result.update({
                    "executionstatus": "Failed",
                    "errormessage": f"File validation failed for {file_key}. See log for details."
                })
                self._send_notification(result["errormessage"])
                decrypted_bytes = None
            end_time = datetime.datetime.utcnow()
            result.update({
                "executionstarttime": start_time,
                "executionendtime": end_time,
                "executiondurationinseconds": (end_time - start_time).total_seconds(),
                "datareadsize": read_size,
                "datawrittensize": read_size,
                "decrypted_bytes": decrypted_bytes
            })
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            return {
                "executionstatus": "Failed",
                "errormessage": f"Exception during file validation: {e}",
                "executionstarttime": None,
                "executionendtime": end_time,
                "executiondurationinseconds": None,
                "datareadsize": None,
                "datawrittensize": None,
                "decrypted_bytes": None
            }

    def data_validation(self, entity_config, audit, decrypted_bytes) -> dict:
        """Data validation step."""
        result = {"executionstatus": "Success", "errormessage": None}
        try:
            start_time = datetime.datetime.utcnow()
            pandas_df = self._load_pandas_df(decrypted_bytes, entity_config.get("input_file_extension", "csv"), entity_config)
            pandas_df = self._rename_and_clean_pandas_df(pandas_df, entity_config.get("column_type_map", {}))
            spark_df = self.spark.createDataFrame(pandas_df)

            landing_count = pandas_df.shape[0]
            dq_validator = DataValidator(
                df=spark_df,
                config=entity_config,
                logger=self.logger,
                file_name=audit["filename"]
            )
            clean_df = dq_validator.run_all_checks()

            main_file, main_file_no_ext, logical_file_name = self.extract_file_info(self.args['MAIN_KEY'])
            self.logger.info(f"Row count before deduplication for {logical_file_name}: {clean_df.count()}")
            if logical_file_name == "EGI_2_0_INC":
                clean_df = deduplicate_egi_2_0_inc(clean_df)
                self.logger.info(f"Row count after deduplication for {logical_file_name}: {clean_df.count()}")
                
            audit_info = dq_validator.get_audit_info()

            if clean_df is None:
                result.update({
                    "executionstatus": "Failed",
                    "errormessage": f"File rejected during data validation: {audit_info}",
                    "landingcount": landing_count,
                    "rawcount": None,
                    "rejectcount": landing_count
                })
                self._send_notification(result["errormessage"])
                end_time = datetime.datetime.utcnow()
            else:
                raw_count = clean_df.count()
                reject_count = landing_count - raw_count
                end_time = datetime.datetime.utcnow()
                result.update({
                    "landingcount": landing_count,
                    "rawcount": raw_count,
                    "rejectcount": reject_count,
                    "final_df": self._map_columns(entity_config, clean_df, audit)
                })
            result.update({
                "executionstarttime": start_time,
                "executionendtime": end_time,
                "executiondurationinseconds": (end_time - start_time).total_seconds()
            })
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            return {
                "executionstatus": "Failed",
                "errormessage": f"Exception during data validation: {e}",
                "executionstarttime": None,
                "executionendtime": end_time,
                "executiondurationinseconds": None
            }

    def _map_columns(self, entity_config, clean_df, audit):
        """Apply column mapping and record hash columns."""
        column_mapping = entity_config.get("column_mapping", {})
        mapped_df = apply_column_mapping_with_withColumn(clean_df, column_mapping)
        hash_columns = entity_config.get("hash_columns", [])
        hash_column_name = entity_config.get("hash_column_name", "record_hash")
        source_file_name = audit["filename"]
        if hash_columns:
            return (
                mapped_df
                .withColumn(hash_column_name, sha2(concat_ws("|", *[col(c) for c in hash_columns]), 256))
                .withColumn("source_file_name", lit(source_file_name))
                .withColumn("batch_run_dt", lit(current_timestamp()).cast(TimestampType()))
                .withColumn("created_by", lit("glue"))
                .withColumn("create_dt", lit(current_timestamp()).cast(TimestampType()))
                .withColumn("source_nm", lit("sc360-dev-nrt-dataload-processor"))
            )
        else:
            self.logger.warning("No hash_columns defined in config; hash_code not generated.")
            return mapped_df

    def load_curated_to_redshift(self, entity_config, audit, final_df) -> dict:
        """Load curated data to Redshift."""
        result = {"executionstatus": "Success", "errormessage": None}
        try:
            start_time = datetime.datetime.utcnow()
            curated_table = entity_config.get('curated_table', 'curated.CUR_REVENUE_EGI_NRT')
            self._execute_redshift_sql(f"TRUNCATE TABLE {curated_table};")
            secret = get_redshift_secret(self.redshift_secret_name)
            main_file, main_file_no_ext, logical_file_name = self.extract_file_info(self.args['MAIN_KEY'])
            today_date_str = datetime.date.today().strftime('%Y-%m-%d')
            redshift_options = {
                "url": self.redshift_url,
                "user": self.redshiftuser,
                "password": secret["password"],
                "dbtable": curated_table,
                "redshiftTmpDir": f"s3://sc360-dev-ww-nrt-bucket/staging/dt={today_date_str}/{logical_file_name}/{main_file_no_ext}/",
                "mode": "overwrite",
            }
            self._write_to_redshift(final_df, redshift_options)
            total_count = final_df.count()
            end_time = datetime.datetime.utcnow()
            result.update({
                "executionstarttime": start_time,
                "executionendtime": end_time,
                "executiondurationinseconds": (end_time - start_time).total_seconds(),
                "totalcount": total_count
            })
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            return {
                "executionstatus": "Failed",
                "errormessage": f"Exception during curated Redshift load: {e}",
                "executionstarttime": None,
                "executionendtime": end_time,
                "executiondurationinseconds": None
            }

    def load_published_to_redshift(self, entity_config, audit) -> dict:
        """
        Load published data to Redshift according to load_type (scd2 or snapshot).
        """
        result = {"executionstatus": "Success", "errormessage": None}
        try:
            start_time = datetime.datetime.utcnow()
            load_type = entity_config.get("load_type", "scd2").lower()  # <--- READ load_type HERE
            hash_columns = entity_config.get("hash_columns", [])
            primary_keys = entity_config.get("primary_key_columns", [])
            target_table = entity_config.get("published_table", "published.r_revenue_egi_nrt")
            staging_table = entity_config.get("curated_table", "curated.CUR_REVENUE_EGI_NRT")
            
            if load_type == "scd2":
                sql_gen = SCD2SQLGenerator(
                    target_table=target_table,
                    staging_table=staging_table,
                    primary_key_columns=primary_keys,
                    hash_columns=hash_columns,
                    surrogate_key="r_revenue_egi_nrt_id"
                )
                self._execute_redshift_sql(sql_gen.generate_update_sql())
                self._execute_redshift_sql(sql_gen.generate_insert_sql())
            elif load_type == "snapshot":
                sql_gen = SNAPSHOTSQLGenerator(
                    target_table=target_table,
                    staging_table=staging_table,
                    primary_key_columns=primary_keys,
                )
                self._execute_redshift_sql(sql_gen.generate_update_sql())
                self._execute_redshift_sql(sql_gen.generate_insert_sql())
            else:
                raise Exception(f"Unknown load_type: {load_type}")
    
            end_time = datetime.datetime.utcnow()
            result.update({
                "executionstarttime": start_time,
                "executionendtime": end_time,
                "executiondurationinseconds": (end_time - start_time).total_seconds()
            })
            return result
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            return {
                "executionstatus": "Failed",
                "errormessage": f"Exception during published Redshift load: {e}",
                "executionstarttime": None,
                "executionendtime": end_time,
                "executiondurationinseconds": None
            }

    def audit_log(self, audit, process_name):
        """Centralized audit log call."""
        audit["processname"] = process_name
    
        # Truncate error_message to 2000 characters if present
        if "errormessage" in audit and audit["errormessage"]:
            audit["errormessage"] = str(audit["errormessage"])[:2000]
            # Print error to application log
            self.logger.error(f"AUDIT ERROR ({process_name}): {audit['errormessage']}")
    
        insert_glue_audit_log_rdsdata(
            self.rds_client, self.resourcearn, self.secretarn, self.database, audit
        )
        
    def archive_and_cleanup_s3_files(self, main_filename: str, metadata_filename: str):
        """
        Archives and deletes the provided main and metadata file from LandingZone to ArchiveZone, partitioned by date and logical name.
        Handles exceptions and logs any errors.
        """
        s3 = self.s3_client
        bucket = self.args['S3_BUCKET']
        landing_prefix = "LandingZone/"
        archive_prefix = "ArchiveZone/"
        today = datetime.date.today()
        dt_str = today.strftime("dt=%Y-%m-%d")
        logical_name = main_filename.rsplit('_', 1)[0]
    
        for filename in [main_filename, metadata_filename]:
            landing_key = os.path.join(landing_prefix, filename)
            archive_key = f"{archive_prefix}{dt_str}/{logical_name}/{filename}"
            try:
                # Copy to archive
                s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': landing_key}, Key=archive_key)
                self.logger.info(f"Copied {landing_key} to {archive_key} in archive.")
            except Exception as e:
                self.logger.error(f"Failed to archive {landing_key} to {archive_key}: {e}")
                continue  # Optionally, you can choose to exit or raise here
    
            try:
                # Delete from landing
                s3.delete_object(Bucket=bucket, Key=landing_key)
                self.logger.info(f"Deleted {landing_key} from landing.")
            except Exception as e:
                self.logger.error(f"Failed to delete {landing_key} from landing after archiving: {e}")
                
    def main(self):
        entity_config = self._get_entity_config()
        audit = self.audit_context.copy()
        try:
            # Step 1: File Validation
            file_val_result = self.file_validation(entity_config, audit)
            audit.update(file_val_result)
            self.audit_log(audit, "FileValidation")
            if audit['executionstatus'] == "Failed":
                return
    
            # Step 2: Data Validation
            data_val_result = self.data_validation(entity_config, audit, file_val_result["decrypted_bytes"])
            audit.update(data_val_result)
            self.audit_log(audit, "DataValidation")
            if audit['executionstatus'] == "Failed":
                return
            
            # Step 3: Redshift Curated Load
            curated_result = self.load_curated_to_redshift(entity_config, audit, data_val_result["final_df"])
            audit.update(curated_result)
            self.audit_log(audit, "RedshiftCuratedLoad")
            if audit['executionstatus'] == "Failed":
                return
            
            # Step 4: Redshift Published Load
            published_result = self.load_published_to_redshift(entity_config, audit)
            audit.update(published_result)
            self.audit_log(audit, "RedshiftPublishedLoad")
            if audit['executionstatus'] == "Failed":
                return
            
            # After all steps and audit logs
            main_filename = os.path.basename(self.args['MAIN_KEY'])
            metadata_filename = os.path.basename(self.args['METADATA_KEY'])
            self.archive_and_cleanup_s3_files(main_filename, metadata_filename)
            print("Glue job completed successfully, and files archived and cleaned up.")
            
        except Exception as exc:
            error_message = f"Exception during Glue job: {str(exc)}"
            self.logger.error(error_message)
            self.logger.error(traceback.format_exc())
            self._send_notification(error_message)
            audit.update({
                "executionstatus": "Failed",
                "errormessage": error_message
            })
            self.audit_log(audit, "Failure")

if __name__ == "__main__":
    job = NRTGlueJob()
    job.main()
