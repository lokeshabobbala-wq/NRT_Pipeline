"""
file_validation.py

File validation package for S3 data pipeline, to be imported into Glue jobs.

Implements all validation steps as methods on FileValidator, following the
coding standards and docstring/logging/error-handling style of sc360-dev-nrt-dataload-trigger.py.
"""

import os
import boto3
import csv
import codecs
import datetime as dt
import ast
from botocore.exceptions import ClientError
from kms_decrypt_util import KMSDecryptionHelper
import aws_encryption_sdk
from aws_encryption_sdk.identifiers import CommitmentPolicy
from aws_encryption_sdk.key_providers.kms import StrictAwsKmsMasterKeyProvider

class FileValidator:
    """
    Class for validating files landed in S3 as per pipeline checklist.

    Usage:
        validator = FileValidator(bucket, region, logger, allowed_prefixes, rds_secret_name, expected_extension)
        result = validator.validate_file(file_key, metadata_key, file_config)
    """

    def __init__(self, bucket, region, logger, allowed_prefixes, rds_secret_name, expected_extension, kms_id=None, max_sla_hours=2):
        """
        Initialize FileValidator.

        Args:
            bucket (str): S3 bucket name.
            region (str): Region name.
            logger: logger object (your custom Logging instance).
            allowed_prefixes (list): Expected file prefixes for name validation.
            rds_secret_name (str): Secret name for RDS credentials (for audit logging).
            expected_extension (str): Allowed file extension (e.g. 'csv').
            max_sla_hours (int): Time window for SLA check.
        """
        self.bucket = bucket
        self.region = region
        self.logger = logger
        self.allowed_prefixes = allowed_prefixes
        self.expected_extension = expected_extension
        self.max_sla_hours = max_sla_hours
        self.kms_id = kms_id
        self.s3 = boto3.client('s3')
        self.rds_secret_name = rds_secret_name

    def check_file_presence(self, key):
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            self.logger.info(f"File found: s3://{self.bucket}/{key}")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                self.logger.error(f"File not found: s3://{self.bucket}/{key}")
                return False
            self.logger.error(f"Error in head_object: {e}")
            raise

    def check_file_naming_pattern(self, filename):
        is_valid = any(filename.startswith(prefix) for prefix in self.allowed_prefixes)
        self.logger.info(f"Naming pattern check for {filename}: {is_valid}")
        return is_valid

    def check_file_size(self, key):
        """
        Check the file size using list_objects_v2 for a single file.
        """
        try:
            # Use list_objects_v2 to get metadata for files under the given prefix (key)
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)

            # Validate if the file exists
            if 'Contents' not in response or len(response['Contents']) == 0:
                self.logger.error(f"File {key} not found in bucket {self.bucket}.")
                return False

            # Get the file size from the first (and expected only) object in the response
            file_size = response['Contents'][0]['Size']
            self.logger.info(f"File {key} size: {file_size}")
            
            # Return True if the file size is greater than 0
            return file_size > 0

        except Exception as e:
            self.logger.error(f"Error getting file size for {key}: {e}")
            return False


    def check_file_extension(self, filename):
        ext = filename.split('.')[-1].lower()
        is_valid = ext == self.expected_extension.lower()
        self.logger.info(f"Extension check: {ext} vs allowed {self.expected_extension.lower()} = {is_valid}")
        return is_valid

    def check_kms_encryption(self, key):
        resp = self.s3.head_object(Bucket=self.bucket, Key=key)
        enc = resp.get('ServerSideEncryption')
        self.logger.info(f"KMS encryption: {enc}")
        return enc == 'aws:kms'

    def check_file_integrity(self, key):
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            next(csv.reader(codecs.getreader("utf-8")(obj["Body"])))
            self.logger.info(f"File {key} opened successfully (integrity pass).")
            return True
        except Exception as e:
            self.logger.error(f"Failed to open file {key}: {e}")
            return False

    def check_timeliness(self, key):
        import datetime
        last_modified = self.s3.head_object(Bucket=self.bucket, Key=key)['LastModified']
        now = datetime.datetime.utcnow().replace(tzinfo=last_modified.tzinfo)
        delta = (now - last_modified).total_seconds() / 3600
        is_valid = delta <= self.max_sla_hours
        self.logger.info(f"Timeliness check for {key}: {delta:.2f} hours old (valid={is_valid})")
        return is_valid

    def find_metadata_file(self, data_filename, metadata_files):
        """
        Return the matching metadata file for a given data file, or None.
        """
        base = data_filename.split('.')[0]
        for m in metadata_files:
            if m.startswith(f"SC360metadata_{base}"):
                self.logger.info(f"Matching metadata file for {data_filename}: {m}")
                return m
        self.logger.error(f"No metadata found for {data_filename}")
        return None
    
    def check_file_size_matches_metadata(self, data_key, metadata_key):
        """
        Check that decrypted file size matches the value specified in the metadata file.
        Handles KMS-encrypted data and metadata files if necessary.
        """
        try:
            # Step 1: Decrypt metadata file if KMS encrypted
            plaintext_meta = self.check_kms_encryption_and_decrypt(metadata_key)
            if plaintext_meta is None:
                m_obj = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
                raw_content = m_obj["Body"].read().decode("utf-8")
            else:
                raw_content = plaintext_meta.decode("utf-8")

            # Parse metadata CSV
            import csv
            metadataInfo = list(csv.reader(raw_content.splitlines()))
            metadataInfo = [row for row in metadataInfo if row]
            if not metadataInfo:
                self.logger.error("Metadata file is empty or malformed.")
                return False

            size_from_meta = int(metadataInfo[0][0].split('|')[1])

            # Step 2: Decrypt data file if KMS encrypted
            plaintext_data = self.check_kms_encryption_and_decrypt(data_key)
            if plaintext_data is None:
                # Not encrypted, fetch directly
                d_obj = self.s3.get_object(Bucket=self.bucket, Key=data_key)
                data_bytes = d_obj["Body"].read()
            else:
                data_bytes = plaintext_data

            # Step 3: Compare sizes
            actual_size = len(data_bytes)
            self.logger.info(f"File size from metadata: {size_from_meta}, decrypted file size: {actual_size}")
            return actual_size == size_from_meta

        except Exception as e:
            self.logger.error(f"Error checking file size against metadata: {e}")
            return False


    def check_duplicates(self, file_list):
        """
        Check for duplicate data files by base name (excluding timestamp).
        """
        seen = set()
        for f in file_list:
            base = "_".join(f.split('/')[-1].split('_')[:-1])
            if base in seen:
                self.logger.error("Duplicate file found: " + f)
                return True
            seen.add(base)
        self.logger.info("No duplicates found.")
        return False

    def check_kms_encryption_and_decrypt(self, key):
        """
        If file is KMS encrypted, decrypt and return plaintext bytes.
        If not, returns None.
        """
        if not self.kms_id:
            self.logger.info("No KMS_ID provided, skipping decryption.")
            return None
        try:
            self.logger.info(f"Attempting to decrypt file {key} using KMS_ID: {self.kms_id}")
            decrypt_helper = KMSDecryptionHelper(self.kms_id, self.region)
            plaintext = decrypt_helper.decrypt_s3_object(self.bucket, key, is_base64=True)
            self.logger.info(f"Decryption successful for {key}")
            return plaintext
        except Exception as e:
            self.logger.error(f"Decryption failed for {key}: {e}")
            return None
    
    def validate_file(self, file_key, metadata_key, file_config, all_data_keys=None, all_metadata_keys=None):
        filename = file_key.split('/')[-1]
        self.logger.info(f"Starting validation for file: {file_key}")

        if not self.check_file_presence(file_key):
            self.logger.error("File presence check failed.")
            return False

        if not self.check_file_naming_pattern(filename):
            self.logger.error("File naming pattern check failed.")
            return False

        # Matching Metadata File Exists
        if not metadata_key and all_metadata_keys is not None:
            metadata_key = self.find_metadata_file(filename, all_metadata_keys)
            if not metadata_key:
                self.logger.error("Matching metadata file check failed.")
                return False
        elif not metadata_key:
            self.logger.error("No metadata key provided for metadata existence check.")
            return False

        if not self.check_file_size(file_key):
            self.logger.error("File size check failed (empty file).")
            return False
        
        # File Size Matches Metadata
        if not self.check_file_size_matches_metadata(file_key, metadata_key):
            self.logger.error("File size matches metadata check failed.")
            return False
        
        if not self.check_file_extension(filename):
            self.logger.error("File extension/type check failed.")
            return False

        # KMS DECRYPTION/VALIDATION
        decrypted_bytes = None
        try:
            decrypted_bytes = self.check_kms_encryption_and_decrypt(file_key)
        except Exception as e:
            self.logger.error(f"KMS decryption failed: {e}")
            return False

        # File integrity: try to open as CSV -- use decrypted bytes if present
        try:
            if decrypted_bytes:
                csv.reader(codecs.iterdecode(decrypted_bytes.splitlines(), "utf-8")).__next__()
            else:
                self.check_file_integrity(file_key)
        except Exception as e:
            self.logger.error(f"File integrity check failed: {e}")
            return False
        
        if not self.check_timeliness(file_key):
            self.logger.error("File timeliness/SLA check failed.")
            return False
        
        # Duplicate Check
        if all_data_keys is not None and self.check_duplicates(all_data_keys):
            self.logger.error("Duplicate check failed.")
            return False
        
        self.logger.info(f"All validation checks passed for {file_key}")
        return True
