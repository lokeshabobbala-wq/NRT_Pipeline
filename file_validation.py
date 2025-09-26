"""
file_validation.py

Optimized and modular file validation for S3 data pipeline.

- Decrypts data file only once and passes decrypted bytes to all downstream checks.
- Returns both validation status and decrypted bytes for downstream DataFrame loading.
"""

import os
import boto3
import csv
import codecs
from botocore.exceptions import ClientError
from kms_decrypt_util import KMSDecryptionHelper

class FileValidator:
    def __init__(
        self, bucket, region, logger, allowed_prefixes, rds_secret_name,
        expected_extension, kms_id=None, max_sla_hours=2
    ):
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
        Check the file size using list_objects_v2 for a single file (encrypted size).
        """
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
            if 'Contents' not in response or len(response['Contents']) == 0:
                self.logger.error(f"File {key} not found in bucket {self.bucket}.")
                return False
            file_size = response['Contents'][0]['Size']
            self.logger.info(f"File {key} size: {file_size}")
            return file_size > 0
        except Exception as e:
            self.logger.error(f"Error getting file size for {key}: {e}")
            return False

    def check_file_extension(self, filename):
        ext = filename.split('.')[-1].lower()
        is_valid = ext == self.expected_extension.lower()
        self.logger.info(f"Extension check: {ext} vs allowed {self.expected_extension.lower()} = {is_valid}")
        return is_valid

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

    def get_decrypted_bytes(self, file_key):
        """
        Return the decrypted file bytes, or plain file bytes if not encrypted.
        """
        decrypted_bytes = self.check_kms_encryption_and_decrypt(file_key)
        if decrypted_bytes is None:
            d_obj = self.s3.get_object(Bucket=self.bucket, Key=file_key)
            decrypted_bytes = d_obj["Body"].read()
        return decrypted_bytes

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

    def check_file_size_matches_metadata(self, metadata_key, decrypted_bytes):
        """
        Compare the provided decrypted_bytes size to the value specified in the metadata file.
        Handles KMS-encrypted metadata files if necessary.
        """
        try:
            plaintext_meta = self.check_kms_encryption_and_decrypt(metadata_key)
            if plaintext_meta is None:
                m_obj = self.s3.get_object(Bucket=self.bucket, Key=metadata_key)
                raw_content = m_obj["Body"].read().decode("utf-8")
            else:
                raw_content = plaintext_meta.decode("utf-8")

            metadataInfo = list(csv.reader(raw_content.splitlines()))
            metadataInfo = [row for row in metadataInfo if row]
            if not metadataInfo:
                self.logger.error("Metadata file is empty or malformed.")
                return False

            size_from_meta = int(metadataInfo[0][0].split('|')[1])
            actual_size = len(decrypted_bytes)
            self.logger.info(f"File size from metadata: {size_from_meta}, decrypted file size: {actual_size}")
            return actual_size == size_from_meta

        except Exception as e:
            self.logger.error(f"Error checking file size against metadata: {e}")
            return False

    def check_file_integrity(self, decrypted_bytes):
        """
        Check if the file can be opened as a CSV using provided decrypted bytes.
        """
        try:
            csv.reader(codecs.iterdecode(decrypted_bytes.splitlines(), "utf-8")).__next__()
            self.logger.info("File opened successfully (integrity pass).")
            return True
        except Exception as e:
            self.logger.error(f"Failed to open decrypted file: {e}")
            return False

    def check_timeliness(self, key):
        import datetime
        last_modified = self.s3.head_object(Bucket=self.bucket, Key=key)['LastModified']
        now = datetime.datetime.utcnow().replace(tzinfo=last_modified.tzinfo)
        delta = (now - last_modified).total_seconds() / 3600
        is_valid = delta <= self.max_sla_hours
        self.logger.info(f"Timeliness check for {key}: {delta:.2f} hours old (valid={is_valid})")
        return is_valid

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

    def validate_file(
        self, file_key, metadata_key, file_config,
        all_data_keys=None, all_metadata_keys=None
    ):
        filename = file_key.split('/')[-1]
        self.logger.info(f"Starting validation for file: {file_key}")

        if not self.check_file_presence(file_key):
            self.logger.error("File presence check failed.")
            return False, None

        if not self.check_file_naming_pattern(filename):
            self.logger.error("File naming pattern check failed.")
            return False, None

        # Matching Metadata File Exists
        if not metadata_key and all_metadata_keys is not None:
            metadata_key = self.find_metadata_file(filename, all_metadata_keys)
            if not metadata_key:
                self.logger.error("Matching metadata file check failed.")
                return False, None
        elif not metadata_key:
            self.logger.error("No metadata key provided for metadata existence check.")
            return False, None

        if not self.check_file_size(file_key):
            self.logger.error("File size check failed (empty file).")
            return False, None

        decrypted_bytes = self.get_decrypted_bytes(file_key)

        if not self.check_file_size_matches_metadata(metadata_key, decrypted_bytes):
            self.logger.error("File size matches metadata check failed.")
            return False, None

        if not self.check_file_extension(filename):
            self.logger.error("File extension/type check failed.")
            return False, None

        if not self.check_file_integrity(decrypted_bytes):
            self.logger.error("File integrity check failed.")
            return False, None

        if not self.check_timeliness(file_key):
            self.logger.error("File timeliness/SLA check failed.")
            return False, None

        if all_data_keys is not None and self.check_duplicates(all_data_keys):
            self.logger.error("Duplicate check failed.")
            return False, None

        self.logger.info(f"All validation checks passed for {file_key}")
        return True, decrypted_bytes
