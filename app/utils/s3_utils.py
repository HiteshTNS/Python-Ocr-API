import os

import boto3
from botocore.exceptions import ClientError
import logging

logger = logging.getLogger(__name__)

def get_s3_client(settings=None):
    """
    Returns a boto3 S3 client.
    If settings is provided, uses explicit credentials; otherwise, uses environment/default profile.
    """
    if settings:
        return boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
            region_name=settings.aws_region
        )
    return boto3.client("s3")

def download_s3_file(s3_key, local_path, settings=None):
    """
    Downloads a file from S3 to a local path.
    s3_key: The S3 object key (path within the bucket)
    local_path: Local file path to save the file
    settings: Optional AppSettings instance for credentials
    """
    s3 = get_s3_client(settings)
    bucket = settings.s3_bucket if settings else os.environ["S3_BUCKET"]
    try:
        logger.info(f"Trying to download S3://{bucket}/{s3_key}")

        s3.download_file(bucket, s3_key, local_path)
        logger.info(f"Downloaded S3://{bucket}/{s3_key} to {local_path}")
    except ClientError as e:
        logger.error(f"Error downloading S3://{bucket}/{s3_key}: {e}")
        raise

def delete_s3_file(s3_key, settings=None):
    """
    Deletes a file from S3.
    s3_key: The S3 object key (path within the bucket)
    settings: Optional AppSettings instance for credentials
    """
    s3 = get_s3_client(settings)
    bucket = settings.s3_bucket if settings else os.environ["S3_BUCKET"]
    try:
        s3.delete_object(Bucket=bucket, Key=s3_key)
        logger.info(f"Deleted S3://{bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error deleting S3://{bucket}/{s3_key}: {e}")
        raise
