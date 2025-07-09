import boto3
from app.models.config import settings  # Import your settings here

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
        region_name=settings.aws_region_name,
    )

def list_pdfs_in_s3():
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    pdf_keys = []
    for page in paginator.paginate(Bucket=settings.s3_source_bucket, Prefix=settings.pdf_input_prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.pdf'):
                pdf_keys.append(obj['Key'])
    return pdf_keys

def download_s3_file(key, local_path):
    s3 = get_s3_client()
    s3.download_file(settings.s3_source_bucket, key, local_path)

def upload_s3_json(local_path, key):
    s3 = get_s3_client()
    s3.upload_file(local_path, settings.s3_source_bucket, key)

def list_jsons_in_s3(s3, bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    json_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                json_keys.append(obj['Key'])
    return json_keys

def copy_s3_file(s3, source_bucket, source_key, dest_bucket, dest_key):
    s3.copy_object(
        Bucket=dest_bucket,
        CopySource={'Bucket': source_bucket, 'Key': source_key},
        Key=dest_key
    )

def list_files_in_s3_prefix(s3, bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('/'):
                files.append(obj['Key'])
    return files

def clear_s3_prefix(s3, bucket, prefix=""):
    """
    Deletes all objects under the given prefix in the specified S3 bucket.
    """
    paginator = s3.get_paginator('list_objects_v2')
    keys_to_delete = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys_to_delete.append({'Key': obj['Key']})
    if keys_to_delete:
        # S3 delete_objects can delete up to 1000 objects at a time
        for i in range(0, len(keys_to_delete), 1000):
            s3.delete_objects(
                Bucket=bucket,
                Delete={'Objects': keys_to_delete[i:i+1000]}
            )
