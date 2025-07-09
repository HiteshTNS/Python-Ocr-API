import boto3

s3 = boto3.client(
    "s3",
    aws_access_key_id="AKIARXSMU5K5SKK6AEEU",
    aws_secret_access_key="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",
    region_name="us-west-2"
)

s3.copy_object(
    Bucket="claims-pdf-destination",
    CopySource={'Bucket': "claims-pdf-source", 'Key': "pdf/0407_001.pdf"},
    Key="0407_001.pdf"
)
