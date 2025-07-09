from pydantic_settings import BaseSettings

class AppSettings(BaseSettings):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region_name: str
    s3_source_bucket: str
    s3_destination_bucket: str
    pdf_input_prefix: str
    pdf_json_output_prefix: str
    batch_size: int
    class Config:
        env_file = ".env"  # This can be overridden dynamically

settings = AppSettings()
