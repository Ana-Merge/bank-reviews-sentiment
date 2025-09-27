from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(secrets_dir="/run/secrets")
    db_url: str
    cors_allowed_origins: list[str]
    auth_token_lifetime: int = 86400
    auth_token_secret_key: str

    region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    bucket_name: str
    aws_endpoint: str
    aws_access_domain_name: str
