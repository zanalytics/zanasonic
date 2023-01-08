import os
from pathlib import Path
from typing import Dict, List, Optional

from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pydantic import BaseModel
from strictyaml import YAML, load

# find .env automagically by walking up directories until it's found
dotenv_path = find_dotenv()

# load up the entries as environment variables
load_dotenv(dotenv_path)

# Project Directories
PACKAGE_ROOT = Path(__file__).resolve().parent
ROOT = PACKAGE_ROOT.parent
CONFIG_FILE_PATH = PACKAGE_ROOT / "config.yml"


class AppConfig(BaseModel):
    """
    Application-level config.
    """

    random_state: str
    package_name: str
    package_version: str


class PricePaidConfig(BaseModel):
    """
    Price Paid data_management config
    """

    price_paid_raw_data: str
    price_paid_google_drive_raw_data: str
    price_paid_processed_data: str
    price_paid_master_processed_data: str
    price_paid_columns: List[str]
    price_paid_date_column: List[str]
    price_paid_min_price: int
    price_paid_max_price: int
    price_paid_columns_to_drop: List[str]


class HousePriceIndexConfig(BaseModel):
    """
    House Price Index
    """

    hpi_raw_data: str
    hpi_google_drive_raw_data: str
    hpi_processed_data: str
    hpi_columns: List[str]
    hpi_rename_columns: Dict


class PostcodeConfig(BaseModel):
    """
    Postcode Index
    """

    postcode_raw_data: str
    postcode_google_drive_raw_data: str
    postcode_processed_data: str


class AwsConfig(BaseModel):
    """
    The AWS configuration
    """

    ENVIRONMENT: str = os.environ.get("ENVIRONMENT")
    AWS_ACCOUNT: str = os.environ.get(f"AWS_{ENVIRONMENT.upper()}_ACCOUNT")
    AWS_REGION: str = os.environ.get("AWS_REGION")

    # S3
    MOCK_ZONE_BUCKET: str = "zanasonic-scratch"
    LANDING_ZONE_BUCKET: str = f"zanasonic-landing-zone-{ENVIRONMENT}"
    CLEAN_ZONE_BUCKET: str = f"zanasonic-clean-zone-{ENVIRONMENT}"
    CURATED_ZONE_BUCKET: str = f"zanasonic-curated-zone-{ENVIRONMENT}"

    # ECR
    ECR_REPOSITORY: str = f"{ENVIRONMENT}/zanasonic"

    # SageMaker
    PROCESSING_JOB_IMAGE_URI: str = (
        f"{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com/{ECR_REPOSITORY}:latest"
    )
    PROCESSING_JOB_ROLE_ARN: str = f"arn:aws:iam::{AWS_ACCOUNT}:{ENVIRONMENT}"

    # RDS
    POSTGRES_DBNAME: str = os.environ.get("POSTGRES_DBNAME")
    POSTGRES_ADDRESS: str = os.environ.get("POSTGRES_ADDRESS")
    POSTGRES_PORT: str = os.environ.get("POSTGRES_PORT")
    POSTGRES_USERNAME: str = os.environ.get("POSTGRES_USERNAME")
    POSTGRES_PASSWORD: str = os.environ.get("POSTGRES_PASSWORD")


class Config(BaseModel):
    """Master config object."""

    app_config: AppConfig
    price_paid_config: PricePaidConfig
    house_price_index_config: HousePriceIndexConfig
    postcode_config: PostcodeConfig
    aws_config: AwsConfig


def find_config_file() -> Path:
    """Locate the configuration file."""
    if CONFIG_FILE_PATH.is_file():
        return CONFIG_FILE_PATH
    raise Exception(f"Config not found at {CONFIG_FILE_PATH!r}")


def fetch_config_from_yaml(cfg_path: Path = None) -> YAML:
    """Parse YAML containing the package configuration."""

    if not cfg_path:
        cfg_path = find_config_file()

    if cfg_path:
        with open(cfg_path, mode="r", encoding="utf-8") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the data_management attribute from the strictyaml YAML type.
    _config = Config(
        app_config=AppConfig(**parsed_config.data),
        price_paid_config=PricePaidConfig(**parsed_config.data),
        house_price_index_config=HousePriceIndexConfig(**parsed_config.data),
        postcode_config=PostcodeConfig(**parsed_config.data),
        aws_config=AwsConfig(**parsed_config.data),
    )

    return _config


config = create_and_validate_config()
