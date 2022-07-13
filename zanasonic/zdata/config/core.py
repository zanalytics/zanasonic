from pathlib import Path
from typing import Dict, List, Sequence
from loguru import logger
from pydantic import BaseModel
from strictyaml import YAML, load

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
    Price Paid zdata config
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


class Config(BaseModel):
    """Master config object."""

    app_config: AppConfig
    price_paid_config: PricePaidConfig
    house_price_index_config: HousePriceIndexConfig
    postcode_config: PostcodeConfig


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
        with open(cfg_path, "r") as conf_file:
            parsed_config = load(conf_file.read())
            return parsed_config
    raise OSError(f"Did not find config file at path: {cfg_path}")


def create_and_validate_config(parsed_config: YAML = None) -> Config:
    """Run validation on config values."""
    if parsed_config is None:
        parsed_config = fetch_config_from_yaml()

    # specify the zdata attribute from the strictyaml YAML type.
    _config = Config(
        app_config=AppConfig(**parsed_config.data),
        price_paid_config=PricePaidConfig(**parsed_config.data),
        house_price_index_config=HousePriceIndexConfig(**parsed_config.data),
        postcode_config=PostcodeConfig(**parsed_config.data),
    )

    return _config


config = create_and_validate_config()


price_paid_path = Path(config.price_paid_config.price_paid_raw_data)
postcode_path = Path(config.postcode_config.postcode_raw_data)
house_price_index_path = Path(config.house_price_index_config.hpi_raw_data)

# if price_paid_path.is_file() == False:
#     raise Exception(f"Price Paid zdata missing from {price_paid_path}")
# elif postcode_path.is_file() == False:
#     raise Exception(f"Postcode zdata missing from {postcode_path}")
# elif house_price_index_path.is_file() == False:
#     raise Exception(f"House Price Index zdata missing from {house_price_index_path}")
