import pandas as pd
from loguru import logger
from janitor import clean_names
from zanasonic_etl.config.core import config


def london_zone(df: pd.DataFrame, zone_number: int = 10) -> pd.DataFrame:
    df["london_zone"] = df["london_zone"].fillna(zone_number)
    return df


def postcode_process(df: pd.DataFrame) -> pd.DataFrame:
    dataframe = df.pipe(clean_names).pipe(london_zone)
    logger.info(f"postcode data shape: {dataframe.shape}")
    return dataframe


# Read in postcode data
processed_postcode_df = (
    pd.read_csv(config.postcode_config.postcode_raw_data, low_memory=False)
    .pipe(postcode_process)
    .pipe(london_zone)
    .to_parquet(path=config.postcode_config.postcode_processed_data, index=False)
)

logger.success(f"postcode data saved to: {config.postcode_config.postcode_processed_data}")
