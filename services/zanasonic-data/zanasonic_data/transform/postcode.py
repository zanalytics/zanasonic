import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic_data.config.core import config


def london_zone(df: pd.DataFrame, zone_number: int = 10) -> pd.DataFrame:
    df["london_zone"] = df["london_zone"].fillna(zone_number)
    return df


def postcode_process(df: pd.DataFrame) -> pd.DataFrame:
    dataframe = df.pipe(clean_names).pipe(london_zone)
    logger.info(f"postcode data shape: {dataframe.shape}")
    return dataframe


def transform_postcode(
    raw_path: str = config.postcode_config.postcode_raw_data,
    processed_path: str = config.postcode_config.postcode_processed_data,
):
    # Read in postcode data
    processed_postcode_df = (
        pd.read_csv(raw_path, low_memory=False)
        .pipe(postcode_process)
        .pipe(london_zone)
        .to_parquet(path=processed_path, index=False)
    )

    logger.success(f"postcode data saved to: {processed_path}")


if __name__ == "__main__":
    transform_postcode()
