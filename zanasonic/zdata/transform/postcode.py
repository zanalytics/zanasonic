import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic.zdata.config.core import config


def london_zone(data_frame: pd.DataFrame, zone_number: int = 10) -> pd.DataFrame:
    """
    Assigns the fixed zone_number to all NA rows


    Parameters
    ----------
    data_frame: pd.DataFrame
        The dataframe with london_zone
    zone_number: int=10
        Assigns value of 10 by default

    Returns
    -------
    pd.DataFrame
        A dataframe with populated london_zone
    """
    data_frame["london_zone"] = data_frame["london_zone"].fillna(zone_number)
    return data_frame


def transform_postcode(
    raw_path: str = config.postcode_config.postcode_raw_data,
    processed_path: str = config.postcode_config.postcode_processed_data,
):
    """
    Transforms the postcode data.
    1. Cleans column names
    2. Assigns the london zone
    3. Saves to Parquet


    Parameters
    ----------
    raw_path: str
        Path to raw data
    processed_path: str
        Path to save the processed data

    Returns
    -------
    None
    """
    # Read in postcode zdata
    processed_postcode_data_frame = (
        pd.read_csv(raw_path, low_memory=False)
        .pipe(clean_names)
        .pipe(london_zone)
        .to_parquet(path=processed_path, index=False)
    )

    logger.success(f"postcode data saved to: {processed_path}")


if __name__ == "__main__":
    transform_postcode()
