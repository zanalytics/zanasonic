from typing import Dict, List

import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic.data_management.config.core import config


def select_hpi_columns(data_frame: pd.DataFrame):
    """
    Keeps only selected columns


    Parameters
    ----------
    data_frame: pd.DataFrame
        Dataframe to filter

    Returns
    -------
    pd.DataFrame
    """
    data_frame = data_frame[
        [
            "date",
            "regionname",
            "areacode",
            "index",
            "detachedindex",
            "semidetachedindex",
            "terracedindex",
            "flatindex",
        ]
    ]
    return data_frame


def format_date(data_frame: pd.DataFrame):
    """
    Creates the hpi_month_year column with just month and year

    Parameters
    ----------
    data_frame: pd.DataFrame
        Dataframe to filter

    Returns
    -------
    pd.DataFrame
    """
    data_frame["date"] = pd.to_datetime(data_frame["date"], format="%d/%m/%Y")
    data_frame["hpi_month_year"] = data_frame.date.dt.to_period("M")
    return data_frame


def transform_hpi(
    raw_path: str = config.house_price_index_config.hpi_raw_data,
    rename_columns: List = config.house_price_index_config.hpi_rename_columns,
    processed_path: str = config.house_price_index_config.hpi_processed_data,
):
    """
    Transforms the house price index data

    Parameters
    ----------
    raw_path: str
        Path of the raw data
    rename_columns: str
        Rename dictionary for HPI columns
    processed_path: str
        Path to save the processed data

    Returns
    -------
    pd.DataFrame
    """
    processed_hpi_data_frame = (
        pd.read_csv(
            raw_path,
            low_memory=False,
        )
        .pipe(clean_names)
        .pipe(select_hpi_columns)
        .rename(rename_columns, axis="columns")
        .pipe(format_date)
        .to_parquet(path=processed_path, index=True)
    )

    logger.success(f"house price index data saved to: {processed_path}")


if __name__ == "__main__":
    transform_hpi()
