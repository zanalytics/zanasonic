from typing import Any, Dict, List, Union

import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic_data.config.core import config


def select_hpi_columns(df: pd.DataFrame, index_columns: List):
    df = df[
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
    return df


def format_date(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["hpi_month_year"] = df.date.dt.to_period("M")
    return df


def house_price_index_process(
    df: pd.DataFrame, index_columns: List, hpi_rename_columns: Dict
) -> pd.DataFrame:
    dataframe = (
        df.pipe(clean_names)
        .pipe(select_hpi_columns, index_columns=index_columns)
        .rename(hpi_rename_columns, axis="columns")
        .pipe(format_date)
    )
    logger.info(f"price paid data shape: {dataframe.shape}")
    return dataframe


# Read in price paid data
def transform_hpi(
    raw_path: str = config.house_price_index_config.hpi_raw_data,
    index_columns: List = config.house_price_index_config.hpi_columns,
    rename_columns: List = config.house_price_index_config.hpi_rename_columns,
    processed_path: str = config.house_price_index_config.hpi_processed_data,
):
    processed_hpi_df = (
        pd.read_csv(
            raw_path,
            low_memory=False,
        )
        .pipe(
            house_price_index_process,
            index_columns=index_columns,
            hpi_rename_columns=rename_columns,
        )
        .to_parquet(path=processed_path, index=True)
    )

    logger.success(f"house price index data saved to: {processed_path}")


if __name__ == "__main__":
    transform_hpi()
