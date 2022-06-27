from typing import Dict, List, Any, Union
import pandas as pd
from loguru import logger
from janitor import clean_names
from zanasonic_etl.config.core import config


def select_hpi_columns(df: pd.DataFrame, index_columns: List):
    df = df[
        ["date", "regionname", "areacode", "index", "detachedindex", "semidetachedindex", "terracedindex", "flatindex"]
    ]
    return df


def format_date(df: pd.DataFrame):
    df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
    df["hpi_month_year"] = df.date.dt.to_period("M")
    return df


def house_price_index_process(df: pd.DataFrame, index_columns: List, hpi_rename_columns: Dict) -> pd.DataFrame:
    dataframe = (
        df.pipe(clean_names)
        .pipe(select_hpi_columns, index_columns=index_columns)
        .rename(hpi_rename_columns, axis="columns")
        .pipe(format_date)
    )
    logger.info(f"price paid data shape: {dataframe.shape}")
    return dataframe


# Read in price paid data
processed_hpi_df = (
    pd.read_csv(
        config.house_price_index_config.hpi_raw_data,
        low_memory=False,
    )
    .pipe(
        house_price_index_process,
        index_columns=config.house_price_index_config.hpi_columns,
        hpi_rename_columns=config.house_price_index_config.hpi_rename_columns,
    )
    .to_parquet(path=config.house_price_index_config.hpi_processed_data, index=True)
)

logger.success(f"house price index data saved to: {config.house_price_index_config.hpi_processed_data}")
