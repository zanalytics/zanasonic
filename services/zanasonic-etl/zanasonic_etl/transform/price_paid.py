from typing import Dict, List, Any, Union
import pandas as pd
from loguru import logger
from janitor import clean_names
from zanasonic_etl.config.core import config


def set_min_max_price(df: pd.DataFrame, min_price: int = 10000, max_price: int = 5000000) -> pd.DataFrame:
    df = df.loc[(df.price >= min_price) & (df.price <= max_price)]
    return df


def month_year(df: pd.DataFrame, month_year_column: str):
    df[month_year_column] = df.date.dt.to_period("M")
    return df


def price_paid_process(
    df: pd.DataFrame,
    duplicate_columns: List,
    drop_columns: List,
    min_price: int = 10000,
    max_price: int = 5000000,
) -> pd.DataFrame:
    dataframe = (
        df.pipe(clean_names)
        .assign(id=df.id.str.strip("{}"))
        .set_index("id")
        .sort_values(by="date", ascending=False)
        .dropna(subset=["postcode"])
        .drop_duplicates(subset=duplicate_columns, keep="first")
        .pipe(set_min_max_price, min_price=min_price, max_price=max_price)
        .drop(columns=drop_columns)
        .assign(current_month_year=df.date.dt.to_period("M").max())
        .pipe(month_year, month_year_column="price_paid_month_year")
    )
    logger.info(f"price paid data shape: {dataframe.shape}")
    return dataframe


# Read in price paid data
processed_price_paid_df = (
    pd.read_csv(
        config.price_paid_config.price_paid_raw_data,
        parse_dates=config.price_paid_config.price_paid_date_column,
        names=config.price_paid_config.price_paid_columns,
        low_memory=False,
    )
    .pipe(
        price_paid_process,
        duplicate_columns=config.price_paid_config.price_paid_columns[1:],
        drop_columns=config.price_paid_config.price_paid_columns_to_drop,
        min_price=10000,
        max_price=5000000,
    )
    .to_parquet(path=config.price_paid_config.price_paid_processed_data, index=True)
)

logger.success(f"price paid data saved to: {config.price_paid_config.price_paid_processed_data}")
