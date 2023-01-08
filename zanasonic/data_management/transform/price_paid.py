from datetime import datetime
from typing import List

import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic.data_management.config.core import config


def set_min_max_price(
    data_frame: pd.DataFrame, min_price: int = 10000, max_price: int = 5000000
) -> pd.DataFrame:
    """
    Filters the price paid dataset to only include properties
    that have a price between min_price and max_price.


    Parameters
    ----------
    data_frame: pd.DataFrame
        Specify the dataframe that we want to manipulate
    min_price: int=10000
        Set the minimum price of the dataframe
    max_price: int=5000000
        Set the maximum price of a house

    Returns
    -------
    pd.DataFrame
        The dataframe with the price values within the specified range
    """
    data_frame = data_frame.loc[
        (data_frame.price >= int(min_price)) & (data_frame.price <= int(max_price))
    ]
    return data_frame


def month_year(data_frame: pd.DataFrame, month_year_column: str) -> pd.DataFrame:
    """
    Creates the month_year column in format MM-YYYY. Takes a DataFrame and a string of the datetime column.

    Parameters
    ----------
    data_frame:pd.DataFrame
        Specify the dataframe that we want to perform the function on
    month_year_column:str
        Specify the name of the column that contains a string representation of a date

    Returns
    -------
    pd.DataFrame
        A dataframe with the month and year of each row in the data_frame

    """
    data_frame[month_year_column] = data_frame.date.dt.to_period("M")
    return data_frame


def price_paid_process(
    data_frame: pd.DataFrame,
    duplicate_columns: List,
    drop_columns: List,
    min_price: int = 10000,
    max_price: int = 5000000,
) -> pd.DataFrame:
    """
    The price_paid_process function takes a dataframe of property sales data_management and returns a cleaned, filtered
    dataframe. The function:
    - Cleans the column names to remove spaces and special characters;
    - Removes duplicate rows based on the columns specified in `duplicate_columns`;
    - Filters out any rows with

    Parameters
    ----------
    data_frame:pd.DataFrame
        Specify the dataframe that is being passed into the function
    duplicate_columns:List
        Specify which columns should be used to determine if a row is a duplicate
    drop_columns:List
        Drop columns that are not needed for the analysis
    min_price:int=10000
        Set the minimum price of a property that we want to include in our analysis
    max_price:int=5000000
        Set the maximum price of a property to be included in the dataframe

    Returns
    -------
    pd.DataFrame
        A dataframe.

    """
    dataframe = (
        data_frame.pipe(clean_names)
        .assign(id=data_frame.id.str.strip("{}"))
        .set_index("id")
        .sort_values(by="date", ascending=False)
        .dropna(subset=["postcode"])
        .drop_duplicates(subset=duplicate_columns, keep="first")
        .pipe(set_min_max_price, min_price=min_price, max_price=max_price)
        .drop(columns=drop_columns)
        .assign(current_month_year=data_frame.date.dt.to_period("M").max())
        .pipe(month_year, month_year_column="price_paid_month_year")
    )
    logger.info(f"price paid data_management shape: {dataframe.shape}")
    return dataframe


def transform_price_paid(
    raw_path: str = config.price_paid_config.price_paid_raw_data,
    processed_path: str = config.price_paid_config.price_paid_processed_data,
):
    """
    Reads in the raw price paid data_management defined in the config.yaml. Processes the dataframe and returns a parquet file
    in the processed directory.

    Parameters
    ----------
    raw_path:str=config.price_paid_config.price_paid_raw_data
        Set the path to the raw data_management file
    processed_path:str=config.price_paid_config.price_paid_processed_data
        Specify the path to which we want to write our processed data_management

        Specify the path to the raw data_management file

    Returns
    -------
    pd.DataFrame
        A dataframe with the following columns:
    """
    processed_price_paid_data_frame = pd.read_csv(
        raw_path,
        parse_dates=config.price_paid_config.price_paid_date_column,
        date_parser=lambda x: datetime.strptime(x, "%d/%m/%Y"),
        names=config.price_paid_config.price_paid_columns,
        low_memory=False,
    ).pipe(
        price_paid_process,
        duplicate_columns=config.price_paid_config.price_paid_columns[1:],
        drop_columns=config.price_paid_config.price_paid_columns_to_drop,
    )

    processed_price_paid_data_frame.to_parquet(path=processed_path, index=True)

    logger.success(f"price paid data_management saved to: {processed_path}")


if __name__ == "__main__":
    transform_price_paid()
