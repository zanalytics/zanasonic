from typing import List

import pandas as pd
from janitor import clean_names
from loguru import logger

from zanasonic_data.config.core import config


def set_min_max_price(
    df: pd.DataFrame, min_price: int = 10000, max_price: int = 5000000
) -> pd.DataFrame:
    """
    Filters the price paid dataset to only include properties
    that have a price between min_price and max_price.


    Parameters
    ----------
    df:pd.DataFrame
        Specify the dataframe that we want to manipulate
    min_price:int=10000
        Set the minimum price of the dataframe
    max_price:int=5000000
        Set the maximum price of a house

    Returns
    -------
    pd.DataFrame
        The dataframe with the price values within the specified range
    """
    df = df.loc[(df.price >= min_price) & (df.price <= max_price)]
    return df


def month_year(df: pd.DataFrame, month_year_column: str) -> pd.DataFrame:
    """
    Creates the month_year column in format MM-YYYY. Takes a DataFrame and a string of the datetime column.

    Parameters
    ----------
    df:pd.DataFrame
        Specify the dataframe that we want to perform the function on
    month_year_column:str
        Specify the name of the column that contains a string representation of a date

    Returns
    -------
    pd.DataFrame
        A dataframe with the month and year of each row in the df

    """
    df[month_year_column] = df.date.dt.to_period("M")
    return df


def price_paid_process(
    df: pd.DataFrame,
    duplicate_columns: List,
    drop_columns: List,
    min_price: int = 10000,
    max_price: int = 5000000,
) -> pd.DataFrame:
    """
    The price_paid_process function takes a dataframe of property sales data and returns a cleaned, filtered
    dataframe. The function:
    - Cleans the column names to remove spaces and special characters;
    - Removes duplicate rows based on the columns specified in `duplicate_columns`;
    - Filters out any rows with

    Parameters
    ----------
    df:pd.DataFrame
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


def transform_price_paid(
    raw_path: str = config.price_paid_config.price_paid_raw_data,
    date_columns: List = config.price_paid_config.price_paid_date_column,
    column_names: List = config.price_paid_config.price_paid_columns,
    duplicate_columns: List = config.price_paid_config.price_paid_columns[1:],
    drop_columns: List = config.price_paid_config.price_paid_columns_to_drop,
    min_price: int = 10000,
    max_price: int = 5000000,
    processed_path: str = config.price_paid_config.price_paid_processed_data,
) -> pd.DataFrame:
    """
    Reads in the raw price paid data defined in the config.yaml. Processes the dataframe and returns a parquet file
    in the processed directory.

    Parameters
    ----------
    raw_path:str=config.price_paid_config.price_paid_raw_data
        Set the path to the raw data file
    date_columns:List=config.price_paid_config.price_paid_date_column
        Specify the columns that contain date values
    column_names:List=config.price_paid_config.price_paid_columns
        Specify the names of the columns in the dataframe
    duplicate_columns:List=config.price_paid_config.price_paid_columns[1:]
        Remove the first column from the dataframe
    drop_columns:List=config.price_paid_config.price_paid_columns_to_drop
        Drop columns from the dataframe
    min_price:int=10000
        Filter out the data that is less than 10000
    max_price:int=5000000
        Filter out any price paid data that is above £5,000,000
    processed_path:str=config.price_paid_config.price_paid_processed_data
        Specify the path to which we want to write our processed data

        Specify the path to the raw data file

    Returns
    -------
    pd.DataFrame
        A dataframe with the following columns:
    """
    processed_price_paid_df = (
        pd.read_csv(
            raw_path,
            parse_dates=date_columns,
            names=column_names,
            low_memory=False,
        )
        .pipe(
            price_paid_process,
            duplicate_columns=duplicate_columns,
            drop_columns=drop_columns,
            min_price=min_price,
            max_price=max_price,
        )
        .to_parquet(path=processed_path, index=True)
    )

    logger.success(f"price paid data saved to: {processed_path}")

if __name__ == "__main__":
    transform_price_paid()
