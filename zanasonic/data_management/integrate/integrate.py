from typing import Dict

import pandas as pd

from zanasonic.data_management.config.core import config, logger


def adjust_price(data_frame: pd.DataFrame, house_type: Dict):
    """
    Create an Adjusted price based on the HPI.

    Notes
    ----------
    Can be improved later to create a HPI for each month. Would be useful for time series.

    Parameters
    ----------
    data_frame: pd.Dataframe
        Dataframe
    house_type: Dict
       The house type dictionary.

    Returns
    -------
    A dataframe with adjusted price.
    """
    for key, value in house_type.items():
        data_frame.loc[data_frame["type"] == key, "adjusted_price"] = (
            data_frame[value["current_index"]] / data_frame[value["pp_index"]]
        ) * data_frame["price"]
    return data_frame


def drop_index_columns(data_frame: pd.DataFrame):
    """
    Drops any columns with area_code or index

    Parameters
    ----------
    data_frame: pd.Dataframe
        Dataframe

    Returns
    -------
    A dataframe remaining columns
    """
    data_frame = data_frame.drop(
        columns=data_frame.columns[
            data_frame.columns.str.contains(pat="area_code|index")
        ]
    )
    return data_frame


def integrate(
    price_paid_processed_path: str = config.price_paid_config.price_paid_processed_data,
    postcode_processed_path: str = config.postcode_config.postcode_processed_data,
    hpi_processed_path: str = config.house_price_index_config.hpi_processed_data,
    price_paid_master_path: str = config.price_paid_config.price_paid_master_processed_data,
):
    """
    Drops any columns with area_code or index

    Parameters
    ----------
    price_paid_processed_path: str
        Path to the processed Price paid data
    postcode_processed_path: str
        Path to the processed Postcode data
    hpi_processed_path: str
        Path to the processed HPI data
    price_paid_master_path: str
        Path to save the dataset with all 3 datasets joined together

    Returns
    -------
    A dataframe remaining columns
    """
    hpi_index_columns = {
        "detached_index": "pp_detached_index",
        "semi_detached_index": "pp_semi_detached_index",
        "terraced_index": "pp_terraced_index",
        "flat_index": "pp_flat_index",
        "index": "pp_other_index",
    }

    hpi_index_type = {
        "D": {"current_index": "detached_index", "pp_index": "pp_detached_index"},
        "S": {
            "current_index": "semi_detached_index",
            "pp_index": "pp_semi_detached_index",
        },
        "T": {"current_index": "terraced_index", "pp_index": "pp_terraced_index"},
        "F": {"current_index": "flat_index", "pp_index": "pp_flat_index"},
        "O": {"current_index": "index", "pp_index": "pp_other_index"},
    }

    postcode_data_frame = pd.read_parquet(postcode_processed_path)
    hpi_data_frame = pd.read_parquet(hpi_processed_path).drop(columns=["date"])

    data_frame_price_paid_master = (
        pd.read_parquet(price_paid_processed_path)
        .merge(postcode_data_frame, on="postcode")
        .merge(
            hpi_data_frame,
            how="inner",
            left_on=["district_code", "price_paid_month_year"],
            right_on=["area_code", "hpi_month_year"],
        )
        .rename(hpi_index_columns, axis="columns")
        .drop(columns=["hpi_month_year", "region_name", "area_code"])
        .merge(
            hpi_data_frame,
            how="inner",
            left_on=["district_code", "current_month_year"],
            right_on=["area_code", "hpi_month_year"],
        )
        .pipe(adjust_price, house_type=hpi_index_type)
        .pipe(drop_index_columns)
        .to_parquet(path=price_paid_master_path, index=True)
    )

    logger.info(f"written location: {price_paid_master_path}")


if __name__ == "__main__":
    integrate()
