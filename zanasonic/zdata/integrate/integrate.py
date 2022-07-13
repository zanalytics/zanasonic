import pandas as pd
from zanasonic.zdata.config.core import config, logger
from typing import Dict


def adjust_price(df: pd.DataFrame, house_type: Dict):
    for k, v in house_type.items():
        df.loc[df["type"] == k, "adjusted_price"] = (
            df[v["current_index"]] / df[v["pp_index"]]
        ) * df["price"]
    return df


def drop_index_columns(df):
    df = df.drop(columns=df.columns[df.columns.str.contains(pat="area_code|index")])
    return df


price_paid_index_columns = {
    "detached_index": "pp_detached_index",
    "semi_detached_index": "pp_semi_detached_index",
    "terraced_index": "pp_terraced_index",
    "flat_index": "pp_flat_index",
    "index": "pp_other_index",
}

house_type_index = {
    "D": {"current_index": "detached_index", "pp_index": "pp_detached_index"},
    "S": {"current_index": "semi_detached_index", "pp_index": "pp_semi_detached_index"},
    "T": {"current_index": "terraced_index", "pp_index": "pp_terraced_index"},
    "F": {"current_index": "flat_index", "pp_index": "pp_flat_index"},
    "O": {"current_index": "index", "pp_index": "pp_other_index"},
}


def integrate(
    price_paid_processed_path: str = config.price_paid_config.price_paid_processed_data,
    postcode_processed_path: str = config.postcode_config.postcode_processed_data,
    hpi_processed_path: str = config.house_price_index_config.hpi_processed_data,
    price_paid_master_path: str = config.price_paid_config.price_paid_master_processed_data,
    hpi_index_columns: dict[str, str] = price_paid_index_columns,
    hpi_index_type: dict[str, str] = house_type_index,
):

    postcode_df = pd.read_parquet(postcode_processed_path)
    hpi_df = pd.read_parquet(hpi_processed_path).drop(columns=["date"])

    df_price_paid_master = (
        pd.read_parquet(price_paid_processed_path)
        .merge(postcode_df, on="postcode")
        .merge(
            hpi_df,
            how="inner",
            left_on=["district_code", "price_paid_month_year"],
            right_on=["area_code", "hpi_month_year"],
        )
        .rename(hpi_index_columns, axis="columns")
        .drop(columns=["hpi_month_year", "region_name", "area_code"])
        .merge(
            hpi_df,
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
