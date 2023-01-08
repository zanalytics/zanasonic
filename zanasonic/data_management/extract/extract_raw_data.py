import awswrangler as wr
from loguru import logger

from zanasonic.data_management.config.core import config


def extract():
    """
    Extract function
    """

    # TODO: Will need to split this function and add arguments but have made it one for now.
    # TODO: change the paths when the landing, clean, and curated zone buckets are created.
    # TODO: will need to point to the source of the raw data but using static data for now.

    price_paid_raw_path = f"{config.aws_config.MOCK_ZONE_BUCKET}/data/raw/price_paid/06_2022/pp_sample.csv"

    price_paid_landing_path = f"{config.aws_config.MOCK_ZONE_BUCKET}/data/landing/price_paid/06_2022/pp_sample.parquet"

    house_price_index_raw_path = f"{config.aws_config.MOCK_ZONE_BUCKET}/data/raw/house_price_index/house_price_index.csv"
    house_price_index_landing_path = f"{config.aws_config.MOCK_ZONE_BUCKET}/data/landing/house_price_index/house_price_index.parquet"

    postcode_raw_path = (
        f"{config.aws_config.MOCK_ZONE_BUCKET}/data/raw/postcode/postcode.csv"
    )
    postcode_landing_path = (
        f"{config.aws_config.MOCK_ZONE_BUCKET}/data/landing/postcode/postcode.parquet"
    )

    price_paid_df = wr.s3.read_csv(
        path=f"s3://{price_paid_raw_path}",
        parse_dates=config.price_paid_config.price_paid_date_column,
        names=config.price_paid_config.price_paid_columns,
    )

    logger.info(f"shape of price paid: {price_paid_df.shape}")

    wr.s3.to_parquet(
        df=price_paid_df, path=f"s3://{price_paid_landing_path}", index=False
    )

    logger.success(f"written price paid to: {price_paid_landing_path}")

    house_price_index_df = wr.s3.read_csv(
        path=f"s3://{house_price_index_raw_path}",
    )

    logger.info(f"shape of house price: {house_price_index_df.shape}")

    wr.s3.to_parquet(
        df=house_price_index_df,
        path=f"s3://{house_price_index_landing_path}",
        index=False,
    )

    logger.success(f"written house price to: {house_price_index_landing_path}")

    postcode_df = wr.s3.read_csv(path=f"s3://{postcode_raw_path}", low_memory=False)

    logger.info(f"shape of postcode: {postcode_df.shape}")

    wr.s3.to_parquet(df=postcode_df, path=f"s3://{postcode_landing_path}", index=False)

    logger.success(f"written postcode to: {postcode_landing_path}")


if __name__ == "__main__":
    extract()
