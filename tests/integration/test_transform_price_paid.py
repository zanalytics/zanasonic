from zanasonic.data_management.transform.price_paid import transform_price_paid
from zanasonic.data_management.config.core import config
import pandas as pd
import pathlib
from pandas.testing import assert_frame_equal


def test_transform_price_paid(processed_price_paid_df):
    """
    GIVEN: The Price Paid row data without the header
    WHEN: The transform_price_paid function is called
    THEN: The resulting dataframe should be the same as the one in the static folder
    """

    processed_path = pathlib.Path(
        "tests/sample_data/processed/price-paid-test-processed.parquet"
    )

    transform_price_paid(
        raw_path="tests/sample_data/static/pp-test.csv",
        processed_path=str(processed_path),
    )

    response_df = pd.read_parquet(str(processed_path))
    expected_df = processed_price_paid_df

    assert processed_path.exists()
    assert processed_path.is_file()
    assert len(response_df.columns) == len(expected_df.columns)
    assert response_df.shape == expected_df.shape
    assert len(response_df.columns) == len(expected_df.columns)
    assert_frame_equal(response_df, expected_df)
    processed_path.unlink()
