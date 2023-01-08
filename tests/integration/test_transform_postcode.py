import pathlib

import pandas as pd
from pandas.testing import assert_frame_equal

from zanasonic.data_management.transform.postcode import transform_postcode


def test_transform_postcode(processed_postcode_df):
    """
    GIVEN: A raw HPI Dataset
    WHEN: The transform_hpi function is called
    THEN: The resulting dataframe should be the same as the one in the static folder
    """

    processed_path = pathlib.Path(
        "tests/sample_data/processed/postcode-test-processed.parquet"
    )

    transform_postcode(
        raw_path="tests/sample_data/static/postcode-test.csv",
        processed_path=str(processed_path),
    )

    response_df = pd.read_parquet(str(processed_path))
    expected_df = processed_postcode_df

    assert processed_path.exists()
    assert processed_path.is_file()
    assert len(response_df.columns) == len(expected_df.columns)
    assert response_df.shape == expected_df.shape
    assert len(response_df.columns) == len(expected_df.columns)
    assert_frame_equal(response_df, expected_df)
    processed_path.unlink()
