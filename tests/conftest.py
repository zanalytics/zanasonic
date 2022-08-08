import pandas as pd
import pytest


@pytest.fixture
def raw_price_paid_df():
    return pd.read_csv("tests/sample_data/static/pp-test.csv")


@pytest.fixture
def raw_postcode_df():
    return pd.read_csv("tests/sample_data/static/pp-postcode.csv")


@pytest.fixture
def raw_hpi_df():
    return pd.read_csv("tests/sample_data/static/hpi-test.csv")


@pytest.fixture
def processed_price_paid_df():
    return pd.read_parquet("tests/sample_data/static/price-paid-processed-test.parquet")


@pytest.fixture
def processed_postcode_df():
    return pd.read_parquet("tests/sample_data/static/postcode-processed-test.parquet")


@pytest.fixture
def processed_hpi_df():
    return pd.read_parquet("tests/sample_data/static/hpi-processed-test.parquet")
