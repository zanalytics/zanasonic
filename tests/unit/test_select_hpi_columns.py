from janitor import clean_names

from zanasonic.data_management.transform.house_price_index import \
    select_hpi_columns


def test_select_hpi_columns(raw_hpi_df):
    """
    GIVEN: the raw house-price-index data
    WHEN: the select hpi columns function is called
    THEN: The resulting dataframe will have 8 columns
    """
    raw_hpi_clean_df = clean_names(raw_hpi_df)
    response_df = select_hpi_columns(data_frame=raw_hpi_clean_df)
    expected = [
        "date",
        "regionname",
        "areacode",
        "index",
        "detachedindex",
        "semidetachedindex",
        "terracedindex",
        "flatindex",
    ]
    assert len(response_df.columns) == len(expected)
    assert list(response_df.columns) == expected
