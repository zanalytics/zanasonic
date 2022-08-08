from zanasonic.data_management.transform.house_price_index import format_date
import pandas as pd
from pandas.testing import assert_frame_equal


def test_format_date():
    """
    GIVEN: A dataframe with the date column in the format dd/mm/yyyy
    WHEN: The format date function is called
    THEN: The resulting dataframe will have date and hpi_month_year in format mm/yyyy
    """

    response = format_date(data_frame=pd.DataFrame(["08/10/2020"], columns=["date"]))

    expected = pd.DataFrame(
        [["08/10/2020", "10/2020"]], columns=["date", "hpi_month_year"]
    )
    expected["date"] = pd.to_datetime(expected["date"], format="%d/%m/%Y")
    expected["hpi_month_year"] = expected.date.dt.to_period("M")
    assert len(response.columns) == 2
    assert list(response.columns) == list(expected.columns)
    assert "date" in list(response.columns)
    assert "hpi_month_year" in list(response.columns)
    assert_frame_equal(expected, response)
