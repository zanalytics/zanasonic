import pandas as pd
from zanasonic.data_management.config.core import config

pp_df = pd.read_csv(
    "tests/sample_data/static/pp-test.csv",
    parse_dates=config.price_paid_config.price_paid_date_column,
    names=config.price_paid_config.price_paid_columns,
    low_memory=False,
)

print(pp_df.head())
print(pd.to_datetime(pp_df["date"]))

# print(pp_df.date.astype('datetime64[ns]'))
# postcode_df = pd.read_csv("data/raw/postcodes.csv", low_memory=False)
# hpi_df = pd.read_csv("data/raw/house_price_index.csv", index_col=[0])
#
# postcodes_list = list(set(pp_df.postcode))
# postcodes_test_df = postcode_df[postcode_df["Postcode"].isin(postcodes_list)]
# district_codes_list = list(set(postcodes_test_df["District Code"]))
# hpi_test_df = hpi_df[hpi_df["AreaCode"].isin(district_codes_list)]
#
# pp_df.to_csv("pp-test.csv", index=False)
# postcodes_test_df.to_csv("postcode-test.csv", index=False)
# hpi_test_df.to_csv("hpi-test.csv", index=False)
