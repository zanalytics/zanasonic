import pandas as pd

from zanasonic.data_management.config.core import config

data_frame = pd.read_parquet(
    path=config.price_paid_config.price_paid_master_processed_data
)

data_frame = data_frame[data_frame["postcode"] == "S60 8BJ"]
data_frame.to_csv("data/processed/price_paid_master.csv")
