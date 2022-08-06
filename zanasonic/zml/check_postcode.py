import pandas as pd
from zanasonic.zdata.config.core import config

data_frame = pd.read_parquet(
    path=config.price_paid_config.price_paid_master_processed_data
)
data_frame = data_frame[data_frame["postcode"] == "S60 8BJ"]
data_frame.to_csv("dn.csv")
