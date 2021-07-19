import pandas as pd
from sklearn.model_selection import train_test_split

# columns to read

RANDOM_SEED = 42


# onehot encode
ONE_HOT_VARS = ["type"]

# categorical variables to encode
CATEGORICAL_VARS = ["district", "constituency", "postcode_district"]


FEATURES = [
    "district",
    "constituency",
    "postcode_district",
    "average_income",
    "type",
    "year",
    "price",
]

DROP_VARS = []

# load dataset
data = pd.read_csv(
    "./zanasonic/datasets/processed/pp_nottinghamshire.csv", usecols=FEATURES
)

# Split the data - train, validation and test
train_set, test_set = train_test_split(data, test_size=0.25, random_state=RANDOM_SEED)

# # load the pre-selected features
# # ==============================

# X_train = train_set.drop('price', axis = 1)
# X_test = test_set.drop('price', axis = 1)

# y_train = train_set['price']
# y_test = test_set['price']

# logging.info(f"Training shape: {train_set.shape}")
# logging.info(f"Test shape: {test_set.shape}")

# Save the split files
train_set.to_csv("./zanasonic/datasets/processed/train.csv", index=False)
test_set.to_csv("./zanasonic/datasets/processed/test.csv", index=False)
