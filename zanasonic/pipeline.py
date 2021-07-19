from feature_engine.encoding import RareLabelEncoder, OrdinalEncoder, OneHotEncoder

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.ensemble import RandomForestRegressor

from zanasonic.config.core import config

price_pipe = Pipeline(
    [
        # ==== VARIABLE TRANSFORMATION =====
        (
            "one_hot_encode",
            OneHotEncoder(
                top_categories=None,
                variables=config.model_config.one_hot_vars,
                drop_last=True,
            ),
        ),
        # == CATEGORICAL ENCODING
        (
            "rare_label_encoder",
            RareLabelEncoder(
                tol=0.01, n_categories=3, variables=config.model_config.categorical_vars
            ),
        ),
        # encode categorical and discrete variables using the target mean
        (
            "categorical_encoder",
            OrdinalEncoder(
                encoding_method="ordered",
                variables=config.model_config.categorical_vars,
            ),
        ),
        ("scaler", MinMaxScaler()),
        ("model", RandomForestRegressor()),
    ]
)
