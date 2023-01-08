import pandas as pd
from pydantic.main import ModelMetaclass
from typing import List
import json

def validate_data_schema(data_schema: ModelMetaclass):
    """This decorator will validate a pd.DataFrame object against the given data_schema."""

    def data_lint(func):
        def wrapper(*args, **kwargs):
            response = func(*args, **kwargs)
            if isinstance(response, pd.DataFrame):
                # check result of the function execution against the data_schema
                df_dict = response.to_dict(orient="records")
                
                # Wrap the data_schema into a helper class for validation
                class ValidationWrap(BaseModel):
                    df_dict: List[data_schema]
                # Do the validation
                _ = ValidationWrap(df_dict=df_dict)
            else:
                raise TypeError("Function is not returning object of type pandas.DataFrame.")

            # return the function result
            return response
        return wrapper
    return data_lint

