from cleo import Command

from zanasonic.data_management.extract.extract_raw_data import extract
from zanasonic.data_management.extract.extract_raw_data_kaggle import \
    extract_raw_data_kaggle


class Extract(Command):
    """
    Extracts the data from Kaggle.

    extract
        {source? : Extract dataset from kaggle}
    """

    def handle(self):
        source = self.argument("source")

        if source == "kaggle":
            extract_raw_data_kaggle()
        if source == "s3":
            extract()
        else:
            self.line("<error>argument invalid source<error>")
