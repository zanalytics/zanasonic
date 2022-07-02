from cleo import Command
from zanasonic_data.extract.extract_raw_data_kaggle import extract_raw_data_kaggle


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
        else:
            self.line("<error>argument invalid source<error>")
