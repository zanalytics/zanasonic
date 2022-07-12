from cleo import Command
from zanasonic.data.config.create_data_folder import create_data_folder


class DataFolder(Command):
    """
    Creates the data folder in the current directory with raw/train/test/validation/predictions

    data-folder
    """

    def handle(self):
        create_data_folder()
