import subprocess


def create_data_folder():
    """
    Downloads the raw data from Kaggle.


    Parameters
    ----------

    Returns
    -------

        A dictionary with the following keys:
    """
    commands = ["mkdir -p ./data/raw/",
                "mkdir -p ./data/processed/",
                "mkdir -p ./data/train/",
                "mkdir -p ./data/test/",
                "mkdir -p ./data/validation/",
                "mkdir -p ./data/predictions/"
                ]

    for command in commands:
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()


if __name__ == "__main__":
    create_data_folder()
