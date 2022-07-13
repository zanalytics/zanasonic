import subprocess


def create_data_folder():
    """
    Downloads the raw zdata from Kaggle.


    Parameters
    ----------

    Returns
    -------

        A dictionary with the following keys:
    """
    commands = ["mkdir -p ./zdata/raw/",
                "mkdir -p ./zdata/processed/",
                "mkdir -p ./zdata/train/",
                "mkdir -p ./zdata/test/",
                "mkdir -p ./zdata/validation/",
                "mkdir -p ./zdata/predictions/"
                ]

    for command in commands:
        process = subprocess.Popen(command.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()


if __name__ == "__main__":
    create_data_folder()
