import subprocess


def extract_raw_data_kaggle():
    """
    Downloads the raw data from Kaggle.


    Parameters
    ----------

    Returns
    -------

        A dictionary with the following keys:
    """
    bash_command = "kaggle datasets download chrispen/zanasonic -p ./../../data/raw/ --force --unzip"
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()


if __name__ == "__main__":
    extract_raw_data_kaggle()
