from zanasonic.console.commands.transform import Transform
from zanasonic.console.commands.extract import Extract
from zanasonic.console.commands.integrate import Integrate
from zanasonic.console.commands.create_data_folder import DataFolder
from zanasonic.zdata.config.core import config

from cleo import Application

application = Application(
    name=config.app_config.package_name, version=config.app_config.package_version
)
application.add(Transform())
application.add(Extract())
application.add(Integrate())
application.add(DataFolder())


def main():
    """
    Main function that brings all the CLI commands together
    """
    return application.run()


if __name__ == "__main__":
    main()
