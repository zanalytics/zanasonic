from zanasonic_data.console.transform import Transform
from zanasonic_data.config.core import config
from cleo import Application

application = Application(config.app_config.package_name, config.app_config.package_version)
application.add(Transform())


def main():
    return application.run()


if __name__ == "__main__":
    main()
