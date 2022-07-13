from cleo import Command
from zanasonic.zdata.integrate.integrate import integrate


class Integrate(Command):
    """
    This command merges all the datasets together and creates the adjusted price for the current month.

    integrate
    """

    def handle(self):
        integrate()
