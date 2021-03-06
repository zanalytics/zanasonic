from cleo import Command
from zanasonic.zdata.transform.price_paid import transform_price_paid
from zanasonic.zdata.transform.postcode import transform_postcode
from zanasonic.zdata.transform.house_price_index import transform_hpi


class Transform(Command):
    """
    Transforms the raw price paid dataset.

    transform
        {dataset? : Which dataset do you want to transform price-paid, hpi, postcode?}
        {--y|yell : If set, the task will yell in uppercase letters}
    """

    def handle(self):
        dataset = self.argument("dataset")

        if dataset == "price-paid":
            transform_price_paid()
        elif dataset == "postcode":
            transform_postcode()
        elif dataset == "hpi":
            transform_hpi()
        else:
            self.line(
                "<error>argument invalid can only be price-paid, postcode or hpi<error>"
            )
