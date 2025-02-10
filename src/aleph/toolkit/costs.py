from decimal import Decimal
from math import ceil

from aleph.toolkit.constants import PRICE_PRECISION


def format_cost(v: Decimal | str, p: int = PRICE_PRECISION) -> Decimal:
    return ceil(Decimal(v) * 10**p) / Decimal(10**p)


def format_cost_str(v: Decimal | str, p: int = PRICE_PRECISION) -> str:
    n = format_cost(v, p)
    return "{:.{p}f}".format(n, p=p)
