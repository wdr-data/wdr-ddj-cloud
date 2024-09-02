import datetime as dt
from decimal import Decimal

import babel.dates
import babel.numbers


def format_datetime(dt: dt.datetime, locale: str = "de_DE") -> str:
    return babel.dates.format_datetime(dt, locale=locale)


def format_date(dt: dt.datetime, locale: str = "de_DE") -> str:
    return babel.dates.format_date(dt, locale=locale)


def format_time(dt: dt.datetime, locale: str = "de_DE") -> str:
    return babel.dates.format_time(dt, locale=locale)


def format_number(number: float | str | Decimal, places: int = 2, locale: str = "de_DE") -> str:
    number = Decimal(number)

    # round to the given number of decimal places
    number = number.quantize(Decimal(10) ** -places)

    return babel.numbers.format_number(number, locale=locale)


def format_integer(number: int, locale: str = "de_DE") -> str:
    return babel.numbers.format_number(number, locale=locale)
