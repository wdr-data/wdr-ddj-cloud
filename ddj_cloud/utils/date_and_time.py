"""Provides helper functions for dates."""

import datetime as dt
from zoneinfo import ZoneInfo

BERLIN = ZoneInfo("Europe/Berlin")
UTC = ZoneInfo("UTC")


def local_now() -> dt.datetime:
    """Generate current datetime (Berlin time zone).

    Returns:
        dt.datetime: Current datetime.
    """
    return dt.datetime.now(tz=BERLIN)


def local_today() -> dt.date:
    """Generate current date (Berlin time zone).

    Returns:
        dt.date: Today's date.
    """
    return local_now().date()


def local_today_midnight() -> dt.datetime:
    """Generate current date at midnight (Berlin time zone).

    Returns:
        dt.datetime: Today's date at midnight.
    """
    return local_now().replace(hour=0, minute=0, second=0, microsecond=0)


def local_yesterday() -> dt.date:
    """Generate yesterday's date (Berlin time zone).

    Returns:
        dt.date: Yesterday's date.
    """
    return local_today() - dt.timedelta(days=1)


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    """Generate a list of dates within a range. Start and end are both
    inclusive.

    Args:
        start (dt.date): Start date for range.
        end (dt.date): End date for range.

    Returns:
        List[dt.date]: List of dates between start and end.
    """

    delta = (end - start).days
    return [start + dt.timedelta(days=delta_days) for delta_days in range(delta + 1)]


def date_param(
    date: dt.date | None,
    *,
    default: dt.date | None = None,
    earliest: dt.date | None = None,
    latest: dt.date | None = None,
) -> dt.date | None:
    """For when you have an optional date parameter in your function but you want to limit the
    range of dates allowed. Also allows you to set a default.

    Args:
        date (Optional[dt.date]): The date you want to filter.
        default (Optional[dt.date]): Provide a default in case the date is None.
        earliest (Optional[dt.date]): The earliest date you want to allow.
        latest (Optional[dt.date]): The latest date you want to allow.

    Returns:
        Optional[dt.date]: The resulting date, or None if both ``date`` and ``default`` are ``None``.
    """
    if date is None:
        return default

    if earliest:
        date = max(earliest, date)

    if latest:
        date = min(latest, date)

    return date


def to_timedelta(seconds: int | None) -> dt.timedelta | None:
    """Generate a timedelta from an int containing a number of seconds.

    Args:
        seconds (Optional[int]): Amount of seconds to convert to timedelta. Also
            accepts None as input.

    Returns:
        Optional[dt.timedelta]: timedelta - returns None if seconds are None.
    """
    if seconds is not None:
        return dt.timedelta(seconds=seconds)
    else:
        return None


def iso_as_local(date_time: str | None, tz: ZoneInfo = BERLIN) -> dt.datetime | None:
    """Add timezone info to timezone naive isoformat date/time string.

    Args:
        date_time (Optional[str]): String containing timezone naive isoformat date/time
            string.
        tz (pytz.timezone, optional): Timezone to add to naive string. Defaults to
            BERLIN.

    Returns:
        Optional[dt.datetime]: dt.datetime object with tz timezone. Returns None if
            input date_time is None.
    """
    if date_time is not None:
        return dt.datetime.fromisoformat(date_time).replace(tzinfo=tz)
    else:
        return None
