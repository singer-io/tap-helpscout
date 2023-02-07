import os
from datetime import datetime


def get_abs_path(path: str):
    """Returns absolute path for URL."""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def parse_date(date_value):
    """Pass in string-formatted-datetime, parse the value, and return it as an
    un-formatted datetime object."""
    date_formats = {
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f+00:00",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%d",
    }
    for date_format in date_formats:
        try:
            return datetime.strptime(date_value, date_format)
        except ValueError:
            continue
