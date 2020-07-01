import dateparser
import re


def parse_datetime(datetime: str) -> dict:
    """
    Takes a datetime as string and returns a dictionary of parsed date and time. Implements the dateparser
    library for flexible date time parsing (https://dateparser.readthedocs.io/). Assumes GB formatting for
    dates i.e. Day/Month/Year (can handle multiple dividers for date e.g. ".", "/", "-" etc)

    Parameters
    ----------
    datetime: str
        datetime string to parse, can be date, or date and time.
    Returns
    -------
    dict
         {"date": None (if invalid datetime string) or string ("%day/%month/%year)
         "time": float (minutes passed for given date) or None (if no time value present in parsed string)}
    """
    result = dict()
    datetime = datetime.strip()
    pattern = "^[0-9]{1,2}[/.-][0-9]{1,2}[/.-]([0-9]{2}|[0-9]{4})$"
    if re.match(pattern, datetime):
        result["time"] = None
    datetime = dateparser.parse(datetime, locales=["en-GB"])
    if datetime is None:
        return {"date": None, "time": None}
    result["date"] = f"{datetime.day}/{datetime.month}/{datetime.year}"
    if "time" not in result.keys():
        result["time"] = (datetime.hour * 60) + datetime.minute
    return result
