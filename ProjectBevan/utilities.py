import dateparser
from IPython import get_ipython
from tqdm import tqdm
from tqdm.notebook import tqdm as tqdm_notebook
import re


def which_environment() -> str:
    """
    Test if module is being executed in the Jupyter environment.
    Returns
    -------
    str
        'jupyter', 'ipython' or 'terminal'
    """
    try:
        ipy_str = str(type(get_ipython()))
        if 'zmqshell' in ipy_str:
            return 'jupyter'
        if 'terminal' in ipy_str:
            return 'ipython'
    except:
        return 'terminal'


def progress_bar(x: iter,
                 verbose: bool = True,
                 **kwargs) -> callable:
    """
    Generate a progress bar using the tqdm library. If execution environment is Jupyter, return tqdm_notebook
    otherwise used tqdm.
    Parameters
    -----------
    x: iterable
        some iterable to pass to tqdm function
    verbose: bool, (default=True)
        Provide feedback (if False, no progress bar produced)
    kwargs:
        additional keyword arguments for tqdm
    :return: tqdm or tqdm_notebook, depending on environment
    """
    if not verbose:
        return x
    if which_environment() == 'jupyter':
        return tqdm_notebook(x, **kwargs)
    return tqdm(x, **kwargs)


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


def verbose_print(verbose: bool):
    return print if verbose else lambda *a, **k: None
