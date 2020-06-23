""" Set of tools that will used for functions around dates """
import pandas as pd


def dateparse(date):
    """
        Parse a date as format DD/MM/YYYY and retunrs a datetime datetype
    """
    return pd.datetime.strptime(date, "%d/%m/%Y")


def load_with_format_and_clean(path, sep, col_dates, parser):
    """
        This function loads a dataframe from a csv file but also converts
        any date to a date format before loading into DB.
        It does also remove any empty columns
    """

    df = pd.read_csv(path, sep=sep, parse_dates=col_dates, date_parser=parser)
    df.dropna(how="all", axis=1, inplace=True)
    return df
