""" Pipeline to load all csv files into a SQLITE3 database """
import luigi
from luigi.contrib import sqla
from sqlalchemy import Date, String, Float
from tools.date import dateparse, load_with_format_and_clean
from tools.execute_query_to_gsheet import main


class InsertEmployees(sqla.CopyToTable):
    """
        Insert all records from users.csv into a sqlite3 database
        for the Employees table
    """

    columns = [
        (["id", String(length=10)], {"primary_key": True}),
        (["firstName", String(length=50)], {}),
        (["firstName", String(length=50)], {}),
        (["lastName", String(length=50)], {}),
        (["startDate", Date()], {}),
        (["localization", String(length=50)], {}),
        (["birthDate", Date()], {}),
    ]

    connection_string = "sqlite:///swile.db"
    table = "Employees"

    def rows(self):
        """
            Load the users.csv file, parse/convert dates
            Each row in the dataframe is then inserted into the Employees table
        """
        employees_df = load_with_format_and_clean(
            path="data/users.csv",
            sep=";",
            col_dates=["startDate", "birthDate"],
            parser=dateparse,
        )

        for _, row in employees_df.iterrows():
            yield row


class InsertOpportunities(sqla.CopyToTable):
    """
        Insert all records from users.csv into a sqlite3 database
        for the Opportunities table
    """

    columns = [
        (["id", String(length=10)], {"primary_key": True}),
        (["status", String(length=50)], {}),
        (["accountId", String(length=50)], {}),
        (["employeeId", String(length=50)], {}),
        (["attributionDate", Date()], {}),
    ]

    connection_string = "sqlite:///swile.db"
    table = "Opportunities"

    def rows(self):
        """
            Load the users.csv file, parse/convert dates
            Each row in the dataframe is then inserted into the Employees table
        """
        employees_df = load_with_format_and_clean(
            path="data/opportunities.csv",
            sep=";",
            col_dates=["attributionDate"],
            parser=dateparse,
        )

        for _, row in employees_df.iterrows():
            yield row


class InsertAccountsActivity(sqla.CopyToTable):
    """
        Insert all records from users.csv into a sqlite3 database
        for the AccountsActvity table
    """

    columns = [
        (["month", Date()], {}),
        (["accountId", String(length=50)], {}),
        (["grossBookings", Float(precision=2)], {}),
    ]

    connection_string = "sqlite:///swile.db"
    table = "AccountsActvity"

    def rows(self):
        """
            Load the users.csv file, parse/convert dates
            Each row in the dataframe is then inserted into the
            Opportunities table
        """
        employees_df = load_with_format_and_clean(
            path="data/accounts_with_bookings.csv",
            sep=";",
            col_dates=["month"],
            parser=dateparse,
        )

        for _, row in employees_df.iterrows():
            yield row


class LoadDataGsheet(luigi.Task):
    """
        Execute SQL statements
        Sends the data to Gsheet
        Data is then used for the dashboard
    """

    def requires(self):
        yield InsertEmployees()
        yield InsertOpportunities()
        yield InsertAccountsActivity()

    def run(self):
        main()
