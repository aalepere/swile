from __future__ import print_function

import os.path
import pickle
import sqlite3

import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1cqG_FRGjnOqHYx0GKKuxG7flzpCxfkuiC12aNDqDeRU"
SAMPLE_RANGE_NAME = "DATA1!A:K"


def main():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    con = sqlite3.connect("swile.db")
    df = pd.read_sql_query(
        """select
	U.*,
	A.avg_month
        from
	(
	select
		e.id,
		e.localization,
		STRFTIME('%Y',(e.birthDate)) as year_date,
		e.lastName,
		o.attributionDate,
		sum(case when o.status = 'signed' then 1 else 0 end) as num_signed,
		sum(case when o.status = 'lost' then 1 else 0 end) as num_lost,
		sum(case when o.status = 'never touched' then 1 else 0 end) as num_never_touched,
		sum(case when o.status = 'under negociation' then 1 else 0 end) as num_under_nego,
		round(sum(case when o.status = 'lost' then 1 else 0 end)* 1.0 / sum(case when o.status = 'signed' then 1 else 0 end),
		2) as loss_ratio
	from
		Employees e
	inner join Opportunities o on
		o.employeeId = e.id
	group by
		e.id,
		e.localization,
		e.firstName,
		e.lastName,
		o.attributionDate) U
        inner join (
	select
		o.employeeId,
		avg(aa.grossBookings) as avg_month
	from
		AccountsActvity aa
	inner join Opportunities o on
		o.accountId = aa.accountId
	group by
		o.employeeId ) A on
	A.employeeId = U.id""",
        con,
    )
    print(df)

    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID,
            valueInputOption="RAW",
            range=SAMPLE_RANGE_NAME,
            body=dict(
                majorDimension="ROWS", values=df.T.reset_index().T.values.tolist()
            ),
        )
        .execute()
    )
