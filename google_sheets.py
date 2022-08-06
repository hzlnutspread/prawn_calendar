from __future__ import print_function
import os
import gspread
from gspread_dataframe import set_with_dataframe
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from dotenv import load_dotenv


def authorize():
    load_dotenv()
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    token_json = {
        "token": os.environ.get('TOKEN'),
        "refresh_token": os.environ.get('REFRESH_TOKEN'),
        "token_uri": os.environ.get('TOKEN_URI'),
        "client_id": os.environ.get('CLIENT_ID'),
        "client_secret": os.environ.get('CLIENT_SECRET'),
        "scopes": [os.environ.get('SCOPES')],
        "expiry": "2025-08-05T07:46:56.867548Z"}

    credentials = {"installed":
                       {"client_id": os.environ.get('CLIENT_ID'),
                        "project_id": os.environ.get('PROJECT_ID'),
                        "auth_uri": os.environ.get('AUTH_URI'),
                        "token_uri": os.environ.get('TOKEN_URI'),
                        "auth_provider_x509_cert_url": os.environ.get('AUTH_PROVIDER'),
                        "client_secret": os.environ.get('CLIENT_SECRET'),
                        "redirect_uris": [os.environ.get('REDIRECT_URIS')]}
                   }
    creds = Credentials.from_authorized_user_info(token_json, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("refreshing token...")
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def update_gs(df, creds):
    SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')
    try:
        gc = gspread.authorize(creds)
        worksheet1 = gc.open_by_key(SPREADSHEET_ID).worksheet('Sheet1')
        worksheet1.clear()
        set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False, include_column_header=True,
                           resize=True)
    except HttpError as err:
        print(err)


